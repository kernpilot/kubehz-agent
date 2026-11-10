package desired

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"reflect"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// jitterFraction desynchronizes a fleet of agents: each wait is the configured
// interval plus a uniform random slice of up to this fraction of it, so
// thousands of clusters never poll the API in lockstep.
const jitterFraction = 0.1

// Freshness bounds for acting on a document.
//
// Neither a 304 nor a 200 proves that the platform re-affirmed anything: any
// cache between the agent and the API — a proxy, a CDN, a broken gateway —
// can serve either one from a frozen copy forever. The healer asks for a
// re-run on EVERY tick (it is a continuous control loop), so without a bound
// a healing-armed agent would keep deleting machines on intent nobody
// re-affirmed.
//
// The only proof the agent can get is a REVALIDATION: an unconditional
// request (no If-None-Match, so no cache may answer it with a 304) that the
// platform answers with a document. Revalidations run on their own cadence,
// whatever the polls in between return, and the poller counts the polls since
// the last one that succeeded:
//
//   - at staleRevalidateTicks it revalidates, and keeps revalidating on every
//     poll until one succeeds;
//   - past staleHealingTicks the agent stops acting while healing is armed.
//     Healing deletes machines, so it gets the tightest bound;
//   - past staleActingTicks every other retry stops as well.
//
// The bounds count POLLS, so the effective time follows
// KUBEHZ_DESIRED_POLL_SECONDS (default 60 s → about 10 minutes and 1 hour).
// A refusal is reported through the Actor, never only logged, and the next
// successful revalidation clears it.
const (
	staleRevalidateTicks = 5
	staleHealingTicks    = 10
	staleActingTicks     = 60
)

// Actor consumes a freshly pulled desired-state document and acts (or refuses
// to act) LOCALLY. Implemented by the executor; an interface so the poller is
// unit-testable with a recording fake and owns no acting policy itself.
type Actor interface {
	// Reconcile drives local state toward doc, reporting outcomes as it goes.
	// It returns true when a re-run against the SAME document on the next poll
	// tick is wanted: after a transient failure, and on every tick while
	// healing is armed (a continuous control loop). UNCHANGED intent — a 304,
	// or the same document served again — re-runs acting only when the last
	// pass asked for it.
	Reconcile(ctx context.Context, doc *Doc) (retry bool)
	// RefuseStale reports that the agent is NOT acting on doc because the
	// platform has not proved it fresh inside the freshness bound. reason is
	// a stable sentence for the action detail; the agent keeps polling and
	// resumes as soon as a revalidation succeeds.
	RefuseStale(ctx context.Context, doc *Doc, reason string)
}

// Poller runs the pull loop: conditional GET every interval (+ jitter), with
// a revalidation on its own cadence, acting via the Actor on new intent and
// on requested re-runs. Error handling mirrors the heartbeat Sender's
// discipline: capped exponential backoff, with 401/403 honoring the FULL
// backoff (an identity failure is not cured by polling faster — nothing here
// preempts the wait) and surfaced loudly.
type Poller struct {
	client     *Client
	actor      Actor
	interval   time.Duration
	newBackoff func() *publisher.Backoff
	log        *slog.Logger

	afterFunc func(time.Duration) <-chan time.Time // injectable timer for tests
	jitter    func() float64                       // fraction in [0,1); injectable
}

// NewPoller builds a Poller. logger may be nil (uses slog.Default);
// baseBackoff/maxBackoff bound the retry spacing after failed polls.
func NewPoller(client *Client, actor Actor, interval, baseBackoff, maxBackoff time.Duration, logger *slog.Logger) *Poller {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &Poller{
		client:     client,
		actor:      actor,
		interval:   interval,
		newBackoff: func() *publisher.Backoff { return publisher.NewBackoff(baseBackoff, maxBackoff) },
		log:        logger,
		afterFunc:  time.After,
		jitter:     rand.Float64,
	}
}

// Run polls until ctx is cancelled. The first poll fires immediately so a
// restarted agent reconverges without waiting a full interval (restart =
// re-poll + reconverge; harmless because the Actor is idempotent).
func (p *Poller) Run(ctx context.Context) {
	backoff := p.newBackoff()
	// cached is the last document the platform served. nil means there is
	// nothing to act on: at start, and after a REJECTION (a 401/403, another
	// definitive 4xx, an invalid document). A rejection revokes whatever the
	// platform authorized before; only a new document restores it.
	var cached *Doc
	// pending: cached still has to run — it is new intent, or the last pass
	// asked for a re-run (a transient failure, or an armed healer).
	pending := false
	// sinceProof counts the polls since the last successful REVALIDATION.
	// Ordinary answers (200 or 304) do not reset it.
	sinceProof := 0
	// staleLogged keeps the refusal to one log line per stale episode (it is
	// reported on every tick, where identical details cost nothing).
	staleLogged := false

	for {
		revalidate := cached == nil || sinceProof >= staleRevalidateTicks
		doc, notModified, err := p.fetch(ctx, revalidate, sinceProof)
		var wait time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			sinceProof++
			if IsRejection(err) {
				if cached != nil {
					p.log.Warn("the platform rejected the desired-state poll; the cached document is dropped and the agent acts again only on a new one",
						"error", err.Error(), "revision", cached.Revision)
				}
				cached, pending = nil, false
				p.client.ForgetETag()
			}
			var authErr *publisher.AuthError
			if errors.As(err, &authErr) {
				// A re-enrollment rotates the Secret under the running pod.
				// Re-read the token first (same discipline as the Sender): a
				// CHANGED token is polled again at once with a fresh backoff.
				if p.reloadToken(ctx) {
					backoff.Reset()
					continue
				}
			}
			wait = backoff.Next()
			if authErr != nil {
				// Identity problem: surface loudly but keep retrying (recovery
				// is a redeploy/rotation, outside the agent's authority). The
				// full backoff wait is honored — same fix as the Sender's.
				p.log.Error("desired-state poll auth rejected; will keep retrying",
					"error", authErr.Error(), "retryIn", wait.String(), "attempt", backoff.Attempt())
			} else {
				p.log.Warn("desired-state poll failed; backing off",
					"error", err.Error(), "retryIn", wait.String(), "attempt", backoff.Attempt())
			}
		case notModified:
			backoff.Reset()
			sinceProof++
			wait = p.jitteredInterval()
		default:
			backoff.Reset()
			if revalidate {
				sinceProof = 0
				staleLogged = false
			} else {
				sinceProof++
			}
			// New intent runs. The SAME document again (a revalidation, a
			// server without ETags) is unchanged intent, exactly like a 304:
			// re-running a pass that already converged would undo a manual
			// change made since (deliberate: the desired doc is per-revision
			// intent, not a continuous enforcement loop fighting manual ops or
			// the autoscaler). The whole document is compared, because the
			// execution flags can change without a revision bump.
			if cached == nil || !reflect.DeepEqual(cached, doc) {
				cached, pending = doc, true
				// healing is logged as the ARMED state (both server bits — the
				// same conjunction the executor acts on), plus the effective
				// policy numbers, so arming/policy changes have a positive log
				// signal.
				p.log.Info("desired state pulled",
					"revision", doc.Revision,
					"pools", len(doc.WorkerPools),
					"scaling", doc.Execution.Scaling,
					"upgrades", doc.Execution.Upgrades,
					"healing", doc.Execution.Healing && doc.Healing.Enabled,
					"healingMaxUnhealthy", doc.Healing.MaxUnhealthy,
					"healingUnhealthyAfterSeconds", doc.Healing.UnhealthyAfterSeconds,
					"healingCooldownSeconds", doc.Healing.CooldownSeconds)
			}
			wait = p.jitteredInterval()
		}

		if pending && cached != nil {
			if bound := staleBound(cached); sinceProof > bound {
				if !staleLogged {
					staleLogged = true
					p.log.Warn("the platform has not proved the desired document fresh within the freshness bound; acting stopped",
						"polls", sinceProof, "bound", bound, "revision", cached.Revision)
				}
				p.actor.RefuseStale(ctx, cached, staleReason(bound))
			} else if err == nil {
				// Only a poll that reached the platform runs acting; a failed
				// poll carries no news.
				pending = p.actor.Reconcile(ctx, cached)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-p.afterFunc(wait):
		}
	}
}

// fetch performs the poll: a conditional GET normally, and an unconditional
// revalidation when revalidate is set.
func (p *Poller) fetch(ctx context.Context, revalidate bool, sinceProof int) (*Doc, bool, error) {
	if revalidate {
		if sinceProof > 0 {
			p.log.Debug("revalidating the desired document (unconditional request)",
				"pollsSinceProof", sinceProof)
		}
		return p.client.Revalidate(ctx)
	}
	return p.client.Fetch(ctx)
}

// staleBound is how many polls without a successful revalidation a document
// may still drive. Healing deletes machines, so an armed healer gets the
// tightest bound.
func staleBound(doc *Doc) int {
	if doc.Execution.Healing && doc.Healing.Enabled {
		return staleHealingTicks
	}
	return staleActingTicks
}

// staleReason is the sentence the refusal reports. It carries the BOUND, not
// the current count, so every tick of one stale episode reports the same bytes
// and the action store stays quiet.
func staleReason(bound int) string {
	return fmt.Sprintf(
		"refusing to act: the platform has not answered an unconditional request for this document in %d polls — the agent does not act on intent nobody re-affirmed",
		bound)
}

// reloadToken re-reads bearer A after a rejection and reports whether it
// changed. Errors are logged (never the token) and count as "unchanged".
func (p *Poller) reloadToken(ctx context.Context) bool {
	changed, err := p.client.ReloadToken(ctx)
	if err != nil {
		p.log.Warn("could not re-read the agent token after a rejection", "error", err.Error())
		return false
	}
	if changed {
		p.log.Info("agent token changed at its source; polling desired state again with the new token")
	}
	return changed
}

func (p *Poller) jitteredInterval() time.Duration {
	return p.interval + time.Duration(p.jitter()*jitterFraction*float64(p.interval))
}
