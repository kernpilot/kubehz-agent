package desired

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// jitterFraction desynchronizes a fleet of agents: each wait is the configured
// interval plus a uniform random slice of up to this fraction of it, so
// thousands of clusters never poll the API in lockstep.
const jitterFraction = 0.1

// Freshness bounds for acting on a CACHED document (the 304 path).
//
// A 304 is not proof that the platform re-affirmed anything: any cache between
// the agent and the API — a proxy, a CDN, a broken gateway — can answer one
// from a frozen representation forever. Only a 200 with a body is an
// affirmation. The healer asks for a re-run on EVERY tick (it is a continuous
// control loop), so without a bound a healing-armed agent would keep deleting
// machines on intent nobody re-affirmed.
//
// The poller therefore counts the polls since the platform last served a
// document, and:
//
//   - after staleRevalidateTicks it fetches UNCONDITIONALLY (no If-None-Match,
//     no-cache) — no cache may answer that with a 304, so one full body every
//     few polls buys the proof back;
//   - past staleHealingTicks the agent stops acting on the cached document
//     while healing is armed. Healing deletes machines, so it gets the
//     tightest bound;
//   - past staleActingTicks every other retry stops as well.
//
// The bounds count POLLS, so the effective time follows
// KUBEHZ_DESIRED_POLL_SECONDS (default 60 s → about 10 minutes and 1 hour).
// A refusal is reported through the Actor, never only logged, and a single
// served document clears it.
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
	// healing is armed (a continuous control loop). A 304 alone never
	// re-triggers acting.
	Reconcile(ctx context.Context, doc *Doc) (retry bool)
	// RefuseStale reports that the agent is NOT acting on doc because the
	// platform has not served it again inside the freshness bound. reason is a
	// stable sentence for the action detail; the agent keeps polling and
	// resumes as soon as a document arrives.
	RefuseStale(ctx context.Context, doc *Doc, reason string)
}

// Poller runs the pull loop: conditional GET every interval (+ jitter), acting
// via the Actor on every non-304 response. Error handling mirrors the
// heartbeat Sender's discipline: capped exponential backoff, with 401/403
// honoring the FULL backoff (an identity failure is not cured by polling
// faster — nothing here preempts the wait) and surfaced loudly.
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
	var cached *Doc // last successfully pulled doc, for transient-failure retries
	retryPending := false
	// unaffirmed counts the polls since the platform last SERVED a document.
	// A 304 and a failed poll both leave the intent unconfirmed.
	unaffirmed := 0
	// staleLogged keeps the refusal to one log line per stale episode (it is
	// reported on every tick, where identical details cost nothing).
	staleLogged := false

	for {
		doc, notModified, err := p.fetch(ctx, unaffirmed)
		var wait time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			unaffirmed++
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
			unaffirmed++
			// Unchanged intent. Acting re-runs ONLY if the previous pass asked
			// for it — a 304 must not otherwise re-assert state (deliberate:
			// the desired doc is per-revision intent, not a continuous
			// enforcement loop fighting manual ops/the autoscaler) — and only
			// while the document is still fresh enough to act on.
			if retryPending && cached != nil {
				if bound := staleBound(cached); unaffirmed <= bound {
					retryPending = p.actor.Reconcile(ctx, cached)
				} else {
					if !staleLogged {
						staleLogged = true
						p.log.Warn("the platform has not served the desired document within the freshness bound; acting stopped",
							"polls", unaffirmed, "bound", bound, "revision", cached.Revision)
					}
					p.actor.RefuseStale(ctx, cached, staleReason(bound))
				}
			}
			wait = p.jitteredInterval()
		default:
			backoff.Reset()
			unaffirmed = 0
			staleLogged = false
			cached = doc
			// healing is logged as the ARMED state (both server bits — the same
			// conjunction the executor acts on), plus the effective policy
			// numbers, so arming/policy changes have a positive log signal.
			p.log.Info("desired state pulled",
				"revision", doc.Revision,
				"pools", len(doc.WorkerPools),
				"scaling", doc.Execution.Scaling,
				"upgrades", doc.Execution.Upgrades,
				"healing", doc.Execution.Healing && doc.Healing.Enabled,
				"healingMaxUnhealthy", doc.Healing.MaxUnhealthy,
				"healingUnhealthyAfterSeconds", doc.Healing.UnhealthyAfterSeconds,
				"healingCooldownSeconds", doc.Healing.CooldownSeconds)
			retryPending = p.actor.Reconcile(ctx, doc)
			wait = p.jitteredInterval()
		}

		select {
		case <-ctx.Done():
			return
		case <-p.afterFunc(wait):
		}
	}
}

// fetch performs the poll: a conditional GET normally, and an unconditional,
// cache-busting revalidation every staleRevalidateTicks polls that served no
// document. unaffirmed is the current count of such polls.
func (p *Poller) fetch(ctx context.Context, unaffirmed int) (*Doc, bool, error) {
	if unaffirmed >= staleRevalidateTicks && unaffirmed%staleRevalidateTicks == 0 {
		p.log.Info("desired state unchanged for several polls; revalidating without the ETag",
			"polls", unaffirmed)
		return p.client.Revalidate(ctx)
	}
	return p.client.Fetch(ctx)
}

// staleBound is how many unaffirmed polls a cached document may still drive.
// Healing deletes machines, so an armed healer gets the tightest bound.
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
		"refusing to act: the platform has not served this document again in %d polls — the agent does not act on intent nobody re-affirmed",
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
