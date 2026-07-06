package desired

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// jitterFraction desynchronizes a fleet of agents: each wait is the configured
// interval plus a uniform random slice of up to this fraction of it, so
// thousands of clusters never poll the API in lockstep.
const jitterFraction = 0.1

// Actor consumes a freshly pulled desired-state document and acts (or refuses
// to act) LOCALLY. Implemented by the executor; an interface so the poller is
// unit-testable with a recording fake and owns no acting policy itself.
type Actor interface {
	// Reconcile drives local state toward doc, reporting outcomes as it goes.
	// It returns true when a TRANSIENT failure warrants re-running against the
	// same document on the next poll tick (a 304 does not normally re-trigger
	// acting; a transient failure is the exception so it self-heals at the
	// poll cadence instead of waiting for the next revision).
	Reconcile(ctx context.Context, doc *Doc) (retry bool)
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

	for {
		doc, notModified, err := p.client.Fetch(ctx)
		var wait time.Duration
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			wait = backoff.Next()
			var authErr *publisher.AuthError
			if errors.As(err, &authErr) {
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
			// Unchanged intent. Acting re-runs ONLY if the previous pass hit a
			// transient failure — a 304 must not otherwise re-assert state
			// (deliberate: the desired doc is per-revision intent, not a
			// continuous enforcement loop fighting manual ops/the autoscaler).
			if retryPending && cached != nil {
				retryPending = p.actor.Reconcile(ctx, cached)
			}
			wait = p.jitteredInterval()
		default:
			backoff.Reset()
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

func (p *Poller) jitteredInterval() time.Duration {
	return p.interval + time.Duration(p.jitter()*jitterFraction*float64(p.interval))
}
