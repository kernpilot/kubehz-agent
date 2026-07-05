package publisher

import (
	"context"
	"time"
)

// Cadence configures the push timing (managed-platform-spec §2 defaults:
// full=60s, debounce=10s, minGap=15s).
type Cadence struct {
	// FullInterval forces a push at least this often even with no changes
	// (keeps the live view fresh and doubles as a retry heartbeat).
	FullInterval time.Duration
	// Debounce is the quiet period a burst of informer changes must settle for
	// before a change-triggered push (coalesces a rollout's event storm).
	Debounce time.Duration
	// MinGap is the floor between any two pushes (rate limit / API politeness).
	MinGap time.Duration
}

// PushReason labels why a flush fired (logging only).
type PushReason string

const (
	ReasonChange   PushReason = "change"
	ReasonPeriodic PushReason = "periodic"
)

// decision is the pure output of decide: push now, or wait this long.
type decision struct {
	push   bool
	reason PushReason
	wait   time.Duration
}

// decide is the pure coalescing rule — no timers, no goroutines — so the whole
// debounce/min-gap/periodic policy is exhaustively unit-testable.
//
// Inputs:
//   - now:        current time
//   - lastPush:   when the last payload was sent (zero => never)
//   - dirtySince: when the oldest un-pushed change arrived (zero => clean)
//   - lastFull:   when the last full/periodic push happened (zero => never)
//
// Rules:
//   - A change is due at max(dirtySince+Debounce, lastPush+MinGap) — it must
//     both settle (debounce) AND respect the min-gap since the last push.
//   - A periodic push is due at max(lastFull+FullInterval, lastPush+MinGap).
//   - The earliest applicable due time wins; if it is <= now, push (labeling
//     the reason), else wait until it.
func decide(now, lastPush, dirtySince, lastFull time.Time, c Cadence) decision {
	var (
		changeDue   time.Time
		haveChange  = !dirtySince.IsZero()
		periodicDue = maxTime(lastFull.Add(c.FullInterval), lastPush.Add(c.MinGap))
	)
	if haveChange {
		changeDue = maxTime(dirtySince.Add(c.Debounce), lastPush.Add(c.MinGap))
	}

	// Pick the earliest applicable due time.
	due := periodicDue
	reason := ReasonPeriodic
	if haveChange && changeDue.Before(due) {
		due = changeDue
		reason = ReasonChange
	}
	// If a change is pending and both are ready, prefer the change label (it is
	// the more specific trigger) — periodic is the floor.
	if haveChange && !changeDue.After(now) {
		reason = ReasonChange
		due = changeDue
	}

	if !due.After(now) {
		return decision{push: true, reason: reason}
	}
	return decision{push: false, reason: reason, wait: due.Sub(now)}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Coalescer turns a stream of change signals into debounced, rate-limited,
// periodically-refreshed flush() calls. flush is invoked synchronously from
// Run's goroutine; keep it cheap (it should hand off to the Sender).
type Coalescer struct {
	cadence Cadence
	now     func() time.Time // injectable for tests
}

// NewCoalescer builds a Coalescer. now may be nil (uses time.Now).
func NewCoalescer(c Cadence, now func() time.Time) *Coalescer {
	if now == nil {
		now = time.Now
	}
	return &Coalescer{cadence: c, now: now}
}

// Run consumes change signals until ctx is cancelled, calling flush(reason)
// according to the cadence. An initial periodic push fires promptly so the
// dashboard converges without waiting a full interval.
func (co *Coalescer) Run(ctx context.Context, changes <-chan struct{}, flush func(PushReason)) {
	var dirtySince, lastPush, lastFull time.Time

	timer := time.NewTimer(0) // fire immediately for the initial snapshot
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			if dirtySince.IsZero() {
				dirtySince = co.now()
			}
		case <-timer.C:
		}

		now := co.now()
		d := decide(now, lastPush, dirtySince, lastFull, co.cadence)
		if d.push {
			flush(d.reason)
			lastPush = now
			lastFull = now // any push sends a full snapshot
			dirtySince = time.Time{}
			resetTimer(timer, co.cadence.FullInterval)
		} else {
			resetTimer(timer, d.wait)
		}
	}
}

// resetTimer safely reschedules a timer to fire after d.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	if d <= 0 {
		d = time.Millisecond
	}
	t.Reset(d)
}
