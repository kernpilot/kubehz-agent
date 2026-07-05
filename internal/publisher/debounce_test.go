package publisher

import (
	"testing"
	"time"
)

func TestDecide(t *testing.T) {
	c := Cadence{FullInterval: 60 * time.Second, Debounce: 10 * time.Second, MinGap: 15 * time.Second}
	t0 := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	var zero time.Time

	tests := []struct {
		name       string
		now        time.Time
		lastPush   time.Time
		dirtySince time.Time
		lastFull   time.Time
		wantPush   bool
		wantReason PushReason
		wantWait   time.Duration
	}{
		{
			name: "cold start: never pushed, no changes -> push immediately (periodic)",
			now:  t0, lastPush: zero, dirtySince: zero, lastFull: zero,
			wantPush: true, wantReason: ReasonPeriodic,
		},
		{
			name: "just pushed, clean -> wait for the full interval",
			now:  t0.Add(5 * time.Second), lastPush: t0, dirtySince: zero, lastFull: t0,
			wantPush: false, wantReason: ReasonPeriodic, wantWait: 55 * time.Second,
		},
		{
			name: "change right after push -> min-gap floor dominates the debounce",
			now:  t0.Add(3 * time.Second), lastPush: t0, dirtySince: t0.Add(2 * time.Second), lastFull: t0,
			wantPush: false, wantReason: ReasonChange, wantWait: 12 * time.Second, // due at t0+15s (min-gap)
		},
		{
			name: "change settled and min-gap elapsed -> push (change)",
			now:  t0.Add(20 * time.Second), lastPush: t0, dirtySince: t0.Add(2 * time.Second), lastFull: t0,
			wantPush: true, wantReason: ReasonChange,
		},
		{
			name: "recent change, min-gap passed but debounce still coalescing -> wait for debounce",
			now:  t0.Add(20 * time.Second), lastPush: t0, dirtySince: t0.Add(18 * time.Second), lastFull: t0,
			wantPush: false, wantReason: ReasonChange, wantWait: 8 * time.Second, // due at t0+28s (dirty+debounce)
		},
		{
			name: "no change, full interval elapsed -> push (periodic)",
			now:  t0.Add(61 * time.Second), lastPush: t0, dirtySince: zero, lastFull: t0,
			wantPush: true, wantReason: ReasonPeriodic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := decide(tt.now, tt.lastPush, tt.dirtySince, tt.lastFull, c)
			if d.push != tt.wantPush {
				t.Fatalf("push = %t, want %t (%+v)", d.push, tt.wantPush, d)
			}
			if d.reason != tt.wantReason {
				t.Errorf("reason = %q, want %q", d.reason, tt.wantReason)
			}
			if !tt.wantPush && d.wait != tt.wantWait {
				t.Errorf("wait = %s, want %s", d.wait, tt.wantWait)
			}
		})
	}
}

// The oldest change in a burst anchors the debounce window (bounded latency,
// never starves during a long rollout). This verifies the Coalescer's contract
// of holding dirtySince at the FIRST change.
func TestDecide_BurstAnchoredOnFirstChange(t *testing.T) {
	c := Cadence{FullInterval: 60 * time.Second, Debounce: 10 * time.Second, MinGap: 15 * time.Second}
	t0 := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	first := t0.Add(1 * time.Second)
	// Even though "now" is well past later changes, the due time is anchored on
	// the first change + max(debounce, min-gap-from-lastPush).
	d := decide(t0.Add(30*time.Second), t0, first, t0, c)
	if !d.push {
		t.Errorf("expected push once anchored window elapsed, got %+v", d)
	}
}
