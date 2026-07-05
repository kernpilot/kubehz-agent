package publisher

import (
	"math/rand"
	"time"
)

// Backoff is a capped exponential backoff with full jitter. It is used by the
// Sender to space out retries after a failed push without hammering the API.
// Zero value is not usable — construct with NewBackoff.
type Backoff struct {
	base   time.Duration
	max    time.Duration
	factor float64
	// attempt is the number of consecutive failures so far.
	attempt int
	// jitter returns a fraction in [0,1); injectable for deterministic tests.
	jitter func() float64
}

// NewBackoff builds a backoff from base up to max (factor 2). base/max default
// to 1s/5m when non-positive.
func NewBackoff(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 5 * time.Minute
	}
	if max < base {
		max = base
	}
	return &Backoff{base: base, max: max, factor: 2, jitter: rand.Float64}
}

// Next returns the wait before the next attempt and advances the state. With
// full jitter the result is uniform in [0, capped-exponential], which avoids a
// thundering herd of agents retrying in lockstep. The minimum returned is base
// so a push is never retried instantly in a hot loop.
func (b *Backoff) Next() time.Duration {
	exp := float64(b.base) * pow(b.factor, b.attempt)
	capped := exp
	if capped > float64(b.max) {
		capped = float64(b.max)
	}
	b.attempt++
	d := time.Duration(b.jitter() * capped)
	if d < b.base {
		d = b.base
	}
	if d > b.max {
		d = b.max
	}
	return d
}

// Reset clears the failure count after a successful push.
func (b *Backoff) Reset() { b.attempt = 0 }

// Attempt reports the current consecutive-failure count (for logging/tests).
func (b *Backoff) Attempt() int { return b.attempt }

func pow(base float64, exp int) float64 {
	out := 1.0
	for i := 0; i < exp; i++ {
		out *= base
	}
	return out
}
