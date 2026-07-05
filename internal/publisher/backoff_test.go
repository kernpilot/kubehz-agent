package publisher

import (
	"testing"
	"time"
)

func TestBackoff_ExponentialWithCap(t *testing.T) {
	b := NewBackoff(time.Second, 30*time.Second)
	b.jitter = func() float64 { return 1.0 } // full jitter -> the cap value

	want := []time.Duration{
		1 * time.Second,  // base * 2^0
		2 * time.Second,  // base * 2^1
		4 * time.Second,  // base * 2^2
		8 * time.Second,  // base * 2^3
		16 * time.Second, // base * 2^4
		30 * time.Second, // capped (would be 32s)
		30 * time.Second, // stays capped
	}
	for i, w := range want {
		got := b.Next()
		if got != w {
			t.Errorf("attempt %d: Next() = %s, want %s", i, got, w)
		}
	}
	if b.Attempt() != len(want) {
		t.Errorf("attempt count = %d, want %d", b.Attempt(), len(want))
	}
}

func TestBackoff_JitterFloorsAtBase(t *testing.T) {
	b := NewBackoff(time.Second, time.Minute)
	b.jitter = func() float64 { return 0.0 } // zero jitter must not yield a 0 wait
	for i := 0; i < 5; i++ {
		if got := b.Next(); got < time.Second {
			t.Errorf("attempt %d: Next() = %s, want >= base(1s)", i, got)
		}
	}
}

func TestBackoff_Reset(t *testing.T) {
	b := NewBackoff(time.Second, time.Minute)
	b.jitter = func() float64 { return 1.0 }
	b.Next()
	b.Next()
	b.Reset()
	if b.Attempt() != 0 {
		t.Errorf("attempt after reset = %d, want 0", b.Attempt())
	}
	if got := b.Next(); got != time.Second {
		t.Errorf("first Next after reset = %s, want 1s", got)
	}
}

func TestNewBackoff_Defaults(t *testing.T) {
	b := NewBackoff(0, 0)
	if b.base != time.Second || b.max != 5*time.Minute {
		t.Errorf("defaults wrong: base=%s max=%s", b.base, b.max)
	}
	// max < base is corrected up to base.
	b2 := NewBackoff(10*time.Second, time.Second)
	if b2.max < b2.base {
		t.Errorf("max should be >= base, got base=%s max=%s", b2.base, b2.max)
	}
}
