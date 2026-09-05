package publisher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// instantAfter is an afterFunc that fires immediately, so backoff waits don't
// slow the test (we assert the retry COUNT, not real timing).
func instantAfter(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func TestSender_RetriesUntilSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError) // fail twice
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("missing bearer on retry: %q", got)
		}
		w.WriteHeader(http.StatusOK) // succeed on the 3rd
	}))
	defer srv.Close()

	pub := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	s := NewSender(pub, time.Millisecond, time.Millisecond, nil)
	s.afterFunc = instantAfter

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Run(ctx) }()

	s.Enqueue(samplePayload())

	waitFor(t, func() bool { return atomic.LoadInt32(&calls) >= 3 }, 2*time.Second)
	cancel()
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("calls = %d, want exactly 3 (2 fails + 1 success)", got)
	}
}

func TestSender_CoalescesToLatest(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	release := make(chan struct{})
	var first int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block the very first request so a second payload can supersede a
		// third-and-later enqueue while we're "in flight".
		if atomic.AddInt32(&first, 1) == 1 {
			<-release
		}
		var p state.Payload
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		seen = append(seen, p.Timestamp)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pub := New(srv.URL, "d", testToken, "0.1.0", srv.Client())
	s := NewSender(pub, time.Millisecond, time.Millisecond, nil)
	s.afterFunc = instantAfter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Run(ctx) }()

	p1 := samplePayload()
	p1.Timestamp = "t1"
	s.Enqueue(p1) // sender picks this up and blocks in the handler

	waitFor(t, func() bool { return atomic.LoadInt32(&first) == 1 }, time.Second)

	// While the first is blocked, enqueue two more; only the LATEST should be
	// sent after the block releases (coalescing at the delivery layer).
	p2 := samplePayload()
	p2.Timestamp = "t2"
	s.Enqueue(p2)
	p3 := samplePayload()
	p3.Timestamp = "t3"
	s.Enqueue(p3)

	close(release)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, ts := range seen {
			if ts == "t3" {
				return true
			}
		}
		return false
	}, 2*time.Second)

	mu.Lock()
	defer mu.Unlock()
	// t2 must have been coalesced away (superseded by t3 before delivery).
	for _, ts := range seen {
		if ts == "t2" {
			t.Errorf("t2 should have been coalesced, but was sent: %v", seen)
		}
	}
}

// TestSender_AuthErrorHonorsBackoff: on 401/403 a newer enqueue must NOT
// preempt the backoff wait (an identity failure is not cured by fresher data;
// preemption would retry a revoked token at the enqueue cadence forever). The
// retry itself must still deliver the LATEST payload.
func TestSender_AuthErrorHonorsBackoff(t *testing.T) {
	var calls int32
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		var p state.Payload
		_ = json.NewDecoder(r.Body).Decode(&p)
		mu.Lock()
		seen = append(seen, p.Timestamp)
		mu.Unlock()
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	pub := New(srv.URL, "d", testToken, "0.1.0", srv.Client())
	s := NewSender(pub, time.Second, time.Minute, nil)

	// Controllable backoff timer: the sender blocks until the test fires it.
	timers := make(chan chan time.Time, 16)
	s.afterFunc = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		timers <- ch
		return ch
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Run(ctx) }()

	p1 := samplePayload()
	p1.Timestamp = "t1"
	s.Enqueue(p1)

	// First attempt 401s; the sender must now be parked in the backoff wait.
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 1 }, 2*time.Second)
	var backoffTimer chan time.Time
	select {
	case backoffTimer = <-timers:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not enter the backoff wait")
	}

	// A newer payload arrives mid-backoff: it must NOT trigger a retry.
	p2 := samplePayload()
	p2.Timestamp = "t2"
	s.Enqueue(p2)
	time.Sleep(50 * time.Millisecond) // generous window for a (buggy) preempt
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("enqueue preempted the auth backoff: %d calls, want 1", got)
	}

	// Backoff elapses: exactly one retry fires, carrying the LATEST payload.
	backoffTimer <- time.Now()
	waitFor(t, func() bool { return atomic.LoadInt32(&calls) == 2 }, 2*time.Second)
	mu.Lock()
	last := seen[len(seen)-1]
	mu.Unlock()
	if last != "t2" {
		t.Errorf("retry sent %q, want the latest payload t2", last)
	}

	cancel()
	wg.Wait()
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// A re-enrollment (`lo kubehz re-enroll`) rotates the in-cluster Secret and
// the kubelet refreshes the projected file under the running pod. The sender
// must re-read the token after a rejection and retry with the new one — the
// old behaviour was a 401 loop until the pod restarted.
func TestSender_AuthErrorReloadsTokenFromFile(t *testing.T) {
	const rotated = "khz_agt_" + "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	var mu sync.Mutex
	var bearers []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		bearers = append(bearers, auth)
		mu.Unlock()
		if auth != "Bearer "+rotated {
			w.WriteHeader(http.StatusUnauthorized) // the old token is revoked
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// The mounted Secret file: starts with the token the publisher was built
	// with, then the "kubelet" swaps in the rotated one.
	tokenFile := t.TempDir() + "/agent-token"
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pub := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	pub.SetTokenReloader(func(context.Context) (string, error) {
		return config.ReadTokenFile(os.ReadFile, tokenFile)
	})
	if err := os.WriteFile(tokenFile, []byte(rotated+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := NewSender(pub, time.Millisecond, time.Millisecond, nil)
	s.afterFunc = instantAfter
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Run(ctx) }()

	s.Enqueue(samplePayload())
	// Two requests seen AND the slot cleared: the 200 was fully processed.
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(bearers) >= 2 && s.take() == nil
	}, 2*time.Second)
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(bearers) != 2 {
		t.Fatalf("requests = %d, want exactly 2 (one 401 with the old token, one 200 with the new)", len(bearers))
	}
	if bearers[0] != "Bearer "+testToken {
		t.Errorf("first request bearer = %q, want the original token", bearers[0])
	}
	if bearers[1] != "Bearer "+rotated {
		t.Errorf("retry bearer = %q, want the rotated token re-read from the file", bearers[1])
	}
	if s.take() != nil {
		t.Error("payload still queued after the successful retry")
	}
}

// An unchanged token must not short-circuit the backoff: a revoked token
// that was NOT rotated still waits the full, growing backoff after every
// rejection, with exactly one reload per rejection.
func TestSender_AuthErrorUnchangedTokenStillBacksOff(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	pub := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	var reloads int32
	pub.SetTokenReloader(func(context.Context) (string, error) {
		atomic.AddInt32(&reloads, 1)
		return testToken, nil // same token: nothing rotated
	})
	s := NewSender(pub, time.Millisecond, time.Second, nil)
	// Deterministic backoff (no jitter) so the waits are comparable, and a
	// recording timer so every wait the sender takes is observable.
	var backoffs []*Backoff
	s.newBackoff = func() *Backoff {
		b := NewBackoff(time.Millisecond, time.Second)
		b.jitter = func() float64 { return 1 }
		backoffs = append(backoffs, b)
		return b
	}
	var mu sync.Mutex
	var waits []time.Duration
	s.afterFunc = func(d time.Duration) <-chan time.Time {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return instantAfter(d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.Run(ctx) }()

	s.Enqueue(samplePayload())
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(waits) >= 3
	}, 2*time.Second)
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if r := atomic.LoadInt32(&reloads); int(r) < len(waits) {
		t.Errorf("reloads = %d, waits = %d: every rejection must re-read the token before waiting", r, len(waits))
	}
	if len(backoffs) != 1 || backoffs[0].Attempt() < 3 {
		t.Errorf("backoff attempts = %v, want one backoff at attempt >= 3 (never reset by an unchanged reload)", backoffs)
	}
	for i := 1; i < 3; i++ {
		if waits[i] < waits[i-1] {
			t.Errorf("waits = %v: an unchanged token must keep the growing backoff", waits)
		}
	}
	if waits[1] <= waits[0] {
		t.Errorf("waits = %v: the second wait must be longer than the first (exponential)", waits)
	}
}
