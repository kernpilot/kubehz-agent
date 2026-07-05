package publisher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
