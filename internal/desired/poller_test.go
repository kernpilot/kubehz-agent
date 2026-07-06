package desired

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// recordingActor records every doc it is handed and returns scripted retry
// values (default false).
type recordingActor struct {
	mu    sync.Mutex
	docs  []*Doc
	retry func(call int) bool
}

func (a *recordingActor) Reconcile(_ context.Context, doc *Doc) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.docs = append(a.docs, doc)
	if a.retry != nil {
		return a.retry(len(a.docs))
	}
	return false
}

func (a *recordingActor) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.docs)
}

func (a *recordingActor) doc(i int) *Doc {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.docs[i]
}

// desiredServer serves a mutable desired doc with the API's real ETag scheme
// ("<revision>-<scaling><upgrades>", strong).
type desiredServer struct {
	mu       sync.Mutex
	revision int
	scaling  bool
	gets     int32
}

func (s *desiredServer) set(revision int, scaling bool) {
	s.mu.Lock()
	s.revision = revision
	s.scaling = scaling
	s.mu.Unlock()
}

func (s *desiredServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&s.gets, 1)
		s.mu.Lock()
		rev, scaling := s.revision, s.scaling
		s.mu.Unlock()
		bit := 0
		if scaling {
			bit = 1
		}
		etag := fmt.Sprintf("%q", fmt.Sprintf("%d-%d0", rev, bit))
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fmt.Fprintf(w, `{"revision":%d,"kubernetesVersion":null,"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":3}],"execution":{"scaling":%t,"upgrades":false}}`, rev, scaling)
	}
}

// timers gives the test full control over the poller's waits (the poller
// blocks until the test fires the returned channel), mirroring the sender
// tests' technique.
type timers struct{ ch chan chan time.Time }

func newTimers() *timers { return &timers{ch: make(chan chan time.Time, 16)} }

func (tm *timers) afterFunc(time.Duration) <-chan time.Time {
	c := make(chan time.Time, 1)
	tm.ch <- c
	return c
}

// next waits for the poller to park in a wait and returns its trigger.
func (tm *timers) next(t *testing.T) chan time.Time {
	t.Helper()
	select {
	case c := <-tm.ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("poller never parked in a wait")
		return nil
	}
}

func newTestPoller(t *testing.T, srvURL string, actor Actor) (*Poller, *timers) {
	t.Helper()
	c := NewClient(srvURL, "kubehz.in.net", testToken, "0.1.0", nil)
	p := NewPoller(c, actor, time.Minute, time.Second, time.Minute, nil)
	tm := newTimers()
	p.afterFunc = tm.afterFunc
	p.jitter = func() float64 { return 0 }
	return p, tm
}

// The 304 path: an unchanged doc must reconcile exactly ONCE — subsequent
// polls ride the ETag and never re-trigger acting.
func TestPoller_NotModifiedDoesNotReact(t *testing.T) {
	srv := &desiredServer{revision: 3, scaling: true}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	actor := &recordingActor{}
	p, tm := newTestPoller(t, ts.URL, actor)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	// Poll 1: 200 → reconcile. Then let two more polls 304 through.
	tm.next(t) <- time.Now()
	tm.next(t) <- time.Now()
	trigger := tm.next(t)

	if got := actor.calls(); got != 1 {
		t.Errorf("reconciles = %d, want exactly 1 (304s must not re-trigger)", got)
	}
	if got := atomic.LoadInt32(&srv.gets); got != 3 {
		t.Errorf("GETs = %d, want 3", got)
	}
	if actor.doc(0).Revision != 3 {
		t.Errorf("acted revision = %d, want 3", actor.doc(0).Revision)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// A revision change (new ETag → 200) triggers a fresh reconcile with the new
// document.
func TestPoller_RevisionChangeTriggersReconcile(t *testing.T) {
	srv := &desiredServer{revision: 3, scaling: true}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	actor := &recordingActor{}
	p, tm := newTestPoller(t, ts.URL, actor)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	first := tm.next(t) // parked after poll 1 (rev 3, reconciled)
	srv.set(4, true)    // desired state moves
	first <- time.Now()
	trigger := tm.next(t) // parked after poll 2 (rev 4)

	if got := actor.calls(); got != 2 {
		t.Fatalf("reconciles = %d, want 2", got)
	}
	if got := actor.doc(1).Revision; got != 4 {
		t.Errorf("second acted revision = %d, want 4", got)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// 401 honors the FULL backoff (the sender's auth-error discipline): the wait
// comes from the backoff (attempt count grows), the actor is never invoked,
// and the error is retried — not fatal — because recovery (rotation/redeploy)
// is outside the agent's authority.
func TestPoller_AuthErrorBacksOffAndNeverActs(t *testing.T) {
	var calls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()

	actor := &recordingActor{}
	c := NewClient(ts.URL, "kubehz.in.net", testToken, "0.1.0", nil)
	p := NewPoller(c, actor, time.Minute, time.Second, time.Minute, nil)
	tm := newTimers()
	p.afterFunc = tm.afterFunc
	p.jitter = func() float64 { return 0 }

	// Deterministic backoff (no jitter) so waits are observable.
	var backoffs []*publisher.Backoff
	p.newBackoff = func() *publisher.Backoff {
		b := publisher.NewBackoff(time.Second, time.Minute)
		backoffs = append(backoffs, b)
		return b
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	// Three consecutive 401s → three backoff waits, attempt count climbing.
	tm.next(t) <- time.Now()
	tm.next(t) <- time.Now()
	trigger := tm.next(t)

	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("GETs = %d, want 3", got)
	}
	if got := actor.calls(); got != 0 {
		t.Errorf("actor invoked %d times on 401s — must never act", got)
	}
	if len(backoffs) != 1 || backoffs[0].Attempt() != 3 {
		t.Errorf("backoff attempts = %v, want one backoff at attempt 3 (full backoff honored)", backoffs)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// A transient executor failure re-runs against the CACHED doc on the next
// (304) poll — self-healing at the poll cadence without a revision bump.
func TestPoller_TransientFailureRetriesOn304(t *testing.T) {
	srv := &desiredServer{revision: 3, scaling: true}
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	actor := &recordingActor{retry: func(call int) bool { return call == 1 }}
	p, tm := newTestPoller(t, ts.URL, actor)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	tm.next(t) <- time.Now() // poll 1: 200, reconcile → retry=true
	tm.next(t) <- time.Now() // poll 2: 304, retryPending → reconcile cached → retry=false
	trigger := tm.next(t)    // poll 3: 304, no retry

	if got := actor.calls(); got != 2 {
		t.Errorf("reconciles = %d, want 2 (one initial + one retry, then stop)", got)
	}
	if actor.doc(1).Revision != 3 {
		t.Errorf("retry used revision %d, want cached 3", actor.doc(1).Revision)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}
