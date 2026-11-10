package desired

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// recordingActor records every doc it is handed and returns scripted retry
// values (default false). It also records the stale-document refusals, in the
// same ordered trail as the reconciles, so a test can assert that acting STOPS
// where the refusals start.
type recordingActor struct {
	mu       sync.Mutex
	docs     []*Doc
	trail    []string // "act" / "refuse", in call order
	refusals []string // refusal reasons, in call order
	retry    func(call int) bool
}

func (a *recordingActor) Reconcile(_ context.Context, doc *Doc) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.docs = append(a.docs, doc)
	a.trail = append(a.trail, "act")
	if a.retry != nil {
		return a.retry(len(a.docs))
	}
	return false
}

func (a *recordingActor) RefuseStale(_ context.Context, _ *Doc, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.trail = append(a.trail, "refuse")
	a.refusals = append(a.refusals, reason)
}

func (a *recordingActor) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.docs)
}

func (a *recordingActor) refused() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.refusals...)
}

func (a *recordingActor) calltrail() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.trail...)
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
		_, _ = fmt.Fprintf(w, `{"revision":%d,"kubernetesVersion":null,"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":3}],"execution":{"scaling":%t,"upgrades":false}}`, rev, scaling)
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

// healingBody is a healing-ARMED document (both bits), the case that asks for
// a re-run on every single tick.
func healingBody(revision int) string {
	return fmt.Sprintf(`{"revision":%d,"kubernetesVersion":null,`+
		`"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":3}],`+
		`"execution":{"scaling":false,"upgrades":false,"healing":true},`+
		`"healing":{"enabled":true,"maxUnhealthy":2,"nodeStartupTimeoutSeconds":600,`+
		`"unhealthyAfterSeconds":300,"cooldownSeconds":900}}`, revision)
}

// drivePolls releases n parked waits, i.e. lets the poller run n more polls.
func drivePolls(t *testing.T, tm *timers, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		tm.next(t) <- time.Now()
	}
}

// firstRefusal reports the index of the first refusal in the call trail
// (-1 = none).
func firstRefusal(trail []string) int {
	for i, s := range trail {
		if s == "refuse" {
			return i
		}
	}
	return -1
}

// A FROZEN CACHE — anything answering 304 forever, proxy or CDN — must not
// keep the healer acting. Healing asks for a re-run on every tick, so without
// a freshness bound the agent would delete machines on intent nobody
// re-affirmed, for as long as the frozen copy is served. Acting must stop at
// the bound, the refusal must be REPORTED, and one served document must
// restore acting.
func TestPoller_FrozenCacheStopsActingAndReportsRefusal(t *testing.T) {
	var mu sync.Mutex
	served := 0
	frozen := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		first := served == 0
		served++
		stillFrozen := frozen
		mu.Unlock()
		if first || !stillFrozen {
			w.Header().Set("ETag", `"3-001"`)
			_, _ = fmt.Fprint(w, healingBody(3))
			return
		}
		// The frozen copy: 304 to EVERY request, conditional or not.
		w.WriteHeader(http.StatusNotModified)
	}))
	defer ts.Close()

	// Healing's real behaviour: every pass asks to be re-run.
	actor := &recordingActor{retry: func(int) bool { return true }}
	p, tm := newTestPoller(t, ts.URL, actor)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	const polls = 20
	drivePolls(t, tm, polls)
	trigger := tm.next(t)

	trail := actor.calltrail()
	acts := actor.calls()
	refusals := actor.refused()

	if acts < 2 {
		t.Fatalf("reconciles = %d — the fresh document must still drive the healer", acts)
	}
	if acts > staleHealingTicks+1 {
		t.Errorf("reconciles = %d over %d polls, want at most %d (the healing freshness bound)",
			acts, polls, staleHealingTicks+1)
	}
	if len(refusals) == 0 {
		t.Fatalf("no refusal reported after %d polls of a frozen 304 (trail %v)", polls, trail)
	}
	if !strings.Contains(refusals[0], "does not act on intent nobody re-affirmed") {
		t.Errorf("refusal reason = %q, want it to say why the agent stopped", refusals[0])
	}
	if i := firstRefusal(trail); i >= 0 {
		for _, step := range trail[i:] {
			if step == "act" {
				t.Fatalf("acted again after refusing; trail = %v", trail)
			}
		}
	}

	// A served document re-affirms the intent: acting resumes at once.
	mu.Lock()
	frozen = false
	mu.Unlock()
	before := actor.calls()
	trigger <- time.Now()
	trigger = tm.next(t)
	if actor.calls() != before+1 {
		t.Errorf("reconciles after a served document = %d, want %d — a fresh document must clear the refusal",
			actor.calls(), before+1)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// The bound must not break healing on a HEALTHY platform. A revision that
// never changes 304s every conditional poll, so the agent revalidates
// unconditionally (no If-None-Match, no-cache) — the origin's 200 re-affirms
// the intent and the healer keeps running, forever, with no refusal.
func TestPoller_RevalidationKeepsHealingAlive(t *testing.T) {
	var mu sync.Mutex
	unconditional := 0
	var revalidateHeaders http.Header
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"3-001"`)
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		mu.Lock()
		unconditional++
		if revalidateHeaders == nil {
			revalidateHeaders = r.Header.Clone()
		}
		mu.Unlock()
		_, _ = fmt.Fprint(w, healingBody(3))
	}))
	defer ts.Close()

	actor := &recordingActor{retry: func(int) bool { return true }}
	p, tm := newTestPoller(t, ts.URL, actor)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	const polls = 20
	drivePolls(t, tm, polls)
	trigger := tm.next(t)

	mu.Lock()
	revalidations, headers := unconditional, revalidateHeaders
	mu.Unlock()

	// The first poll is unconditional too (nothing cached yet).
	if revalidations < 1+polls/(staleRevalidateTicks+1) {
		t.Errorf("unconditional revalidations = %d over %d polls, want at least %d (one every %d polls)",
			revalidations, polls, 1+polls/(staleRevalidateTicks+1), staleRevalidateTicks+1)
	}
	if headers.Get("Cache-Control") != "no-cache" {
		t.Errorf("revalidation Cache-Control = %q, want no-cache (no intermediary may answer it)",
			headers.Get("Cache-Control"))
	}
	if len(actor.refused()) != 0 {
		t.Errorf("refusals = %v, want none — a platform that keeps serving the document is not stale", actor.refused())
	}
	if got := actor.calls(); got != polls+1 {
		t.Errorf("reconciles = %d, want %d (healing runs on every poll while the document stays fresh)", got, polls+1)
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// syncBuffer is a goroutine-safe bytes.Buffer for capturing the poller
// goroutine's log output under -race.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The "desired state pulled" log must carry a POSITIVE healing signal: the
// armed flag (execution.healing && healing.enabled — the exact conjunction the
// executor acts on) plus the effective policy numbers, so arming/policy
// changes are observable without diffing documents.
func TestPoller_LogsHealingArmStateAndPolicy(t *testing.T) {
	for name, tc := range map[string]struct {
		execHealing   bool
		policyEnabled bool
		wantArmed     string
	}{
		"armed":             {execHealing: true, policyEnabled: true, wantArmed: "healing=true"},
		"policy bit off":    {execHealing: true, policyEnabled: false, wantArmed: "healing=false"},
		"execution bit off": {execHealing: false, policyEnabled: true, wantArmed: "healing=false"},
	} {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("ETag", `"9-001"`)
				_, _ = fmt.Fprintf(w, `{"revision":9,"kubernetesVersion":null,"workerPools":[],`+
					`"execution":{"scaling":false,"upgrades":false,"healing":%t},`+
					`"healing":{"enabled":%t,"maxUnhealthy":2,"nodeStartupTimeoutSeconds":600,`+
					`"unhealthyAfterSeconds":300,"cooldownSeconds":900}}`,
					tc.execHealing, tc.policyEnabled)
			}))
			defer ts.Close()

			var buf syncBuffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			c := NewClient(ts.URL, "kubehz.in.net", testToken, "0.1.0", nil)
			p := NewPoller(c, &recordingActor{}, time.Minute, time.Second, time.Minute, logger)
			tm := newTimers()
			p.afterFunc = tm.afterFunc
			p.jitter = func() float64 { return 0 }

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { defer wg.Done(); p.Run(ctx) }()

			trigger := tm.next(t) // parked after poll 1 (200, logged)
			cancel()
			trigger <- time.Now()
			wg.Wait()

			out := buf.String()
			if !strings.Contains(out, "desired state pulled") {
				t.Fatalf("no pull log emitted:\n%s", out)
			}
			for _, want := range []string{
				tc.wantArmed,
				"healingMaxUnhealthy=2",
				"healingUnhealthyAfterSeconds=300",
				"healingCooldownSeconds=900",
			} {
				if !strings.Contains(out, want) {
					t.Errorf("pull log missing %q:\n%s", want, out)
				}
			}
		})
	}
}

// After a rejection the poller re-reads the token (a re-enrollment rotates
// the Secret under the running pod) and, when it changed, polls again at
// once instead of backing off with the dead token.
func TestPoller_AuthErrorReloadsTokenFromFile(t *testing.T) {
	const rotated = "khz_agt_" + "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	var mu sync.Mutex
	var bearers []string
	srv := &desiredServer{revision: 1}
	inner := srv.handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		bearers = append(bearers, auth)
		mu.Unlock()
		if auth != "Bearer "+rotated {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		inner(w, r)
	}))
	defer ts.Close()

	tokenFile := t.TempDir() + "/agent-token"
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	actor := &recordingActor{}
	p, tm := newTestPoller(t, ts.URL, actor)
	p.client.SetTokenReloader(func(context.Context) (string, error) {
		return config.ReadTokenFile(os.ReadFile, tokenFile)
	})
	// The "kubelet" refreshes the projected file with the rotated token.
	if err := os.WriteFile(tokenFile, []byte(rotated+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()

	// First GET 401s with the old token; the reload makes the poller retry
	// immediately (no wait in between), so the first park is AFTER the 200.
	trigger := tm.next(t)

	mu.Lock()
	got := append([]string(nil), bearers...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "Bearer "+testToken || got[1] != "Bearer "+rotated {
		t.Fatalf("bearers = %v, want [old, rotated]", got)
	}
	if actor.calls() != 1 || actor.doc(0).Revision != 1 {
		t.Errorf("actor calls = %d, want 1 reconcile with the doc pulled by the rotated token", actor.calls())
	}

	cancel()
	trigger <- time.Now()
	wg.Wait()
}

// runPoller starts p and returns a stop function that ends it.
func runPoller(t *testing.T, p *Poller, tm *timers) (stop func(trigger chan time.Time)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.Run(ctx) }()
	return func(trigger chan time.Time) {
		cancel()
		trigger <- time.Now()
		wg.Wait()
	}
}

// actsAfter reports whether the trail has an "act" at or after index i.
func actsAfter(trail []string, i int) bool {
	for _, s := range trail[i:] {
		if s == "act" {
			return true
		}
	}
	return false
}

// A CACHED 200 must not stand in for the platform. The origin served an armed
// document (no ETag) once and then went away; a cache keeps the copy and
// serves it to every request that does not say no-cache. Every agent request
// must say no-cache, so the cache must ask the dead origin, and the agent
// acts on nothing it served after that and stops at the bound.
func TestPoller_CachedOKWithoutETagDoesNotKeepActing(t *testing.T) {
	var mu sync.Mutex
	originUp := true
	var missingNoCache int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		up := originUp
		originUp = false
		noCache := r.Header.Get("Cache-Control") == "no-cache"
		if !noCache {
			missingNoCache++
		}
		mu.Unlock()
		switch {
		case up:
			_, _ = fmt.Fprint(w, healingBody(3)) // the origin: no ETag
		case !noCache:
			_, _ = fmt.Fprint(w, healingBody(3)) // the cache's stored copy
		default:
			w.WriteHeader(http.StatusGatewayTimeout) // the cache asked the dead origin
		}
	}))
	defer ts.Close()

	actor := &recordingActor{retry: func(int) bool { return true }}
	p, tm := newTestPoller(t, ts.URL, actor)
	stop := runPoller(t, p, tm)

	drivePolls(t, tm, staleHealingTicks+5)
	trigger := tm.next(t)

	mu.Lock()
	missing := missingNoCache
	mu.Unlock()
	if missing != 0 {
		t.Errorf("%d requests went out without Cache-Control: no-cache — a cache could answer them from a stored copy", missing)
	}
	if got := actor.calls(); got != 1 {
		t.Errorf("reconciles = %d, want 1: after the origin went away nothing may drive the healer (trail %v)",
			got, actor.calltrail())
	}
	if len(actor.refused()) == 0 {
		t.Errorf("no refusal reported after %d polls without the platform (trail %v)", staleHealingTicks+5, actor.calltrail())
	}
	stop(trigger)
}

// Only an UNCONDITIONAL answer proves the platform re-affirmed the document,
// and revalidation has its own cadence. Here a broken cache answers every
// conditional request with its stored 200 while the origin is down: those
// 200s must not reset the freshness count, and the agent must stop acting at
// the healing bound.
func TestPoller_OrdinaryOKDoesNotResetFreshness(t *testing.T) {
	var mu sync.Mutex
	first := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		origin := first
		first = false
		mu.Unlock()
		if origin || r.Header.Get("If-None-Match") != "" {
			w.Header().Set("ETag", `"3-001"`)
			_, _ = fmt.Fprint(w, healingBody(3))
			return
		}
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer ts.Close()

	actor := &recordingActor{retry: func(int) bool { return true }}
	p, tm := newTestPoller(t, ts.URL, actor)
	stop := runPoller(t, p, tm)

	const polls = 3 * staleHealingTicks
	drivePolls(t, tm, polls)
	trigger := tm.next(t)

	trail := actor.calltrail()
	i := firstRefusal(trail)
	if i < 0 {
		t.Fatalf("no refusal after %d polls without one successful revalidation (trail %v)", polls, trail)
	}
	if actsAfter(trail, i) {
		t.Errorf("acted again after refusing (trail %v)", trail)
	}
	if got := actor.calls(); got > staleHealingTicks+1 {
		t.Errorf("reconciles = %d, want at most %d (the healing freshness bound)", got, staleHealingTicks+1)
	}
	stop(trigger)
}

// A revalidation refreshes FRESHNESS; it is not new intent. A scaling pass
// that converged (retry=false) must not run again when the same document
// comes back on a revalidation, or it would undo a manual change made since.
// A changed document (here only an execution flag, same revision and ETag)
// is new intent and runs.
func TestPoller_RevalidationDoesNotReplayConvergedIntent(t *testing.T) {
	var mu sync.Mutex
	upgrades := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		up := upgrades
		mu.Unlock()
		w.Header().Set("ETag", `"3-10"`) // deliberately unchanged below
		if r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprintf(w, `{"revision":3,"kubernetesVersion":null,`+
			`"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":3}],`+
			`"execution":{"scaling":true,"upgrades":%t}}`, up)
	}))
	defer ts.Close()

	actor := &recordingActor{} // converged: retry=false
	p, tm := newTestPoller(t, ts.URL, actor)
	stop := runPoller(t, p, tm)

	// Polls 1, 7, 13 and 19 are unconditional: three revalidations of the
	// same document after the first pull. Park after poll 19.
	drivePolls(t, tm, 3*(staleRevalidateTicks+1))
	parked := tm.next(t)
	if got := actor.calls(); got != 1 {
		t.Fatalf("reconciles = %d over three revalidations of the same document, want 1 — converged intent was replayed", got)
	}

	mu.Lock()
	upgrades = true
	mu.Unlock()
	parked <- time.Now()
	drivePolls(t, tm, staleRevalidateTicks) // poll 25 is the next revalidation
	trigger := tm.next(t)
	if got := actor.calls(); got != 2 {
		t.Fatalf("reconciles = %d, want 2: a changed document (same revision) is new intent", got)
	}
	if !actor.doc(1).Execution.Upgrades {
		t.Error("the second pass did not get the changed document")
	}
	stop(trigger)
}

// A REJECTION revokes what the platform authorized before. After it, a 304 —
// even one that answers a validator from before the rejection — must not
// bring the old document back; only a new document restores acting.
func TestPoller_RejectionRevokesTheCachedDocument(t *testing.T) {
	for name, reject := range map[string]func(w http.ResponseWriter){
		"403": func(w http.ResponseWriter) { w.WriteHeader(http.StatusForbidden) },
		"404": func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
		"invalid document": func(w http.ResponseWriter) {
			w.Header().Set("ETag", `"4-001"`)
			_, _ = fmt.Fprint(w, `{"revision":4,"kubernetesVersion":null,"workerPools":[{"name":"pool-a"}],`+
				`"execution":{"scaling":true,"upgrades":false}}`)
		},
		"wrong field type": func(w http.ResponseWriter) {
			w.Header().Set("ETag", `"4-001"`)
			_, _ = fmt.Fprint(w, `{"revision":4,"kubernetesVersion":null,"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":"3"}],`+
				`"execution":{"scaling":true,"upgrades":false}}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			step := 0
			serveNew := false
			var inm []string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				step++
				n, fresh := step, serveNew
				inm = append(inm, r.Header.Get("If-None-Match"))
				mu.Unlock()
				switch {
				case n == 1:
					w.Header().Set("ETag", `"3-001"`)
					_, _ = fmt.Fprint(w, healingBody(3))
				case n == 2:
					reject(w)
				case fresh:
					w.Header().Set("ETag", `"5-001"`)
					_, _ = fmt.Fprint(w, healingBody(5))
				default:
					w.WriteHeader(http.StatusNotModified) // a cache, whatever was asked
				}
			}))
			defer ts.Close()

			actor := &recordingActor{retry: func(int) bool { return true }}
			p, tm := newTestPoller(t, ts.URL, actor)
			stop := runPoller(t, p, tm)

			drivePolls(t, tm, 4) // polls 2..5: the rejection, then 304s
			parked := tm.next(t)
			if got := actor.calls(); got != 1 {
				t.Fatalf("reconciles = %d, want 1: nothing may act after the rejection until a new document (trail %v)",
					got, actor.calltrail())
			}
			mu.Lock()
			afterReject := inm[2]
			serveNew = true
			mu.Unlock()
			if afterReject != "" {
				t.Errorf("the poll after the rejection sent If-None-Match %q — the revoked document's validator", afterReject)
			}

			parked <- time.Now()
			trigger := tm.next(t)
			if got := actor.calls(); got != 2 || actor.doc(1).Revision != 5 {
				t.Errorf("reconciles = %d, want 2 with the new revision 5: a new document restores acting", got)
			}
			stop(trigger)
		})
	}
}
