package leader

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// testConfig is the same election, sped up: seconds instead of the production
// half-minute, so a test observes a handover without waiting for one.
func testConfig(client *fake.Clientset, identity string) Config {
	return Config{
		Client:        client,
		Namespace:     "kubehz-system",
		Identity:      identity,
		LeaseDuration: 2 * time.Second,
		RenewDeadline: time.Second,
		RetryPeriod:   100 * time.Millisecond,
	}
}

// The holder acts: the callback runs with a live context, and cancelling the
// agent's context ends both the callback and Run.
func TestRun_LeadsUntilTheContextEnds(t *testing.T) {
	client := fake.NewClientset()
	leading := make(chan struct{})
	stopped := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, testConfig(client, "replica-a"), func(lctx context.Context) {
			close(leading)
			<-lctx.Done() // the acting loop runs until leadership ends
			close(stopped)
		})
	}()

	select {
	case <-leading:
	case <-time.After(5 * time.Second):
		t.Fatal("never acquired the lease")
	}

	lease, err := client.CoordinationV1().Leases("kubehz-system").
		Get(context.Background(), LeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("lease not created: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "replica-a" {
		t.Errorf("holder = %v, want replica-a", lease.Spec.HolderIdentity)
	}

	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the acting context was not cancelled when the agent stopped")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// THE POINT OF THE LEASE: a second replica must not act while another holds
// it. Both loops would otherwise patch the same MachineDeployment and delete
// machines against per-process guardrails neither can see.
func TestRun_SecondReplicaDoesNotAct(t *testing.T) {
	client := fake.NewClientset()
	firstLeading := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = Run(ctx, testConfig(client, "replica-a"), func(lctx context.Context) {
			close(firstLeading)
			<-lctx.Done()
		})
	}()
	select {
	case <-firstLeading:
	case <-time.After(5 * time.Second):
		t.Fatal("the first replica never acquired the lease")
	}

	var mu sync.Mutex
	secondActed := false
	go func() {
		_ = Run(ctx, testConfig(client, "replica-b"), func(lctx context.Context) {
			mu.Lock()
			secondActed = true
			mu.Unlock()
			<-lctx.Done()
		})
	}()

	// Well past one lease duration and several retry periods.
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	acted := secondActed
	mu.Unlock()
	if acted {
		t.Fatal("a second replica started acting while the lease was held")
	}

	lease, err := client.CoordinationV1().Leases("kubehz-system").
		Get(context.Background(), LeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "replica-a" {
		t.Errorf("holder = %v, want the original holder replica-a", lease.Spec.HolderIdentity)
	}
}

// Configuration the agent cannot honor must fail loudly instead of acting
// without a lease.
func TestRun_RefusesWithoutClientOrIdentity(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no client":    {Namespace: "kubehz-system", Identity: "a"},
		"no namespace": {Client: fake.NewClientset(), Identity: "a"},
		"no identity":  {Client: fake.NewClientset(), Namespace: "kubehz-system"},
		// The holder stops acting at most RenewDeadline+RetryPeriod after its
		// last renewal; a peer may take over LeaseDuration after it. Equal
		// values leave no time for the callback to return.
		"no drain margin": {
			Client: fake.NewClientset(), Namespace: "kubehz-system", Identity: "a",
			LeaseDuration: 2 * time.Second, RenewDeadline: 1500 * time.Millisecond, RetryPeriod: 500 * time.Millisecond,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var mu sync.Mutex
			acted := false
			err := Run(ctx, cfg, func(context.Context) {
				mu.Lock()
				acted = true
				mu.Unlock()
			})
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run returned %v, want a configuration error", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if acted {
				t.Error("acted despite an unusable election configuration")
			}
		})
	}
}

// leaseHolder returns the holder an update action writes ("" = a release).
func leaseHolder(a k8stesting.Action) (string, bool) {
	u, ok := a.(k8stesting.UpdateAction)
	if !ok {
		return "", false
	}
	l, ok := u.GetObject().(*coordinationv1.Lease)
	if !ok || l.Spec.HolderIdentity == nil {
		return "", ok
	}
	return *l.Spec.HolderIdentity, true
}

// A LOST renewal: acting stops and the callback is JOINED before anything
// else happens. No release is written while the callback still acts (client-go
// with ReleaseOnCancel releases first and cancels after), and Run does not
// contend again until the callback has returned, so two terms never act at
// once in one process.
func TestRun_LostLeaseStopsAndDrainsActingBeforeAnythingElse(t *testing.T) {
	client := fake.NewClientset()
	var failUpdates atomic.Bool
	var liveUncancelled atomic.Int32 // callbacks running that have not seen their cancel yet
	var releaseWhileLive atomic.Bool
	client.PrependReactor("update", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if holder, ok := leaseHolder(a); ok && holder == "" && liveUncancelled.Load() > 0 {
			releaseWhileLive.Store(true)
		}
		if failUpdates.Load() {
			return true, nil, apierrors.NewServiceUnavailable("renewal blocked by the test")
		}
		return false, nil, nil
	})

	var running, maxRunning atomic.Int32
	var terms atomic.Int32
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, testConfig(client, "replica-a"), func(lctx context.Context) {
			n := running.Add(1)
			defer running.Add(-1)
			for {
				m := maxRunning.Load()
				if n <= m || maxRunning.CompareAndSwap(m, n) {
					break
				}
			}
			liveUncancelled.Add(1)
			switch terms.Add(1) {
			case 1:
				close(firstStarted)
				<-lctx.Done()
				liveUncancelled.Add(-1)
				// The apiserver answers again, and this term is still
				// finishing an in-flight write.
				failUpdates.Store(false)
				time.Sleep(800 * time.Millisecond)
			case 2:
				close(secondStarted)
				<-lctx.Done()
				liveUncancelled.Add(-1)
			default:
				<-lctx.Done()
				liveUncancelled.Add(-1)
			}
		})
	}()

	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("never acquired the lease")
	}
	failUpdates.Store(true)

	select {
	case <-secondStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("a lost lease must be a pause: the replica never contended again")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if releaseWhileLive.Load() {
		t.Error("a lease release was written while the acting callback had not stopped — a peer could act beside it")
	}
	if got := maxRunning.Load(); got != 1 {
		t.Errorf("%d acting callbacks ran at once, want 1: Run contended again before the lost term had drained", got)
	}
}

// A CLEAN SHUTDOWN: the lease stays this replica's until the acting callback
// has returned, even when that takes longer than the lease duration, and only
// then is it released, so the successor takes over at once instead of
// waiting the lease out.
func TestRun_ShutdownKeepsTheLeaseUntilActingReturned(t *testing.T) {
	client := fake.NewClientset()
	cfg := testConfig(client, "replica-a") // lease 2 s

	var mu sync.Mutex
	var aReturned, bStarted, releasedAt time.Time
	aLeading := make(chan struct{})
	bLeading := make(chan struct{})
	// The release is the write that clears the holder. (Timing alone cannot
	// prove it: client-go compares lease records at one-second precision, so
	// an expiry can look almost as fast as a release.)
	client.PrependReactor("update", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if holder, ok := leaseHolder(a); ok && holder == "" {
			mu.Lock()
			if releasedAt.IsZero() {
				releasedAt = time.Now()
			}
			mu.Unlock()
		}
		return false, nil, nil
	})

	actx, acancel := context.WithCancel(context.Background())
	aDone := make(chan time.Time, 1)
	go func() {
		_ = Run(actx, cfg, func(lctx context.Context) {
			close(aLeading)
			<-lctx.Done()
			// A write that ignores the cancel for longer than the lease.
			time.Sleep(3 * time.Second)
			mu.Lock()
			aReturned = time.Now()
			mu.Unlock()
		})
		aDone <- time.Now()
	}()
	select {
	case <-aLeading:
	case <-time.After(5 * time.Second):
		t.Fatal("replica-a never acquired the lease")
	}

	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go func() {
		_ = Run(bctx, testConfig(client, "replica-b"), func(lctx context.Context) {
			mu.Lock()
			bStarted = time.Now()
			mu.Unlock()
			close(bLeading)
			<-lctx.Done()
		})
	}()
	time.Sleep(300 * time.Millisecond) // replica-b contends and sees replica-a hold it
	acancel()

	var aRunReturned time.Time
	select {
	case aRunReturned = <-aDone:
	case <-time.After(10 * time.Second):
		t.Fatal("replica-a's Run did not return after cancel")
	}
	select {
	case <-bLeading:
	case <-time.After(10 * time.Second):
		t.Fatal("replica-b never took over")
	}

	mu.Lock()
	defer mu.Unlock()
	if aRunReturned.Before(aReturned) {
		t.Error("Run returned before its acting callback did")
	}
	if !bStarted.After(aReturned) {
		t.Fatalf("replica-b started acting %s BEFORE replica-a's callback returned — two actors at once",
			aReturned.Sub(bStarted))
	}
	if releasedAt.IsZero() {
		t.Fatal("replica-a never released the lease on a clean shutdown — the successor waits it out")
	}
	if releasedAt.Before(aReturned) {
		t.Errorf("replica-a released the lease %s before its acting callback returned", aReturned.Sub(releasedAt))
	}
}

// The reporting side must see when ANOTHER replica holds the lease, and stop
// seeing it once that replica is gone.
func TestObserver_SeesThePeerWhileItHoldsTheLease(t *testing.T) {
	client := fake.NewClientset()
	obsA, obsB := NewObserver(), NewObserver()

	aLeading := make(chan struct{})
	actx, acancel := context.WithCancel(context.Background())
	defer acancel()
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		cfg := testConfig(client, "replica-a")
		cfg.Observer = obsA
		_ = Run(actx, cfg, func(lctx context.Context) {
			close(aLeading)
			<-lctx.Done()
		})
	}()
	select {
	case <-aLeading:
	case <-time.After(5 * time.Second):
		t.Fatal("replica-a never acquired the lease")
	}

	bLeading := make(chan struct{})
	bctx, bcancel := context.WithCancel(context.Background())
	defer bcancel()
	go func() {
		cfg := testConfig(client, "replica-b")
		cfg.Observer = obsB
		_ = Run(bctx, cfg, func(lctx context.Context) {
			close(bLeading)
			<-lctx.Done()
		})
	}()
	obsB.WaitFirstRead(context.Background(), 5*time.Second)

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor("the standby never saw the holder", obsB.PeerHolds)
	if obsA.PeerHolds() {
		t.Error("the holder sees itself as a peer — it would stop reporting its own actions")
	}

	acancel()
	<-aDone
	select {
	case <-bLeading:
	case <-time.After(5 * time.Second):
		t.Fatal("replica-b never took over")
	}
	waitFor("the new holder still sees a peer", func() bool { return !obsB.PeerHolds() })
}

// A holder that stops renewing stops counting after one lease duration, by
// the observer's own clock. Re-reading the same record is not a renewal.
func TestObserver_AStaleRecordStopsCounting(t *testing.T) {
	o := NewObserver()
	now := time.Unix(1_000_000, 0)
	o.now = func() time.Time { return now }
	_ = o.wrap(nil, "me")

	peer := &resourcelock.LeaderElectionRecord{HolderIdentity: "peer", LeaseDurationSeconds: 30}
	o.read(peer, []byte("v1"), nil)
	if !o.PeerHolds() {
		t.Fatal("a fresh peer record does not count")
	}
	now = now.Add(29 * time.Second)
	o.read(peer, []byte("v1"), nil)
	if !o.PeerHolds() {
		t.Error("the peer stopped counting before its lease duration passed")
	}
	now = now.Add(2 * time.Second)
	o.read(peer, []byte("v1"), nil)
	if o.PeerHolds() {
		t.Error("an unchanged record still counts after its lease duration — a dead holder would silence this replica forever")
	}
	o.read(peer, []byte("v2"), nil)
	if !o.PeerHolds() {
		t.Error("a renewal (a changed record) does not count")
	}
	o.read(nil, nil, errors.New("forbidden"))
	now = now.Add(31 * time.Second)
	if o.PeerHolds() {
		t.Error("a record this replica can no longer read still counts after its lease duration")
	}
	o.read(&resourcelock.LeaderElectionRecord{HolderIdentity: "me", LeaseDurationSeconds: 30}, []byte("v3"), nil)
	if o.PeerHolds() {
		t.Error("this replica's own record counts as a peer")
	}
	o.read(&resourcelock.LeaderElectionRecord{LeaseDurationSeconds: 1}, []byte("v4"), nil)
	if o.PeerHolds() {
		t.Error("a released record (no holder) counts as a peer")
	}
	var none *Observer
	if none.PeerHolds() {
		t.Error("a nil observer sees a peer")
	}
}

// The identity is per PROCESS: two agents that share a pod name (a restart, a
// hostname collision) must never look like one holder.
func TestIdentity_IsUniquePerProcess(t *testing.T) {
	a, b := Identity("kubehz-live-agent-abc"), Identity("kubehz-live-agent-abc")
	if a == b {
		t.Fatalf("identity %q repeated — a stale holder would look like this process", a)
	}
	if !strings.HasPrefix(a, "kubehz-live-agent-abc_") {
		t.Errorf("identity = %q, want the pod name first (it names the replica in kubectl)", a)
	}
	if Identity("") == "" {
		t.Error("an empty pod name must still produce an identity (hostname fallback)")
	}
}

// syncBuffer collects log output written from the election goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
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

// Without the Lease RBAC the agent must NOT act — and must say so in its own
// log. The election library retries a denied lease exactly like a held one, so
// the agent cannot tell them apart; the one warning names both causes.
func TestRun_WithoutLeaseRBACItReportsAndDoesNotAct(t *testing.T) {
	client := fake.NewClientset()
	client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
			LeaseName, errors.New("no RBAC for leases"))
	})

	var logs syncBuffer
	cfg := testConfig(client, "replica-a")
	cfg.AcquireWarnAfter = 200 * time.Millisecond
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var mu sync.Mutex
	acted := false
	_ = Run(ctx, cfg, func(lctx context.Context) {
		mu.Lock()
		acted = true
		mu.Unlock()
		<-lctx.Done()
	})

	mu.Lock()
	didAct := acted
	mu.Unlock()
	if didAct {
		t.Fatal("acted without ever holding the lease")
	}
	out := logs.String()
	if !strings.Contains(out, "NOT acting") {
		t.Errorf("log = %q, want the agent to say it is reporting but not acting", out)
	}
	if !strings.Contains(out, "deploy/managed") {
		t.Errorf("log = %q, want the missing RBAC overlay named as a cause", out)
	}
}
