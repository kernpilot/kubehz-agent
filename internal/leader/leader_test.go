package leader

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/kernpilot/kubehz-agent/internal/leader/leasetest"
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

// A holder that stops renewing stops counting once a read finds its record
// unchanged for one lease duration, by the observer's own clock. Re-reading
// the same record is not a renewal.
func TestObserver_AStaleRecordStopsCounting(t *testing.T) {
	o := NewObserver()
	now := time.Unix(1_000_000, 0)
	o.now = func() time.Time { return now }
	_ = o.wrap(nil, "me", LeaseName)

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

// A read that FAILS proves nothing about the holder. While this replica
// cannot read the Lease, the peer it last saw keeps counting, however long
// that takes: a healthy holder is still renewing, and this replica's beats
// would erase its action reports. Only a successful read that finds the
// record unchanged for a full lease duration ends it.
func TestObserver_AFailedReadKeepsThePeer(t *testing.T) {
	o := NewObserver()
	now := time.Unix(1_000_000, 0)
	o.now = func() time.Time { return now }
	_ = o.wrap(nil, "me", LeaseName)

	peer := &resourcelock.LeaderElectionRecord{HolderIdentity: "peer", LeaseDurationSeconds: 30}
	o.read(peer, []byte("v1"), nil)
	for i := 0; i < 5; i++ {
		now = now.Add(31 * time.Second)
		o.read(nil, nil, errors.New("connection refused"))
	}
	if !o.PeerHolds() {
		t.Fatal("the peer stopped counting while this replica could not read the Lease — its beats would erase the holder's reports")
	}
	o.read(peer, []byte("v1"), nil)
	if o.PeerHolds() {
		t.Error("an unchanged record read again after its lease duration still counts")
	}
}

// A NotFound is the one failed read that IS evidence: the apiserver says
// there is no Lease, so no peer holds it. The peer the replica last saw stops
// counting at once. Other errors stay inconclusive.
func TestObserver_AMissingLeaseIsNoPeer(t *testing.T) {
	o := NewObserver()
	now := time.Unix(1_000_000, 0)
	o.now = func() time.Time { return now }
	_ = o.wrap(nil, "me", LeaseName)

	peer := &resourcelock.LeaderElectionRecord{HolderIdentity: "peer", LeaseDurationSeconds: 30}
	o.read(peer, []byte("v1"), nil)
	o.read(nil, nil, apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, LeaseName, errors.New("denied")))
	if !o.PeerHolds() {
		t.Fatal("a Forbidden read ended the peer's count; it proves nothing about the holder")
	}
	o.read(nil, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, LeaseName))
	if o.PeerHolds() {
		t.Fatal("the peer still counts after the apiserver said the Lease does not exist: the replica would stay silent for good")
	}
	o.read(peer, []byte("v2"), nil)
	if !o.PeerHolds() {
		t.Error("a peer that creates the Lease again does not count")
	}
}

// Only the apiserver's own NotFound for the Lease ends the peer's count. Every
// case is an HTTP 404 that client-go reports as NotFound, read through a real
// REST client and the election's own lock. A 404 page from a proxy or a
// router, or a NotFound for another object, says nothing about the Lease: if
// it ended the count, the standby's beats would erase the reports of a holder
// that still acts.
func TestObserver_OnlyTheLeasesOwnNotFoundIsNoPeer(t *testing.T) {
	status := func(details *metav1.StatusDetails) string {
		b, err := json.Marshal(metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure, Reason: metav1.StatusReasonNotFound,
			Code: http.StatusNotFound, Message: "not found", Details: details,
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	leaseDetails := func(name string) *metav1.StatusDetails {
		return &metav1.StatusDetails{Group: "coordination.k8s.io", Kind: "leases", Name: name}
	}
	cases := []struct {
		name, contentType, body string
		noPeer                  bool
	}{
		{"an HTML page", "text/html", "<html><body><h1>404 Not Found</h1></body></html>", false},
		{"a plain-text page", "text/plain; charset=utf-8", "404 page not found\n", false},
		{"no content type", "", "not here", false},
		{"JSON that is not a Status", "application/json", `{"error":"no route"}`, false},
		{"a Status for another Lease", "application/json", status(leaseDetails("another-lease")), false},
		{"a Status for another kind", "application/json", status(&metav1.StatusDetails{Kind: "namespaces", Name: LeaseName}), false},
		{"a Status without details", "application/json", status(nil), false},
		{"the apiserver's NotFound for the Lease", "application/json", status(leaseDetails(LeaseName)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.contentType != "" {
					w.Header().Set("Content-Type", tc.contentType)
				}
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
			if err != nil {
				t.Fatalf("client: %v", err)
			}
			o := NewObserver()
			lock := o.wrap(&resourcelock.LeaseLock{
				LeaseMeta:  metav1.ObjectMeta{Name: LeaseName, Namespace: "kubehz-system"},
				Client:     client.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{Identity: "me"},
			}, "me", LeaseName)
			o.read(&resourcelock.LeaderElectionRecord{HolderIdentity: "peer", LeaseDurationSeconds: 30}, []byte("v1"), nil)

			_, _, err = lock.Get(context.Background())
			if !apierrors.IsNotFound(err) {
				t.Fatalf("client-go did not report this 404 as NotFound (precondition): %v", err)
			}
			if got := !o.PeerHolds(); got != tc.noPeer {
				if tc.noPeer {
					t.Errorf("the apiserver's NotFound for the Lease did not end the peer's count")
				} else {
					t.Errorf("this 404 ended the peer's count; it proves nothing about the Lease (err: %v)", err)
				}
			}
		})
	}
}

// startLeading runs replica-a's Run in the background and waits until it
// acts. It returns the cancel for Run and a channel that yields Run's return
// time.
func startLeading(t *testing.T, cfg Config, onLeading func(context.Context)) (context.CancelFunc, <-chan time.Time) {
	t.Helper()
	leading := make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan time.Time, 1)
	go func() {
		_ = Run(ctx, cfg, func(lctx context.Context) {
			once.Do(func() { close(leading) })
			onLeading(lctx)
		})
		done <- time.Now()
	}()
	select {
	case <-leading:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("never acquired the lease")
	}
	return cancel, done
}

func storedHolder(t *testing.T, client *fake.Clientset) string {
	t.Helper()
	l, err := client.CoordinationV1().Leases("kubehz-system").Get(context.Background(), LeaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// The release on shutdown never touches a Lease a successor holds. Here the
// successor took the Lease while replica-a was still finishing its acting
// callback; replica-a's release reads it and must leave it alone.
func TestRun_ReleaseLeavesASuccessorsLeaseAlone(t *testing.T) {
	client := fake.NewClientset()
	leasetest.EnforceResourceVersion(client)

	cancel, done := startLeading(t, testConfig(client, "replica-a"), func(lctx context.Context) {
		<-lctx.Done()
		// replica-b takes the Lease before this callback returns.
		leases := client.CoordinationV1().Leases("kubehz-system")
		for i := 0; i < 50; i++ {
			l, err := leases.Get(context.Background(), LeaseName, metav1.GetOptions{})
			if err != nil {
				t.Errorf("get lease: %v", err)
				return
			}
			b, ten := "replica-b", int32(10)
			now := metav1.NewMicroTime(time.Now())
			l.Spec.HolderIdentity, l.Spec.LeaseDurationSeconds = &b, &ten
			l.Spec.AcquireTime, l.Spec.RenewTime = &now, &now
			if _, err = leases.Update(context.Background(), l, metav1.UpdateOptions{}); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond) // a renewal came between; read again
		}
		t.Error("the test could not hand the Lease to replica-b")
	})
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := storedHolder(t, client); got != "replica-b" {
		t.Errorf("stored holder = %q after replica-a's shutdown, want replica-b: the release cleared a successor's Lease", got)
	}
}

// The release is a compare-and-swap. A successor can take the Lease between
// the release's read and its write; the write then carries a resourceVersion
// that is no longer the stored one, and it must fail instead of clearing the
// successor's Lease.
func TestRun_ReleaseIsACompareAndSwap(t *testing.T) {
	client := fake.NewClientset()
	versions := leasetest.EnforceResourceVersion(client)

	var callbackReturned, interposed atomic.Bool
	// The first read after the callback returned is the release's own read
	// (the elector renews without reading while it holds the lease). It
	// answers with replica-a's Lease and, at the same moment, lets replica-b
	// take the stored one.
	client.PrependReactor("get", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if !callbackReturned.Load() || interposed.Swap(true) {
			return false, nil, nil
		}
		obj, err := client.Tracker().Get(leasetest.LeaseGVR, a.GetNamespace(), LeaseName)
		if err != nil {
			t.Errorf("tracker get: %v", err)
			return false, nil, nil
		}
		mine := obj.(*coordinationv1.Lease)
		theirs := mine.DeepCopy()
		b, ten := "replica-b", int32(10)
		now := metav1.NewMicroTime(time.Now())
		theirs.Spec.HolderIdentity, theirs.Spec.LeaseDurationSeconds = &b, &ten
		theirs.Spec.AcquireTime, theirs.Spec.RenewTime = &now, &now
		theirs.ResourceVersion = versions.Next()
		if err := client.Tracker().Update(leasetest.LeaseGVR, theirs, a.GetNamespace()); err != nil {
			t.Errorf("tracker update: %v", err)
		}
		return true, mine, nil
	})

	cancel, done := startLeading(t, testConfig(client, "replica-a"), func(lctx context.Context) {
		<-lctx.Done()
		callbackReturned.Store(true)
	})
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if !interposed.Load() {
		t.Fatal("the release never read the Lease")
	}
	if got := storedHolder(t, client); got != "replica-b" {
		t.Errorf("stored holder = %q, want replica-b: the release overwrote a Lease that changed after it was read", got)
	}
}

// STEP-DOWN: once MayLead says this replica must not hold the lease, acting
// stops, the callback is joined, and only then is the lease released. Run
// then stays out of the race while MayLead says no, and contends again once
// it says yes.
func TestRun_StepsDownWhileItMayNotLead(t *testing.T) {
	client := fake.NewClientset()
	var mu sync.Mutex
	var returnedAt, releasedAt time.Time
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

	var unfit atomic.Bool
	var terms atomic.Int32
	second := make(chan struct{})
	cfg := testConfig(client, "replica-a")
	cfg.MayLead = func() error {
		if unfit.Load() {
			return errors.New("the test says no")
		}
		return nil
	}
	cancel, done := startLeading(t, cfg, func(lctx context.Context) {
		n := terms.Add(1)
		if n == 2 {
			close(second)
		}
		<-lctx.Done()
		if n == 1 {
			time.Sleep(300 * time.Millisecond) // an in-flight write finishing
			mu.Lock()
			returnedAt = time.Now()
			mu.Unlock()
		}
	})
	defer func() {
		cancel()
		<-done
	}()

	unfit.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		released := !releasedAt.IsZero()
		mu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the holder never released the lease after MayLead said no")
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	if returnedAt.IsZero() || releasedAt.Before(returnedAt) {
		t.Errorf("the lease was released before the acting callback returned (returned %v, released %v)", returnedAt, releasedAt)
	}
	mu.Unlock()

	// Out of the race: a released lease is free, and a contending replica
	// would take it back within one retry period.
	time.Sleep(time.Second)
	if got := terms.Load(); got != 1 {
		t.Fatalf("%d terms while MayLead said no, want 1: the replica contended again", got)
	}
	if got := storedHolder(t, client); got != "" {
		t.Errorf("stored holder = %q while MayLead said no, want none", got)
	}

	unfit.Store(false)
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("the replica never contended again after MayLead said yes")
	}
}

// Out of the race, a replica still reads the Lease, so its Observer sees the
// holder: the elector, which does those reads otherwise, does not run.
func TestRun_OutOfTheRaceItStillSeesTheHolder(t *testing.T) {
	client := fake.NewClientset()
	bCancel, bDone := startLeading(t, testConfig(client, "replica-b"), func(lctx context.Context) { <-lctx.Done() })
	defer func() {
		bCancel()
		<-bDone
	}()

	obs := NewObserver()
	cfg := testConfig(client, "replica-a")
	cfg.Observer = obs
	cfg.MayLead = func() error { return errors.New("the test says no") }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var acted atomic.Bool
	go func() {
		defer close(done)
		_ = Run(ctx, cfg, func(context.Context) { acted.Store(true) })
	}()
	defer func() {
		cancel()
		<-done
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !obs.PeerHolds() {
		if time.Now().After(deadline) {
			t.Fatal("a replica out of the race never saw the holder — it would report over the holder's actions")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if acted.Load() {
		t.Error("acted while MayLead said no")
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
