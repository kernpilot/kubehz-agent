package leader

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
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
	} {
		t.Run(name, func(t *testing.T) {
			acted := false
			err := Run(context.Background(), cfg, func(context.Context) { acted = true })
			if err == nil {
				t.Fatal("no error for an unusable configuration")
			}
			if acted {
				t.Error("acted despite an unusable election configuration")
			}
		})
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
