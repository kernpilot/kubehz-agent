package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/executor"
	"github.com/kernpilot/kubehz-agent/internal/inventory"
	"github.com/kernpilot/kubehz-agent/internal/leader"
	"github.com/kernpilot/kubehz-agent/internal/leader/leasetest"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

const testToken = "khz_agt_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.35.5"},
		},
	}
}

// TestAgent_RunPublishesAndShutsDown wires the REAL informer→coalescer→sender
// pipeline against a fake clientset and asserts the three lifecycle facts unit
// tests cannot: (1) an authenticated snapshot reaches the endpoint after cache
// sync, (2) a cluster change propagates into a later snapshot, (3) cancelling
// the context stops Run promptly and cleanly.
func TestAgent_RunPublishesAndShutsDown(t *testing.T) {
	// A Ready kube-scheduler static pod: the control-plane manager must turn
	// it into components[] on the beat (the CronJob's read, ported).
	scheduler := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-scheduler-cp-1", Namespace: "kube-system", Labels: map[string]string{"component": "kube-scheduler"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	client := fake.NewClientset(testNode("cp-1"), scheduler)

	var mu sync.Mutex
	var payloads []state.Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("bad Authorization header: %q", got)
		}
		var p state.Payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("payload decode: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := &config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 150 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, client, nil, nil).Run(ctx) }()

	// (1) First snapshot arrives with the pre-existing node.
	waitForPayload(t, &mu, &payloads, func(p state.Payload) bool {
		return p.Schema == state.SchemaVersion && len(p.Nodes) == 1 && p.Nodes[0].Name == "cp-1"
	})

	// (2) A new node lands in a later snapshot (change signal or periodic).
	if _, err := client.CoreV1().Nodes().Create(ctx, testNode("w-1"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	waitForPayload(t, &mu, &payloads, func(p state.Payload) bool { return len(p.Nodes) == 2 })

	// (2b) components[] rides the beat: scheduler Healthy from the static pod.
	// The fake clientset has no REST client, so /readyz reads as unreachable
	// and apiserver/etcd are omitted — never reported Unhealthy.
	waitForPayload(t, &mu, &payloads, func(p state.Payload) bool {
		for _, c := range p.Components {
			if c.Name == "scheduler" && c.Status == "Healthy" {
				return true
			}
		}
		return false
	})
	mu.Lock()
	for _, p := range payloads {
		for _, c := range p.Components {
			if c.Name == "apiserver" || c.Name == "etcd" {
				t.Errorf("beat reported %s=%s although /readyz was unreachable", c.Name, c.Status)
			}
		}
	}
	mu.Unlock()

	// (3) Cancel stops Run promptly with the context error (clean shutdown).
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// TestAgent_DesiredLoopScalesAndReportsActions wires the FULL P3 path: the
// poller pulls a desired doc (scaling authorized, pool-a 2→3), the executor
// patches the fake MachineDeployment, and the resulting done action rides a
// subsequent heartbeat — outbound-only end to end (one httptest server plays
// kubehz-api for both the heartbeat POST and the desired GET).
func TestAgent_DesiredLoopScalesAndReportsActions(t *testing.T) {
	client := fake.NewClientset(testNode("cp-1"))

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			executor.MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:           "MachineList",
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "cluster.k8s.io/v1alpha1",
			"kind":       "MachineDeployment",
			"metadata":   map[string]any{"name": "pool-a", "namespace": "kube-system"},
			"spec":       map[string]any{"replicas": int64(2)},
		}})

	var mu sync.Mutex
	var payloads []state.Payload
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var p state.Payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("payload decode: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /api/clusters/kubehz.in.net/desired", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("desired GET bad Authorization: %q", got)
		}
		const etag = `"7-10"`
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = fmt.Fprint(w, `{"revision":7,"kubernetesVersion":null,"workerPools":[{"name":"pool-a","machineType":"cpx31","desiredReplicas":3}],"execution":{"scaling":true,"upgrades":false}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 150 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
		DesiredPoll:  50 * time.Millisecond,
		MDNamespace:  "kube-system",
		MaxReplicas:  50,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, client, dyn, nil).Run(ctx) }()

	// The done action for revision 7 must reach a heartbeat.
	waitForPayload(t, &mu, &payloads, func(p state.Payload) bool {
		for _, a := range p.Actions {
			if a.Type == state.ActionScale && a.Target == "pool-a" &&
				a.Status == state.ActionDone && a.Revision == 7 {
				return true
			}
		}
		return false
	})

	// And the fake MD must actually be at 3 replicas.
	got, err := dyn.Resource(executor.MachineDeploymentGVR).Namespace("kube-system").
		Get(ctx, "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get MD: %v", err)
	}
	replicas, _, _ := unstructured.NestedFieldNoCopy(got.Object, "spec", "replicas")
	switch n := replicas.(type) {
	case int64:
		if n != 3 {
			t.Errorf("replicas = %d, want 3", n)
		}
	case float64:
		if n != 3 {
			t.Errorf("replicas = %v, want 3", n)
		}
	default:
		t.Errorf("unreadable replicas: %T %v", replicas, replicas)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// A STANDBY sends no beats while another replica holds the acting lease. The
// platform keeps one action snapshot per cluster, latest-wins, and reads a
// beat without actions[] as "clear", so a standby's beats would erase the
// holder's outcomes and refusals. Once the peer's lease runs out (it stops
// renewing), the standby reports again.
func TestAgent_StandbySendsNoBeatsWhileAPeerHoldsTheLease(t *testing.T) {
	now := metav1.NewMicroTime(time.Now())
	peer, two := "peer-replica_0000", int32(2)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leader.LeaseName, Namespace: config.DefaultNamespace},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &peer, LeaseDurationSeconds: &two,
			AcquireTime: &now, RenewTime: &now,
		},
	}
	client := fake.NewClientset(testNode("cp-1"), lease)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			executor.MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:           "MachineList",
		})

	var mu sync.Mutex
	var beats []time.Time
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		beats = append(beats, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /api/clusters/kubehz.in.net/desired", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"revision":0,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 100 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
		DesiredPoll:  50 * time.Millisecond,
		MDNamespace:  "kube-system",
		MaxReplicas:  50,
	}

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ag := New(cfg, client, dyn, nil)
	ag.lease = testLease
	go func() { done <- ag.Run(ctx) }()

	// The peer's lease is valid for two seconds from the standby's first read.
	time.Sleep(time.Second)
	mu.Lock()
	early := len(beats)
	mu.Unlock()
	if early != 0 {
		t.Errorf("%d beats sent in the first second while another replica held the acting lease, want 0", early)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(beats)
		var first time.Time
		if n > 0 {
			first = beats[0]
		}
		mu.Unlock()
		if n > 0 {
			if first.Sub(start) < 1500*time.Millisecond {
				t.Errorf("first beat %s after start, inside the peer's lease", first.Sub(start))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the standby never reported again after the peer's lease ran out")
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// testLease is the acting Lease sped up: seconds instead of the production
// half-minute.
var testLease = leaseTimings{duration: 2 * time.Second, renewDeadline: time.Second, retryPeriod: 100 * time.Millisecond}

// actingAgent builds an agent WITH an acting loop (a dynamic client) against
// a platform that serves a no-op desired document; beat answers the
// heartbeats. The pod name is agent-pod, so the lease identity starts with
// "agent-pod_".
func actingAgent(t *testing.T, beat http.HandlerFunc) (*Agent, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset(testNode("cp-1"))
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			executor.MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:           "MachineList",
		})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", beat)
	mux.HandleFunc("GET /api/clusters/kubehz.in.net/desired", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"revision":0,"kubernetesVersion":null,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	ag := New(&config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 100 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
		DesiredPoll:  50 * time.Millisecond,
		MDNamespace:  "kube-system",
		MaxReplicas:  50,
		PodName:      "agent-pod",
	}, client, dyn, nil)
	ag.lease = testLease
	return ag, client
}

// storedHolder reads the acting Lease's holder ("" = none or no Lease).
func storedHolder(client *fake.Clientset) string {
	l, err := client.CoordinationV1().Leases(config.DefaultNamespace).Get(context.Background(), leader.LeaseName, metav1.GetOptions{})
	if err != nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func waitUntil(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func holdsIt(client *fake.Clientset) bool {
	return strings.HasPrefix(storedHolder(client), "agent-pod_")
}

// SHUTDOWN waits for the acting loop: Run returns only after the drain and
// the lease release are done, because main exits as soon as Run returns. A
// release cut off by the exit leaves the successor waiting the lease out.
func TestAgent_ShutdownWaitsForTheLeaseRelease(t *testing.T) {
	ag, client := actingAgent(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	var mu sync.Mutex
	var released time.Time
	client.PrependReactor("update", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		u, ok := a.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		if l, ok := u.GetObject().(*coordinationv1.Lease); ok && l.Spec.HolderIdentity != nil && *l.Spec.HolderIdentity == "" {
			time.Sleep(500 * time.Millisecond) // a slow apiserver
			mu.Lock()
			released = time.Now()
			mu.Unlock()
		}
		return false, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan time.Time, 1)
	go func() {
		_ = ag.Run(ctx)
		done <- time.Now()
	}()
	waitUntil(t, "the agent never acquired the acting lease", 5*time.Second, func() bool { return holdsIt(client) })

	cancel()
	var returned time.Time
	select {
	case returned = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	if released.IsZero() {
		t.Fatal("Run returned before the acting lease was released — main would exit with the release still to do")
	}
	if returned.Before(released) {
		t.Errorf("Run returned %s before the release finished", released.Sub(returned))
	}
}

// STEP-DOWN: the acting replica is the cluster's one reporter (a standby is
// silent while a peer holds the lease), so a holder whose heartbeats keep
// failing would silence every replica that can report. After the step-down
// window it releases the lease and stays out of the race; once a heartbeat
// goes through it contends again.
func TestAgent_StepsDownWhileItsHeartbeatsFail(t *testing.T) {
	var failing atomic.Bool
	var ok atomic.Int32
	ag, client := actingAgent(t, func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ok.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	ag.stepDownAfter = 300 * time.Millisecond
	ag.sendBackoff = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ag.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitUntil(t, "the agent never acquired the acting lease", 5*time.Second, func() bool { return holdsIt(client) && ok.Load() > 0 })

	failing.Store(true)
	waitUntil(t, "the holder kept the acting lease while its heartbeats failed", 5*time.Second, func() bool { return storedHolder(client) == "" })
	time.Sleep(600 * time.Millisecond)
	if holdsIt(client) {
		t.Fatal("the replica took the lease back while its heartbeats still failed")
	}

	failing.Store(false)
	waitUntil(t, "the replica never contended again after a heartbeat went through", 5*time.Second, func() bool { return holdsIt(client) })
}

// A FORMER HOLDER drops its queued beat once it sees the new holder. The beat
// failed while this replica still held the lease; its retry carries no
// actions and, sent after a peer took over, would erase the peer's.
func TestAgent_FormerHolderDropsItsQueuedBeat(t *testing.T) {
	var failing atomic.Bool
	var failed atomic.Int32
	ag, client := actingAgent(t, func(w http.ResponseWriter, _ *http.Request) {
		if failing.Load() {
			failed.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	leasetest.EnforceResourceVersion(client)
	ag.sendBackoff = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ag.Run(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	waitUntil(t, "the agent never acquired the acting lease", 5*time.Second, func() bool { return holdsIt(client) })

	failing.Store(true)
	waitUntil(t, "no heartbeat failed", 5*time.Second, func() bool { return failed.Load() > 0 })

	// A peer takes the Lease, with the apiserver's compare-and-swap.
	leases := client.CoordinationV1().Leases(config.DefaultNamespace)
	waitUntil(t, "the test could not hand the Lease to the peer", 5*time.Second, func() bool {
		l, err := leases.Get(context.Background(), leader.LeaseName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		peer, thirty := "peer-replica_0000", int32(30)
		now := metav1.NewMicroTime(time.Now())
		l.Spec.HolderIdentity, l.Spec.LeaseDurationSeconds = &peer, &thirty
		l.Spec.AcquireTime, l.Spec.RenewTime = &now, &now
		_, err = leases.Update(context.Background(), l, metav1.UpdateOptions{})
		return err == nil
	})

	// The former holder's next renewal fails and its read shows the peer.
	time.Sleep(1500 * time.Millisecond)
	before := failed.Load()
	time.Sleep(1500 * time.Millisecond)
	if after := failed.Load(); after != before {
		t.Errorf("%d beats sent after the peer took the acting lease, want 0: a queued retry went out", after-before)
	}
	if got := storedHolder(client); got != "peer-replica_0000" {
		t.Errorf("stored holder = %q, want the peer", got)
	}
}

func waitForPayload(t *testing.T, mu *sync.Mutex, payloads *[]state.Payload, match func(state.Payload) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, p := range *payloads {
			if match(p) {
				mu.Unlock()
				return
			}
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("expected payload never arrived")
}

func TestStripManagedFields(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:          "p1",
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
	}}
	out, err := stripManagedFields(pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out.(*corev1.Pod); len(got.ManagedFields) != 0 {
		t.Errorf("managedFields not stripped: %+v", got.ManagedFields)
	}

	// Tombstones are unwrapped, stripped, and re-wrapped.
	tomb := cache.DeletedFinalStateUnknown{Key: "ns/p2", Obj: &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}}},
	}}
	out, err = stripManagedFields(tomb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	inner := out.(cache.DeletedFinalStateUnknown).Obj.(*corev1.Pod)
	if len(inner.ManagedFields) != 0 {
		t.Errorf("tombstone managedFields not stripped: %+v", inner.ManagedFields)
	}

	// Non-objects pass through unchanged rather than erroring (fail-soft).
	if out, err = stripManagedFields("not-an-object"); err != nil || out != "not-an-object" {
		t.Errorf("non-object should pass through, got (%v, %v)", out, err)
	}
}

// TestAgent_InventoryRidesTheBeat wires the full inventory ROUND TRIP through
// all THREE response states (kubehz-api f628c97): a lok8s-written
// ClusterInventory CR exists in the (fake) cluster and its spec must appear
// verbatim as the heartbeat's `inventory` block; a NON-EMPTY availableUpdates
// response lands on the CR's status subresource (with lastReported); an
// ABSENT key (index unreachable) leaves that status untouched across further
// beats; and a PRESENT-but-empty [] (nothing newer) clears it — kubectl-
// visible updates, end to end.
func TestAgent_InventoryRidesTheBeat(t *testing.T) {
	client := fake.NewClientset(testNode("cp-1"))

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			inventory.GVR:                 "ClusterInventoryList",
			executor.MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:           "MachineList",
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "lok8s.dev/v1alpha1",
			"kind":       "ClusterInventory",
			"metadata":   map[string]any{"name": "cluster"},
			"spec": map[string]any{
				"lok8sVersion":      "1.4.2",
				"kind":              "kubeone",
				"provider":          "hetzner",
				"kubernetesVersion": "v1.35.5",
				"specHash":          strings.Repeat("a", 64),
				"renderedAt":        "2026-07-06T10:00:00Z",
				"addons": []any{
					map[string]any{"name": "cilium", "chartVersion": "1.16.1", "category": "networking", "source": "addon"},
				},
			},
		}})

	var mu sync.Mutex
	var payloads []state.Payload
	// respBody drives the server's availableUpdates verdict per phase:
	// non-empty → absent key → empty [].
	respBody := `{"status":"ok","availableUpdates":[{"name":"cilium","current":"1.16.1","latest":"1.17.4"}]}`
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var p state.Payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("payload decode: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, p)
		body := respBody
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// The api computes availableUpdates from the reported inventory and
		// returns them in the heartbeat response (kubehz-api f628c97).
		_, _ = fmt.Fprint(w, body)
	})
	mux.HandleFunc("GET /api/clusters/kubehz.in.net/desired", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"revision":0,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 150 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
		DesiredPoll:  time.Hour, // out of the way; this test is about inventory
		MDNamespace:  "kube-system",
		MaxReplicas:  50,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, client, dyn, nil).Run(ctx) }()

	waitForPayload(t, &mu, &payloads, func(p state.Payload) bool {
		inv := p.Inventory
		return inv != nil && inv.Lok8sVersion == "1.4.2" && inv.Kind == "kubeone" &&
			inv.Provider == "hetzner" && inv.KubernetesVersion == "v1.35.5" &&
			inv.SpecHash == strings.Repeat("a", 64) && inv.RenderedAt == "2026-07-06T10:00:00Z" &&
			len(inv.Addons) == 1 && inv.Addons[0].Name == "cilium" &&
			inv.Addons[0].ChartVersion == "1.16.1" && inv.Addons[0].Source == "addon"
	})

	// crUpdates reads the CR's current status.availableUpdates.
	crUpdates := func() []any {
		cr, err := dyn.Resource(inventory.GVR).Get(ctx, "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ClusterInventory: %v", err)
		}
		got, _, _ := unstructured.NestedSlice(cr.Object, "status", "availableUpdates")
		return got
	}
	// beatsSeen snapshots how many heartbeats the server has answered.
	beatsSeen := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads)
	}
	// waitBeats blocks until n MORE heartbeats than `from` have been answered.
	waitBeats := func(from, n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for beatsSeen() < from+n {
			if time.Now().After(deadline) {
				t.Fatalf("server never saw %d more heartbeats", n)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Phase 1 — non-empty verdict: the updates must land on the CR's status.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := crUpdates()
		cr, err := dyn.Resource(inventory.GVR).Get(ctx, "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ClusterInventory: %v", err)
		}
		lastReported, _, _ := unstructured.NestedString(cr.Object, "status", "lastReported")
		if len(got) == 1 && lastReported != "" {
			entry, _ := got[0].(map[string]any)
			if entry["name"] != "cilium" || entry["current"] != "1.16.1" || entry["latest"] != "1.17.4" {
				t.Errorf("status.availableUpdates mismapped: %v", entry)
			}
			if _, err := time.Parse(time.RFC3339, lastReported); err != nil {
				t.Errorf("status.lastReported %q is not RFC3339: %v", lastReported, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("availableUpdates never reached the CR status (got %v)", got)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Phase 2 — ABSENT key (index unreachable): further beats must NOT touch
	// the status; the last known updates survive the outage.
	mu.Lock()
	respBody = `{"status":"ok"}`
	from := len(payloads)
	mu.Unlock()
	waitBeats(from, 2)
	if got := crUpdates(); len(got) != 1 {
		t.Errorf("an absent availableUpdates key modified the CR status: %v", got)
	}

	// Phase 3 — PRESENT-but-empty [] (nothing newer): the stale entry must be
	// cleared from the CR.
	mu.Lock()
	respBody = `{"status":"ok","availableUpdates":[]}`
	mu.Unlock()
	deadline = time.Now().Add(5 * time.Second)
	for {
		if got := crUpdates(); len(got) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("empty verdict never cleared the CR status (still %v)", crUpdates())
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// TestAgent_NoInventoryCRNoBlock: without the CRD/CR (a cluster never deployed
// by lok8s) the beat must carry NO inventory key — fail-soft, zero noise.
func TestAgent_NoInventoryCRNoBlock(t *testing.T) {
	client := fake.NewClientset(testNode("cp-1"))
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			inventory.GVR:                 "ClusterInventoryList",
			executor.MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:           "MachineList",
		})

	var mu sync.Mutex
	var raws []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		mu.Lock()
		raws = append(raws, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /api/clusters/kubehz.in.net/desired", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"revision":0,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := &config.Config{
		ClusterID:    "kubehz.in.net",
		APIURL:       srv.URL,
		AgentToken:   testToken,
		FullInterval: 150 * time.Millisecond,
		Debounce:     20 * time.Millisecond,
		MinGap:       20 * time.Millisecond,
		DesiredPoll:  time.Hour,
		MDNamespace:  "kube-system",
		MaxReplicas:  50,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, client, dyn, nil).Run(ctx) }()

	// Wait for a couple of beats, then assert none carried the key.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(raws)
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never received two heartbeats")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	for _, raw := range raws {
		if strings.Contains(raw, `"inventory"`) {
			t.Errorf("beat carried an inventory key without a CR: %s", raw)
		}
	}
	mu.Unlock()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}
