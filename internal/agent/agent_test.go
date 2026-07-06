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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/executor"
	"github.com/kernpilot/kubehz-agent/internal/inventory"
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
	client := fake.NewClientset(testNode("cp-1"))

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

// TestAgent_InventoryRidesTheBeat wires the full inventory ROUND TRIP: a
// lok8s-written ClusterInventory CR exists in the (fake) cluster, its spec
// must appear verbatim as the heartbeat's `inventory` block, and the
// availableUpdates the server returns in the heartbeat response must land on
// the CR's status subresource (with lastReported) — kubectl-visible updates,
// end to end.
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
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/clusters/kubehz.in.net/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var p state.Payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("payload decode: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, p)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// The api computes availableUpdates from the reported inventory and
		// returns them in the heartbeat response (parallel api change).
		_, _ = fmt.Fprint(w, `{"status":"ok","availableUpdates":[{"name":"cilium","current":"1.16.1","latest":"1.17.4"}]}`)
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

	// The response's availableUpdates must land on the CR's status.
	deadline := time.Now().Add(5 * time.Second)
	for {
		cr, err := dyn.Resource(inventory.GVR).Get(ctx, "cluster", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get ClusterInventory: %v", err)
		}
		got, _, _ := unstructured.NestedSlice(cr.Object, "status", "availableUpdates")
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
