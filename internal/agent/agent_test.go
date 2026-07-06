package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		map[schema.GroupVersionResource]string{executor.MachineDeploymentGVR: "MachineDeploymentList"},
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
