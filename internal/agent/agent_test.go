package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/config"
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
	go func() { done <- New(cfg, client, nil).Run(ctx) }()

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
