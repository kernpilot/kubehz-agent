package machineissues

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

const ns = "kube-system"

var t0 = time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

func fakeDyn(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			machines.MachineGVR:           "MachineList",
			machines.MachineDeploymentGVR: "MachineDeploymentList",
		}, objects...)
}

func md(name string, matchLabels map[string]string) *unstructured.Unstructured {
	ml := map[string]any{}
	for k, v := range matchLabels {
		ml[k] = v
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "MachineDeployment",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"selector": map[string]any{"matchLabels": ml}},
	}}
}

func machine(name string, labels map[string]string, created time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "Machine",
		"metadata":   map[string]any{"name": name, "namespace": ns},
	}}
	u.SetLabels(labels)
	u.SetCreationTimestamp(metav1.NewTime(created))
	return u
}

func withNodeRef(m *unstructured.Unstructured, node string) *unstructured.Unstructured {
	status, _, _ := unstructured.NestedMap(m.Object, "status")
	if status == nil {
		status = map[string]any{}
	}
	status["nodeRef"] = map[string]any{"kind": "Node", "name": node}
	m.Object["status"] = status
	return m
}

func withTerminalError(m *unstructured.Unstructured, reason, message string) *unstructured.Unstructured {
	status, _, _ := unstructured.NestedMap(m.Object, "status")
	if status == nil {
		status = map[string]any{}
	}
	status["errorReason"] = reason
	status["errorMessage"] = message
	m.Object["status"] = status
	return m
}

func machineEvent(machineName, reason, message string, at time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: machineName + ".ev", Namespace: ns},
		InvolvedObject: corev1.ObjectReference{
			Kind: "Machine", Namespace: ns, Name: machineName,
		},
		Type:          corev1.EventTypeWarning,
		Reason:        reason,
		Message:       message,
		LastTimestamp: metav1.NewTime(at),
	}
}

func newCollector(dyn *dynamicfake.FakeDynamicClient, events EventSource, store *Store) *Collector {
	c := New(dyn, ns, events, store, time.Minute, nil)
	c.now = func() time.Time { return t0 }
	return c
}

// Terminal machine-controller errors (status.errorReason/errorMessage) map to
// one issue with the pool resolved via the MD selector.
func TestCollect_TerminalError(t *testing.T) {
	m := withTerminalError(
		machine("pool-a-abc-x1", map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)),
		"InvalidConfiguration", "machine spec is invalid")
	m = withNodeRef(m, "worker-1") // joined — only the terminal error should fire
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}), m)
	store := NewStore(nil)

	newCollector(dyn, nil, store).collectOnce(context.Background())

	got := store.Snapshot()
	if len(got) != 1 {
		t.Fatalf("issues = %+v, want exactly 1", got)
	}
	want := state.MachineIssue{
		Pool: "pool-a", Machine: "pool-a-abc-x1",
		Reason: "InvalidConfiguration", Message: "machine spec is invalid",
		Since: t0.Format(time.RFC3339),
	}
	if got[0] != want {
		t.Fatalf("issue = %+v, want %+v", got[0], want)
	}
}

// The dogfooded retry loop: the webhook accepted the spec but hcloud rejects
// every create — machine-controller reports it ONLY as Warning events on the
// Machine (reason ReconcilingError), never as a terminal status error.
func TestCollect_RetryLoopEvent(t *testing.T) {
	m := machine("pool-a-abc-x1", map[string]string{"workerset": "pool-a"}, t0.Add(-5*time.Minute))
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}), m)
	store := NewStore(nil)
	events := func() ([]*corev1.Event, error) {
		return []*corev1.Event{
			machineEvent("pool-a-abc-x1", "ReconcilingError",
				"failed to create server: unsupported location for server type", t0.Add(-time.Minute)),
			// An event for a machine that no longer exists is stale noise.
			machineEvent("gone-machine", "ReconcilingError", "whatever", t0),
		}, nil
	}

	newCollector(dyn, events, store).collectOnce(context.Background())

	got := store.Snapshot()
	if len(got) != 1 {
		t.Fatalf("issues = %+v, want exactly 1", got)
	}
	if got[0].Pool != "pool-a" || got[0].Machine != "pool-a-abc-x1" ||
		got[0].Reason != "ReconcilingError" ||
		got[0].Message != "failed to create server: unsupported location for server type" {
		t.Fatalf("issue = %+v", got[0])
	}
}

// A machine whose node never appeared within the window is an issue; a younger
// machine is not (still booting).
func TestCollect_NodeJoinTimeout(t *testing.T) {
	old := machine("pool-a-old", map[string]string{"workerset": "pool-a"}, t0.Add(-11*time.Minute))
	young := machine("pool-a-young", map[string]string{"workerset": "pool-a"}, t0.Add(-2*time.Minute))
	joined := withNodeRef(machine("pool-a-joined", map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)), "worker-9")
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}), old, young, joined)
	store := NewStore(nil)

	newCollector(dyn, nil, store).collectOnce(context.Background())

	got := store.Snapshot()
	if len(got) != 1 {
		t.Fatalf("issues = %+v, want exactly 1 (only the old joinless machine)", got)
	}
	if got[0].Machine != "pool-a-old" || got[0].Reason != "NodeJoinTimeout" {
		t.Fatalf("issue = %+v", got[0])
	}
}

// A machine being deleted is on its way out — its errors must not linger.
func TestCollect_DeletingMachineSkipped(t *testing.T) {
	m := withTerminalError(
		machine("pool-a-dying", map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)),
		"CreateError", "boom")
	del := metav1.NewTime(t0)
	m.SetDeletionTimestamp(&del)
	m.SetFinalizers([]string{"machine.cluster.k8s.io"}) // keep the fake from hard-deleting
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}), m)
	store := NewStore(nil)

	newCollector(dyn, nil, store).collectOnce(context.Background())

	if got := store.Snapshot(); got != nil {
		t.Fatalf("issues = %+v, want none for a deleting machine", got)
	}
}

// `since` is the FIRST-seen time and stays stable across passes; a recovered
// issue is pruned (and its first-seen forgotten, so a recurrence re-stamps).
func TestCollect_FirstSeenStableAndPruned(t *testing.T) {
	m := withNodeRef(withTerminalError(
		machine("pool-a-x", map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)),
		"CreateError", "boom"), "worker-1")
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}), m)
	store := NewStore(nil)
	c := newCollector(dyn, nil, store)

	c.collectOnce(context.Background())
	first := store.Snapshot()[0].Since

	c.now = func() time.Time { return t0.Add(5 * time.Minute) }
	c.collectOnce(context.Background())
	if got := store.Snapshot()[0].Since; got != first {
		t.Fatalf("since flapped: %q → %q", first, got)
	}

	// Recovery: clear the terminal error → the issue disappears and its
	// first-seen entry is pruned.
	unstructured.RemoveNestedField(m.Object, "status", "errorReason")
	unstructured.RemoveNestedField(m.Object, "status", "errorMessage")
	if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Update(
		context.Background(), m, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update machine: %v", err)
	}
	c.collectOnce(context.Background())
	if got := store.Snapshot(); got != nil {
		t.Fatalf("issues = %+v, want none after recovery", got)
	}
	if len(c.firstSeen) != 0 {
		t.Fatalf("firstSeen not pruned: %+v", c.firstSeen)
	}
}

// The snapshot is capped at the server bound (20), keeping the OLDEST issues
// deterministically.
func TestCollect_CapTwenty(t *testing.T) {
	objs := []runtime.Object{md("pool-a", map[string]string{"workerset": "pool-a"})}
	for i := 0; i < 30; i++ {
		objs = append(objs, withTerminalError(
			machine(fmt.Sprintf("pool-a-m%02d", i), map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)),
			"CreateError", "boom"))
	}
	dyn := fakeDyn(objs...)
	store := NewStore(nil)

	newCollector(dyn, nil, store).collectOnce(context.Background())

	got := store.Snapshot()
	if len(got) != state.MaxMachineIssues {
		t.Fatalf("len(issues) = %d, want %d", len(got), state.MaxMachineIssues)
	}
	if got[0].Machine != "pool-a-m00" {
		t.Fatalf("cap order not deterministic: first = %+v", got[0])
	}
}

// An unresolvable machine still surfaces, under the "unknown" pool (the server
// requires a non-empty pool).
func TestCollect_UnknownPool(t *testing.T) {
	m := withNodeRef(withTerminalError(machine("stray", nil, t0.Add(-time.Hour)), "CreateError", "boom"), "worker-1")
	dyn := fakeDyn(m) // no MDs at all
	store := NewStore(nil)

	newCollector(dyn, nil, store).collectOnce(context.Background())

	got := store.Snapshot()
	if len(got) != 1 || got[0].Pool != "unknown" {
		t.Fatalf("issues = %+v, want one under pool 'unknown'", got)
	}
}

// Fail-soft: no Machines API (RBAC/CRD absent) → empty snapshot, no panic, and
// a previously reported set is cleared rather than served stale.
func TestCollect_FailSoftNoMachinesAPI(t *testing.T) {
	dyn := fakeDyn(md("pool-a", map[string]string{"workerset": "pool-a"}),
		withTerminalError(machine("pool-a-x", map[string]string{"workerset": "pool-a"}, t0.Add(-time.Hour)), "CreateError", "boom"))
	store := NewStore(nil)
	c := newCollector(dyn, nil, store)

	c.collectOnce(context.Background())
	if store.Snapshot() == nil {
		t.Fatal("precondition: expected an issue before the API goes away")
	}

	dyn.PrependReactor("list", "machines", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New(`machines.cluster.k8s.io is forbidden`)
	})
	c.collectOnce(context.Background())
	if got := store.Snapshot(); got != nil {
		t.Fatalf("issues = %+v, want cleared when the Machines API is unavailable", got)
	}
}

// The store notifies only on actual change (no beat churn in steady state).
func TestStore_NotifyOnChangeOnly(t *testing.T) {
	notified := 0
	store := NewStore(func() { notified++ })

	issue := state.MachineIssue{Pool: "pool-a", Reason: "CreateError", Message: "boom", Since: t0.Format(time.RFC3339)}
	store.Set([]state.MachineIssue{issue})
	if notified != 1 {
		t.Fatalf("notified = %d after first set, want 1", notified)
	}
	store.Set([]state.MachineIssue{issue}) // identical → no notify
	if notified != 1 {
		t.Fatalf("notified = %d after identical set, want 1", notified)
	}
	store.Set(nil)
	if notified != 2 {
		t.Fatalf("notified = %d after clear, want 2", notified)
	}
	store.Set(nil) // none → none: no notify
	if notified != 2 {
		t.Fatalf("notified = %d after empty re-clear, want 2", notified)
	}
}
