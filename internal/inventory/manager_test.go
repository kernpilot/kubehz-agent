package inventory

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func fakeDyn(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{GVR: "ClusterInventoryList"},
		objects...)
}

// TestManager_SnapshotPresent: with the CR in place, one fetch yields the
// mapped inventory and signals the coalescer exactly once; an unchanged
// re-fetch neither re-notifies nor changes the snapshot.
func TestManager_SnapshotPresent(t *testing.T) {
	notified := 0
	m := NewManager(fakeDyn(inventoryCR()), 0, func() { notified++ }, nil)

	m.fetchOnce(context.Background())
	inv := m.Snapshot()
	if inv == nil {
		t.Fatal("Snapshot() = nil, want inventory")
	}
	if inv.Kind != "kubeone" || len(inv.Addons) != 3 {
		t.Errorf("snapshot mismapped: %+v", inv)
	}
	if notified != 1 {
		t.Errorf("notify count = %d after first fetch, want 1", notified)
	}

	m.fetchOnce(context.Background())
	if notified != 1 {
		t.Errorf("unchanged re-fetch re-notified (count %d)", notified)
	}
}

// TestManager_SnapshotIsACopy: mutating a returned snapshot must not leak into
// the manager's state (the coalescer runs ApplyCaps on it while the poll loop
// may replace it).
func TestManager_SnapshotIsACopy(t *testing.T) {
	m := NewManager(fakeDyn(inventoryCR()), 0, nil, nil)
	m.fetchOnce(context.Background())

	s1 := m.Snapshot()
	s1.Kind = "mutated"
	s1.Addons[0].Name = "mutated"

	s2 := m.Snapshot()
	if s2.Kind != "kubeone" || s2.Addons[0].Name != "cilium" {
		t.Errorf("snapshot mutation leaked into manager state: %+v", s2)
	}
}

// TestManager_AbsentCR: no CR (the every-non-lok8s-cluster case) → nil
// snapshot, no notify (nothing changed from the initial nil), no error spam.
func TestManager_AbsentCR(t *testing.T) {
	notified := 0
	m := NewManager(fakeDyn(), 0, func() { notified++ }, nil)

	m.fetchOnce(context.Background())
	if got := m.Snapshot(); got != nil {
		t.Errorf("Snapshot() = %+v, want nil for absent CR", got)
	}
	if notified != 0 {
		t.Errorf("absent CR notified %d times, want 0", notified)
	}
}

// TestManager_CRDisappears: present → deleted transitions the snapshot back to
// nil and notifies (the beat must drop the block).
func TestManager_CRDisappears(t *testing.T) {
	dyn := fakeDyn(inventoryCR())
	notified := 0
	m := NewManager(dyn, 0, func() { notified++ }, nil)

	m.fetchOnce(context.Background())
	if m.Snapshot() == nil || notified != 1 {
		t.Fatalf("setup: snapshot nil or notified=%d", notified)
	}

	if err := dyn.Resource(GVR).Delete(context.Background(), CRName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete CR: %v", err)
	}
	m.fetchOnce(context.Background())
	if got := m.Snapshot(); got != nil {
		t.Errorf("Snapshot() = %+v after delete, want nil", got)
	}
	if notified != 2 {
		t.Errorf("notify count = %d after disappearance, want 2", notified)
	}
}

// TestManager_ReadForbidden: an RBAC-denied GET fails soft — nil snapshot, no
// panic, the loop keeps going (base-RBAC-not-yet-applied clusters).
func TestManager_ReadForbidden(t *testing.T) {
	dyn := fakeDyn(inventoryCR())
	dyn.PrependReactor("get", "clusterinventories", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: GVR.Group, Resource: GVR.Resource}, CRName, errors.New("denied"))
	})
	m := NewManager(dyn, 0, nil, nil)

	m.fetchOnce(context.Background())
	m.fetchOnce(context.Background()) // second pass: throttled log path, no crash
	if got := m.Snapshot(); got != nil {
		t.Errorf("Snapshot() = %+v under forbidden read, want nil", got)
	}
}
