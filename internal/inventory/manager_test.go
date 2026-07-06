package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/state"
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

var t0 = time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

func updates(names ...string) []state.AvailableUpdate {
	var out []state.AvailableUpdate
	for _, n := range names {
		out = append(out, state.AvailableUpdate{Name: n, Current: "1.0.0", Latest: "2.0.0"})
	}
	return out
}

func patchActions(dyn *dynamicfake.FakeDynamicClient) []k8stesting.PatchAction {
	var out []k8stesting.PatchAction
	for _, a := range dyn.Actions() {
		if pa, ok := a.(k8stesting.PatchAction); ok {
			out = append(out, pa)
		}
	}
	return out
}

// TestManager_HandleUpdatesPatchesStatus is the write-back contract test: the
// server's availableUpdates land on the CR via the STATUS subresource as a
// merge patch carrying exactly availableUpdates + lastReported (RFC3339 now,
// injected clock), under this agent's own field manager — and the lo-owned
// spec plus the sibling status.observedAddons stay untouched.
func TestManager_HandleUpdatesPatchesStatus(t *testing.T) {
	cr := inventoryCR()
	// Seed a sibling status field a DIFFERENT observer could own: the merge
	// patch must leave it alone.
	if err := unstructured.SetNestedField(cr.Object, []any{
		map[string]any{"name": "cilium", "version": "1.16.1", "healthy": true},
	}, "status", "observedAddons"); err != nil {
		t.Fatal(err)
	}
	dyn := fakeDyn(cr)
	m := NewManager(dyn, 0, nil, nil)
	m.now = func() time.Time { return t0 }
	ctx := context.Background()
	m.fetchOnce(ctx)

	m.HandleUpdates(ctx, updates("cilium", "rook-ceph"))

	pas := patchActions(dyn)
	if len(pas) != 1 {
		t.Fatalf("patch actions = %d, want 1", len(pas))
	}
	pa := pas[0]
	if pa.GetSubresource() != "status" {
		t.Errorf("patched subresource %q, want status", pa.GetSubresource())
	}
	if pa.GetPatchType() != types.MergePatchType {
		t.Errorf("patch type %q, want merge", pa.GetPatchType())
	}
	if pa.GetName() != CRName {
		t.Errorf("patched name %q, want %q", pa.GetName(), CRName)
	}
	var body map[string]map[string]any
	if err := json.Unmarshal(pa.GetPatch(), &body); err != nil {
		t.Fatalf("patch body not JSON: %v", err)
	}
	status, ok := body["status"]
	if !ok || len(body) != 1 {
		t.Fatalf("patch must touch ONLY status, got: %s", pa.GetPatch())
	}
	if got := status["lastReported"]; got != t0.Format(time.RFC3339) {
		t.Errorf("lastReported = %v, want %s", got, t0.Format(time.RFC3339))
	}
	if len(status) != 2 {
		t.Errorf("status patch must carry exactly availableUpdates+lastReported, got: %s", pa.GetPatch())
	}

	// Resulting object: status written, spec + observedAddons untouched.
	got, err := dyn.Resource(GVR).Get(ctx, CRName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kind, _, _ := unstructured.NestedString(got.Object, "spec", "kind")
	if kind != "kubeone" {
		t.Errorf("spec mutated by the status write: kind = %q", kind)
	}
	observed, _, _ := unstructured.NestedSlice(got.Object, "status", "observedAddons")
	if len(observed) != 1 {
		t.Errorf("sibling status.observedAddons clobbered: %v", observed)
	}
	written, _, _ := unstructured.NestedSlice(got.Object, "status", "availableUpdates")
	if len(written) != 2 {
		t.Fatalf("status.availableUpdates = %v, want 2 entries", written)
	}
	first, _ := written[0].(map[string]any)
	if first["name"] != "cilium" || first["current"] != "1.0.0" || first["latest"] != "2.0.0" {
		t.Errorf("written update mismapped: %v", first)
	}
}

// TestManager_HandleUpdatesIdempotent: (a) a repeat of the just-written list
// is a no-op; (b) a list equal to what the CR ALREADY carries (agent restart)
// never writes at all; (c) a genuinely new list writes again.
func TestManager_HandleUpdatesIdempotent(t *testing.T) {
	dyn := fakeDyn(inventoryCR())
	m := NewManager(dyn, 0, nil, nil)
	ctx := context.Background()
	m.fetchOnce(ctx)

	m.HandleUpdates(ctx, updates("cilium"))
	m.HandleUpdates(ctx, updates("cilium")) // (a) same again → skip
	if n := len(patchActions(dyn)); n != 1 {
		t.Fatalf("patch actions after repeat = %d, want 1", n)
	}

	// (b) Fresh manager over the SAME cluster state (restart): the baseline
	// comes from the CR's status on first fetch, so no re-write.
	m2 := NewManager(dyn, 0, nil, nil)
	m2.fetchOnce(ctx)
	m2.HandleUpdates(ctx, updates("cilium"))
	if n := len(patchActions(dyn)); n != 1 {
		t.Fatalf("restarted agent re-wrote an unchanged status (%d patches)", n)
	}

	// (c) A different list writes again.
	m2.HandleUpdates(ctx, updates("cilium", "rook-ceph"))
	if n := len(patchActions(dyn)); n != 2 {
		t.Fatalf("changed updates did not write (%d patches)", n)
	}
}

// TestManager_HandleUpdatesNoCR: updates without a CR (server answered but the
// cluster was never lok8s-deployed, or the CR vanished) → no write.
func TestManager_HandleUpdatesNoCR(t *testing.T) {
	dyn := fakeDyn()
	m := NewManager(dyn, 0, nil, nil)
	ctx := context.Background()
	m.fetchOnce(ctx)

	m.HandleUpdates(ctx, updates("cilium"))
	if n := len(patchActions(dyn)); n != 0 {
		t.Errorf("patched status with no CR present (%d patches)", n)
	}

	// Empty/absent updates never write either, even with the CR present.
	dyn2 := fakeDyn(inventoryCR())
	m2 := NewManager(dyn2, 0, nil, nil)
	m2.fetchOnce(ctx)
	m2.HandleUpdates(ctx, nil)
	m2.HandleUpdates(ctx, []state.AvailableUpdate{})
	if n := len(patchActions(dyn2)); n != 0 {
		t.Errorf("empty updates produced %d patches, want 0", n)
	}
}

// warnCounter counts Warn-level records (the warn-once assertion).
type warnCounter struct {
	mu    sync.Mutex
	warns int
}

func (h *warnCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		h.mu.Lock()
		h.warns++
		h.mu.Unlock()
	}
	return nil
}
func (h *warnCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCounter) WithGroup(string) slog.Handler      { return h }
func (h *warnCounter) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.warns
}

// TestManager_StatusForbiddenWarnsOnce: an RBAC-denied status patch warns
// exactly once and never crashes the loop — a cluster on the old (pre-
// clusterinventories) base RBAC must not warn on every beat.
func TestManager_StatusForbiddenWarnsOnce(t *testing.T) {
	dyn := fakeDyn(inventoryCR())
	dyn.PrependReactor("patch", "clusterinventories", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: GVR.Group, Resource: GVR.Resource}, CRName, errors.New("denied"))
	})
	wc := &warnCounter{}
	m := NewManager(dyn, 0, nil, slog.New(wc))
	ctx := context.Background()
	m.fetchOnce(ctx)

	m.HandleUpdates(ctx, updates("cilium"))
	m.HandleUpdates(ctx, updates("cilium", "rook-ceph")) // still forbidden — must stay quiet
	if got := wc.count(); got != 1 {
		t.Errorf("forbidden status patch warned %d times, want exactly 1", got)
	}
	// The read side must be unaffected: the beat keeps its inventory block.
	if m.Snapshot() == nil {
		t.Error("read snapshot lost after a forbidden status write")
	}
}

// TestCapUpdates: server-supplied updates are clamped to the CRD's status
// bounds — nameless entries dropped, strings clamped, list capped at 256 —
// so the write can never be schema-rejected or bloat the user's CR.
func TestCapUpdates(t *testing.T) {
	var in []state.AvailableUpdate
	in = append(in, state.AvailableUpdate{Current: "no-name"}) // dropped
	for i := 0; i < state.MaxAvailableUpdates+10; i++ {
		in = append(in, state.AvailableUpdate{
			Name:    strings.Repeat("n", state.MaxNameLen+5),
			Current: strings.Repeat("c", state.MaxVersionLen+5),
			Latest:  strings.Repeat("l", state.MaxVersionLen+5),
		})
	}
	out := capUpdates(in)
	if len(out) != state.MaxAvailableUpdates {
		t.Fatalf("capUpdates kept %d, want %d", len(out), state.MaxAvailableUpdates)
	}
	if len(out[0].Name) != state.MaxNameLen ||
		len(out[0].Current) != state.MaxVersionLen ||
		len(out[0].Latest) != state.MaxVersionLen {
		t.Errorf("entry not clamped: name=%d current=%d latest=%d",
			len(out[0].Name), len(out[0].Current), len(out[0].Latest))
	}
	if got := capUpdates(nil); got != nil {
		t.Errorf("capUpdates(nil) = %v, want nil", got)
	}
}
