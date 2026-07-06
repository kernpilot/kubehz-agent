package executor

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

const ns = "kube-system"

func md(name string, replicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "MachineDeployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": ns,
		},
		"spec": map[string]any{
			"replicas": replicas,
		},
	}}
}

func withLabels(u *unstructured.Unstructured, path []string, labels map[string]string) *unstructured.Unstructured {
	m := map[string]any{}
	for k, v := range labels {
		m[k] = v
	}
	if err := unstructured.SetNestedMap(u.Object, m, path...); err != nil {
		panic(err)
	}
	return u
}

func withAnnotations(u *unstructured.Unstructured, ann map[string]string) *unstructured.Unstructured {
	u.SetAnnotations(ann)
	return u
}

func fakeDyn(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{MachineDeploymentGVR: "MachineDeploymentList"},
		objects...)
}

func scalingDoc(revision int, pools ...desired.WorkerPool) *desired.Doc {
	return &desired.Doc{
		Revision:    revision,
		WorkerPools: pools,
		Execution:   desired.Execution{Scaling: true},
	}
}

func newExecutor(dyn *dynamicfake.FakeDynamicClient) (*Executor, *actions.Store) {
	store := actions.New(nil)
	return New(dyn, ns, 50, store, nil, nil), store
}

func replicasOf(t *testing.T, dyn *dynamicfake.FakeDynamicClient, name string) int64 {
	t.Helper()
	got, err := dyn.Resource(MachineDeploymentGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	n, ok := currentReplicas(got)
	if !ok {
		t.Fatalf("no readable spec.replicas on %s: %v", name, got.Object["spec"])
	}
	return n
}

func findAction(t *testing.T, store *actions.Store, target string) state.Action {
	t.Helper()
	for _, a := range store.Snapshot() {
		if a.Target == target {
			return a
		}
	}
	t.Fatalf("no action for target %q in %+v", target, store.Snapshot())
	return state.Action{}
}

func patchCount(dyn *dynamicfake.FakeDynamicClient) int {
	n := 0
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "patch" {
			n++
		}
	}
	return n
}

// Happy path: desired 3, actual 2 → one merge patch, replicas land at 3, the
// action reports done with the acted revision.
func TestReconcile_PatchesReplicas(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	retry := exec.Reconcile(context.Background(), scalingDoc(7, desired.WorkerPool{
		Name: "pool-a", MachineType: "cpx31", DesiredReplicas: 3,
	}))

	if retry {
		t.Errorf("happy path must not request a retry")
	}
	if got := replicasOf(t, dyn, "pool-a"); got != 3 {
		t.Errorf("replicas = %d, want 3", got)
	}
	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionDone || a.Type != state.ActionScale || a.Revision != 7 {
		t.Errorf("action = %+v, want done scale @7", a)
	}
	if patchCount(dyn) != 1 {
		t.Errorf("patches = %d, want exactly 1", patchCount(dyn))
	}
}

// Idempotent: already at desired → done WITHOUT a patch (restart/reconverge
// causes zero cluster writes).
func TestReconcile_AlreadyAtDesiredIsNoOpDone(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 3))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(7, desired.WorkerPool{Name: "pool-a", DesiredReplicas: 3}))

	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionDone || !strings.Contains(a.Detail, "already at") {
		t.Errorf("action = %+v, want no-op done", a)
	}
	if patchCount(dyn) != 0 {
		t.Errorf("idempotent pass patched: %d writes", patchCount(dyn))
	}
}

// Out-of-bounds desiredReplicas is REFUSED (failed action), never rewritten
// to the bound and applied.
func TestReconcile_RefusesOutOfBoundsReplicas(t *testing.T) {
	for _, want := range []int{51, -1} {
		dyn := fakeDyn(md("pool-a", 2))
		exec, store := newExecutor(dyn)

		retry := exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: "pool-a", DesiredReplicas: want}))

		if retry {
			t.Errorf("desired=%d: a policy refusal is final, not retried", want)
		}
		a := findAction(t, store, "pool-a")
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "KUBEHZ_MAX_REPLICAS") {
			t.Errorf("desired=%d: action = %+v, want failed bounds refusal", want, a)
		}
		if got := replicasOf(t, dyn, "pool-a"); got != 2 {
			t.Errorf("desired=%d: replicas moved to %d — bounds must never be clamped-and-applied", want, got)
		}
	}
}

// An unmatched pool is skipped and reported failed with the mapping detail.
func TestReconcile_UnmatchedPoolFails(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(1,
		desired.WorkerPool{Name: "ghost", DesiredReplicas: 3},
		desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2},
	))

	a := findAction(t, store, "ghost")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "no MachineDeployment") {
		t.Errorf("action = %+v, want failed no-MD", a)
	}
	// The refusal must not block the other pool.
	if b := findAction(t, store, "pool-a"); b.Status != state.ActionDone {
		t.Errorf("sibling pool blocked by refusal: %+v", b)
	}
}

// Control-plane MDs are never touched — by label on the MD/template or by
// name heuristic; a Hetzner machine-type-looking name (cpx31) is NOT refused.
func TestReconcile_ControlPlaneFilter(t *testing.T) {
	cases := []struct {
		name string
		obj  *unstructured.Unstructured
	}{
		{"md label", withLabels(md("byline", 1), []string{"metadata", "labels"},
			map[string]string{"node-role.kubernetes.io/control-plane": ""})},
		{"template label", withLabels(md("bytpl", 1), []string{"spec", "template", "metadata", "labels"},
			map[string]string{"node-role.kubernetes.io/master": ""})},
		{"name control-plane", md("prod-control-plane", 1)},
		{"name cp segment", md("prod-cp-1", 1)},
		{"name master", md("master-pool", 1)},
	}
	for _, tc := range cases {
		dyn := fakeDyn(tc.obj)
		exec, store := newExecutor(dyn)
		name := tc.obj.GetName()

		retry := exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: name, DesiredReplicas: 3}))

		a := findAction(t, store, name)
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "control-plane") {
			t.Errorf("%s: action = %+v, want control-plane refusal", tc.name, a)
		}
		if retry || patchCount(dyn) != 0 {
			t.Errorf("%s: control-plane MD was touched (retry=%v patches=%d)", tc.name, retry, patchCount(dyn))
		}
	}

	// cpx31-workers must NOT be mistaken for a control plane ("cp" only counts
	// as a whole delimiter-separated segment).
	dyn := fakeDyn(md("cpx31-workers", 2))
	exec, store := newExecutor(dyn)
	exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: "cpx31-workers", DesiredReplicas: 3}))
	if a := findAction(t, store, "cpx31-workers"); a.Status != state.ActionDone {
		t.Errorf("cpx31-workers falsely refused: %+v", a)
	}
}

// An autoscaler-owned MD (node-group annotations, either API-group spelling)
// is refused — the agent never fights another controller.
func TestReconcile_AutoscalerConflictRefusal(t *testing.T) {
	for _, key := range []string{
		"cluster.k8s.io/cluster-api-autoscaler-node-group-min-size",
		"cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size",
	} {
		dyn := fakeDyn(withAnnotations(md("pool-a", 2), map[string]string{key: "1"}))
		exec, store := newExecutor(dyn)

		exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: "pool-a", DesiredReplicas: 3}))

		a := findAction(t, store, "pool-a")
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "cluster-autoscaler") {
			t.Errorf("%s: action = %+v, want autoscaler refusal", key, a)
		}
		if patchCount(dyn) != 0 {
			t.Errorf("%s: autoscaler-owned MD was patched", key)
		}
	}
}

// Server gate: with both execution flags false the executor touches NOTHING —
// not even a List — and clears any previous reports (report-only posture).
func TestReconcile_ExecutionOffClearsAndDoesNothing(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	// Seed a previous revision's report, as if acting had been on.
	store.Begin(3)
	store.Upsert(state.Action{Type: state.ActionScale, Target: "pool-a", Status: state.ActionDone, Revision: 3})

	retry := exec.Reconcile(context.Background(), &desired.Doc{
		Revision:    4,
		WorkerPools: []desired.WorkerPool{{Name: "pool-a", DesiredReplicas: 30}},
		Execution:   desired.Execution{Scaling: false, Upgrades: false},
	})

	if retry {
		t.Errorf("report-only pass requested a retry")
	}
	if got := store.Snapshot(); got != nil {
		t.Errorf("reports not cleared: %+v", got)
	}
	if n := len(dyn.Actions()); n != 0 {
		t.Errorf("executor performed %d API calls while execution was off", n)
	}
}

// A transient patch failure halts the pass (one action at a time): the pool
// behind it stays pending and the pass requests a retry.
func TestReconcile_TransientFailureHaltsPass(t *testing.T) {
	dyn := fakeDyn(md("a-pool", 1), md("b-pool", 1))
	dyn.PrependReactor("patch", "machinedeployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcdserver: request timed out")
	})
	exec, store := newExecutor(dyn)

	retry := exec.Reconcile(context.Background(), scalingDoc(2,
		desired.WorkerPool{Name: "b-pool", DesiredReplicas: 2},
		desired.WorkerPool{Name: "a-pool", DesiredReplicas: 2},
	))

	if !retry {
		t.Errorf("transient failure must request a retry")
	}
	// Name order: a-pool acts first (fails), b-pool queues behind it.
	if a := findAction(t, store, "a-pool"); a.Status != state.ActionFailed {
		t.Errorf("a-pool = %+v, want failed", a)
	}
	if b := findAction(t, store, "b-pool"); b.Status != state.ActionPending || !strings.Contains(b.Detail, "queued") {
		t.Errorf("b-pool = %+v, want pending behind the failure", b)
	}
}

// A failed List fails every pool transiently (nothing to diff) and retries.
func TestReconcile_ListFailureIsTransient(t *testing.T) {
	dyn := fakeDyn()
	dyn.PrependReactor("list", "machinedeployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	exec, store := newExecutor(dyn)

	retry := exec.Reconcile(context.Background(), scalingDoc(2, desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2}))

	if !retry {
		t.Errorf("list failure must request a retry")
	}
	if a := findAction(t, store, "pool-a"); a.Status != state.ActionFailed || !strings.Contains(a.Detail, "list MachineDeployments") {
		t.Errorf("action = %+v, want transient list failure", a)
	}
}

// Authorized-but-unbuilt upgrades are reported unsupported (failed), and only
// when there is an actual pending version difference.
func TestReconcile_UpgradeReportedUnsupported(t *testing.T) {
	v := "v1.36.1"
	doc := &desired.Doc{
		Revision:          5,
		KubernetesVersion: &v,
		Execution:         desired.Execution{Upgrades: true},
	}

	dyn := fakeDyn()
	store := actions.New(nil)
	exec := New(dyn, ns, 50, store, func() string { return "v1.35.5" }, nil)
	exec.Reconcile(context.Background(), doc)

	a := findAction(t, store, v)
	if a.Type != state.ActionUpgrade || a.Status != state.ActionFailed || !strings.Contains(a.Detail, "not supported") {
		t.Errorf("action = %+v, want unsupported upgrade report", a)
	}

	// Already at target (modulo the v prefix) → nothing to report.
	store2 := actions.New(nil)
	exec2 := New(dyn, ns, 50, store2, func() string { return "1.36.1" }, nil)
	exec2.Reconcile(context.Background(), doc)
	if got := store2.Snapshot(); got != nil {
		t.Errorf("in-sync version reported: %+v", got)
	}

	// Unknown observed version → do not guess, do not report.
	store3 := actions.New(nil)
	exec3 := New(dyn, ns, 50, store3, nil, nil)
	exec3.Reconcile(context.Background(), doc)
	if got := store3.Snapshot(); got != nil {
		t.Errorf("unknown observed version reported: %+v", got)
	}
}

// A new revision's pass replaces the previous revision's reports (latest-wins,
// mirroring the server).
func TestReconcile_NewRevisionReplacesReports(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2), md("pool-b", 5))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: "pool-a", DesiredReplicas: 3}))
	exec.Reconcile(context.Background(), scalingDoc(2, desired.WorkerPool{Name: "pool-b", DesiredReplicas: 4}))

	snap := store.Snapshot()
	if len(snap) != 1 || snap[0].Target != "pool-b" || snap[0].Revision != 2 {
		t.Errorf("old revision's reports survived: %+v", snap)
	}
}

// A malformed pool name is refused before touching the API path.
func TestReconcile_InvalidPoolName(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(1, desired.WorkerPool{Name: "Not A Name!", DesiredReplicas: 2}))

	a := findAction(t, store, "Not A Name!")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "invalid pool name") {
		t.Errorf("action = %+v, want invalid-name refusal", a)
	}
	if patchCount(dyn) != 0 {
		t.Errorf("invalid name reached the API")
	}
}
