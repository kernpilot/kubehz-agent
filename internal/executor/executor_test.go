package executor

import (
	"context"
	"errors"
	"log/slog"
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
	"github.com/kernpilot/kubehz-agent/internal/machines"
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
		map[schema.GroupVersionResource]string{
			MachineDeploymentGVR: "MachineDeploymentList",
			machines.MachineGVR:  "MachineList",
			podsGVR:              "PodList", // eviction-unwedge force-deletes
		},
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
	return New(dyn, store, Options{Namespace: ns, MaxReplicas: 50}), store
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

// typedMD is an MD whose template declares a Hetzner server type, the way
// machine-controller stores it (raw provider JSON under providerSpec.value).
func typedMD(name string, replicas int64, serverType string) *unstructured.Unstructured {
	u := md(name, replicas)
	if err := unstructured.SetNestedMap(u.Object, map[string]any{
		"cloudProvider":     "hetzner",
		"cloudProviderSpec": map[string]any{"serverType": serverType},
	}, "spec", "template", "spec", "providerSpec", "value"); err != nil {
		panic(err)
	}
	return u
}

// The document's machineType is DECODED and REPORTED, never applied: changing
// it would replace every server in the pool, and the same template carries the
// image, network and SSH keys. It used to be read and dropped in silence.
func TestReconcile_MachineTypeDivergenceIsReported(t *testing.T) {
	dyn := fakeDyn(typedMD("pool-a", 2, "cpx31"))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(7, desired.WorkerPool{
		Name: "pool-a", MachineType: "cpx41", DesiredReplicas: 3,
	}))

	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionDone {
		t.Fatalf("action = %+v, want the scale to succeed", a)
	}
	if !strings.Contains(a.Detail, "machineType cpx41 not applied") || !strings.Contains(a.Detail, "cpx31") {
		t.Errorf("detail = %q, want it to name the requested and the running machine type", a.Detail)
	}
	// Replicas moved; the template did not.
	if got := replicasOf(t, dyn, "pool-a"); got != 3 {
		t.Errorf("replicas = %d, want 3", got)
	}
	current, err := dyn.Resource(MachineDeploymentGVR).Namespace(ns).Get(context.Background(), "pool-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := machines.MDMachineType(current); got != "cpx31" {
		t.Errorf("machine type = %q, want the untouched cpx31", got)
	}
}

// No note when the types agree, and none when the template's type cannot be
// read — unknown is never reported as a mismatch.
func TestReconcile_MachineTypeQuietWhenMatchedOrUnknown(t *testing.T) {
	for name, tc := range map[string]struct {
		md     *unstructured.Unstructured
		wanted string
	}{
		"same type":    {md: typedMD("pool-a", 2, "cpx31"), wanted: "cpx31"},
		"unknown type": {md: md("pool-a", 2), wanted: "cpx41"},
	} {
		t.Run(name, func(t *testing.T) {
			dyn := fakeDyn(tc.md)
			exec, store := newExecutor(dyn)

			exec.Reconcile(context.Background(), scalingDoc(7, desired.WorkerPool{
				Name: "pool-a", MachineType: tc.wanted, DesiredReplicas: 3,
			}))

			if a := findAction(t, store, "pool-a"); strings.Contains(a.Detail, "machineType") {
				t.Errorf("detail = %q, want no machineType note", a.Detail)
			}
		})
	}
}

// POOL CREATE is out of scope: the refusal must say so, so the platform stops
// waiting for a MachineDeployment the agent will never make.
func TestReconcile_MissingPoolSaysTheAgentNeverCreatesPools(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	exec.Reconcile(context.Background(), scalingDoc(1,
		desired.WorkerPool{Name: "ghost", DesiredReplicas: 3}))

	a := findAction(t, store, "ghost")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "never creates pools") {
		t.Errorf("action = %+v, want a refusal that names the permanent behaviour", a)
	}
	for _, act := range dyn.Actions() {
		if act.GetVerb() == "create" {
			t.Fatalf("the agent created a MachineDeployment: %+v", act)
		}
	}
}

// POOL DELETE is out of scope too: a MachineDeployment the document does not
// declare is never touched, never reported as a failure, and named once in the
// log so an operator can remove it deliberately.
func TestReconcile_UndeclaredPoolIsLeftAloneAndLogged(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2), md("pool-legacy", 4))
	store := actions.New(nil)
	var logs strings.Builder
	exec := New(dyn, store, Options{
		Namespace:   ns,
		MaxReplicas: 50,
		Logger:      slog.New(slog.NewTextHandler(&logs, nil)),
	})

	exec.Reconcile(context.Background(), scalingDoc(3,
		desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2}))

	for _, act := range dyn.Actions() {
		if act.GetVerb() == "delete" {
			t.Fatalf("the agent deleted a MachineDeployment: %+v", act)
		}
	}
	if got := replicasOf(t, dyn, "pool-legacy"); got != 4 {
		t.Errorf("undeclared pool replicas = %d, want the untouched 4", got)
	}
	for _, a := range store.Snapshot() {
		if a.Target == "pool-legacy" {
			t.Errorf("undeclared pool reported as an action: %+v", a)
		}
	}
	if !strings.Contains(logs.String(), "pool-legacy") || !strings.Contains(logs.String(), "never deletes a pool") {
		t.Errorf("log = %q, want the undeclared pool named once", logs.String())
	}
}

// A document the platform stopped serving must not act — and the refusal has
// to reach the heartbeat, one report per declared pool for every armed loop.
func TestRefuseStale_ReportsEveryArmedLoop(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)
	version := "v1.35.6"

	exec.RefuseStale(context.Background(), &desired.Doc{
		Revision:          9,
		KubernetesVersion: &version,
		WorkerPools:       []desired.WorkerPool{{Name: "pool-a", DesiredReplicas: 3}},
		Execution:         desired.Execution{Scaling: true, Upgrades: true, Healing: true},
		Healing:           desired.Healing{Enabled: true, MaxUnhealthy: 2},
	}, "refusing to act: the platform has not served this document again")

	byType := map[string]state.Action{}
	for _, a := range store.Snapshot() {
		byType[a.Type] = a
	}
	for _, typ := range []string{state.ActionScale, state.ActionUpgrade, state.ActionHeal} {
		a, ok := byType[typ]
		if !ok {
			t.Fatalf("no %s refusal reported: %+v", typ, store.Snapshot())
		}
		if a.Status != state.ActionFailed || a.Target != "pool-a" || a.Revision != 9 {
			t.Errorf("%s action = %+v, want a failed pool-a report at revision 9", typ, a)
		}
		if !strings.Contains(a.Detail, "has not served this document") {
			t.Errorf("%s detail = %q, want the poller's reason verbatim", typ, a.Detail)
		}
	}
	if len(dyn.Actions()) != 0 {
		t.Errorf("a refusal must touch the cluster not at all: %+v", dyn.Actions())
	}
}

// Nothing armed, nothing reported: a report-only document has no acting to
// refuse.
func TestRefuseStale_SilentWhenNothingIsArmed(t *testing.T) {
	dyn := fakeDyn(md("pool-a", 2))
	exec, store := newExecutor(dyn)

	exec.RefuseStale(context.Background(), &desired.Doc{
		Revision:    9,
		WorkerPools: []desired.WorkerPool{{Name: "pool-a", DesiredReplicas: 3}},
	}, "refusing to act")

	if got := store.Snapshot(); got != nil {
		t.Errorf("report-only document produced refusals: %+v", got)
	}
}
