package executor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// versionedMD builds a selector-owning MD with replicas and a kubelet version
// (the exact machine-controller field path).
func versionedMD(name string, replicas int64, kubelet string) *unstructured.Unstructured {
	u := selectorMD(name)
	if err := unstructured.SetNestedField(u.Object, replicas, "spec", "replicas"); err != nil {
		panic(err)
	}
	if kubelet != "" {
		if err := unstructured.SetNestedField(u.Object, kubelet,
			"spec", "template", "spec", "versions", "kubelet"); err != nil {
			panic(err)
		}
	}
	return u
}

// versionedMachine builds a pool machine with a kubelet version and an
// optionally joined node.
func versionedMachine(name, pool, kubelet, node string) *unstructured.Unstructured {
	u := workerMachine(name, pool, node, t0.Add(-time.Hour))
	if err := unstructured.SetNestedField(u.Object, kubelet, "spec", "versions", "kubelet"); err != nil {
		panic(err)
	}
	return u
}

func upgradeDoc(revision int, target string, pools ...desired.WorkerPool) *desired.Doc {
	return &desired.Doc{
		Revision:          revision,
		KubernetesVersion: &target,
		WorkerPools:       pools,
		Execution:         desired.Execution{Upgrades: true},
	}
}

func newUpgradeExecutor(dyn *dynamicfake.FakeDynamicClient, observed string) (*Executor, *actions.Store) {
	store := actions.New(nil)
	exec := New(dyn, store, Options{
		Namespace:       ns,
		MaxReplicas:     50,
		ObservedVersion: func() string { return observed },
		Now:             func() time.Time { return t0 },
	})
	return exec, store
}

func mdKubelet(t *testing.T, dyn *dynamicfake.FakeDynamicClient, name string) string {
	t.Helper()
	got, err := dyn.Resource(machines.MachineDeploymentGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return machines.MDKubeletVersion(got)
}

// PRE-FLIGHT: the observed control-plane version must be ≥ the target minor —
// otherwise every pending pool reports failed "control plane not yet at
// target" and NOTHING is patched (workers never outrun the CP).
func TestUpgrade_PreflightRefusesCPBehindTarget(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 2, "v1.34.9"))
	exec, store := newUpgradeExecutor(dyn, "v1.34.9") // CP still on 1.34

	retry := exec.Reconcile(context.Background(),
		upgradeDoc(5, "v1.35.6", desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2}))

	if got := mdKubelet(t, dyn, "pool-a"); got != "v1.34.9" {
		t.Fatalf("MD patched despite failing pre-flight: kubelet = %s", got)
	}
	a := findAction(t, store, "pool-a")
	if a.Type != state.ActionUpgrade || a.Status != state.ActionFailed ||
		!strings.Contains(a.Detail, "control plane not yet at target") {
		t.Errorf("action = %+v, want CP pre-flight refusal", a)
	}
	if !retry {
		t.Errorf("waiting on the CP upgrade must re-evaluate next poll")
	}
}

// PRE-FLIGHT: an unknown observed CP version refuses (never guesses).
func TestUpgrade_PreflightRefusesUnknownCP(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 2, "v1.34.9"))
	exec, store := newUpgradeExecutor(dyn, "")

	retry := exec.Reconcile(context.Background(),
		upgradeDoc(5, "v1.35.6", desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2}))

	if got := mdKubelet(t, dyn, "pool-a"); got != "v1.34.9" {
		t.Fatalf("MD patched with unknown CP version")
	}
	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "control plane version unknown") {
		t.Errorf("action = %+v, want unknown-CP refusal", a)
	}
	if !retry {
		t.Errorf("unknown CP is transient (version refresh) — must retry")
	}
}

// Happy path: CP at target minor → the pool MD's kubelet is patched via
// spec.template.spec.versions.kubelet and the action reports in-progress with
// the exact "vFROM → vTO (n/m)" progress detail.
func TestUpgrade_RollsPool(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 2, "v1.34.9"),
		versionedMachine("pool-a-m1", "pool-a", "v1.34.9", "w-1"),
		versionedMachine("pool-a-m2", "pool-a", "v1.34.9", "w-2"))
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")

	retry := exec.Reconcile(context.Background(),
		upgradeDoc(5, "v1.35.6", desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2}))

	if got := mdKubelet(t, dyn, "pool-a"); got != "v1.35.6" {
		t.Fatalf("MD kubelet = %s, want v1.35.6", got)
	}
	a := findAction(t, store, "pool-a")
	if a.Type != state.ActionUpgrade || a.Status != state.ActionInProgress {
		t.Errorf("action = %+v, want in-progress upgrade", a)
	}
	if a.Detail != "v1.34.9 → v1.35.6 (0/2)" {
		t.Errorf("detail = %q, want the exact vX → vY (n/m) format", a.Detail)
	}
	if !retry {
		t.Errorf("a roll in flight must re-evaluate next poll")
	}
}

// Progress counting: machines at the target WITH a joined node count toward
// n; when all are there the action flips to done "(m/m)".
func TestUpgrade_ProgressCountingToDone(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 2, "v1.34.9"),
		versionedMachine("pool-a-m1", "pool-a", "v1.34.9", "w-1"),
		versionedMachine("pool-a-m2", "pool-a", "v1.34.9", "w-2"))
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")
	doc := upgradeDoc(5, "v1.35.6", desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2})

	// Pass 1: patch lands, 0/2.
	exec.Reconcile(context.Background(), doc)

	// machine-controller replaces m1: new machine at target, joined.
	if err := dyn.Resource(machines.MachineGVR).Namespace(ns).Delete(context.Background(), "pool-a-m1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Create(context.Background(),
		versionedMachine("pool-a-m3", "pool-a", "v1.35.6", "w-3"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	retry := exec.Reconcile(context.Background(), doc)
	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionInProgress || a.Detail != "v1.34.9 → v1.35.6 (1/2)" {
		t.Errorf("mid-roll action = %+v, want in-progress (1/2)", a)
	}
	if !retry {
		t.Errorf("mid-roll must keep re-evaluating")
	}

	// Replacement completes: m2 → m4 at target, joined.
	if err := dyn.Resource(machines.MachineGVR).Namespace(ns).Delete(context.Background(), "pool-a-m2", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Create(context.Background(),
		versionedMachine("pool-a-m4", "pool-a", "v1.35.6", "w-4"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	retry = exec.Reconcile(context.Background(), doc)
	a = findAction(t, store, "pool-a")
	if a.Status != state.ActionDone || a.Detail != "v1.34.9 → v1.35.6 (2/2)" {
		t.Errorf("converged action = %+v, want done (2/2)", a)
	}
	if retry {
		t.Errorf("converged roll must stop requesting retries")
	}
}

// ONE POOL AT A TIME: with two pools behind the target, only the first
// (name order) is patched; the second queues as pending. Once the first
// converges, the second starts on a later pass.
func TestUpgrade_OnePoolAtATime(t *testing.T) {
	dyn := fakeDyn(
		versionedMD("pool-a", 1, "v1.34.9"),
		versionedMD("pool-b", 1, "v1.34.9"),
		versionedMachine("pool-a-m1", "pool-a", "v1.34.9", "w-1"),
		versionedMachine("pool-b-m1", "pool-b", "v1.34.9", "w-2"))
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")
	doc := upgradeDoc(5, "v1.35.6",
		desired.WorkerPool{Name: "pool-a", DesiredReplicas: 1},
		desired.WorkerPool{Name: "pool-b", DesiredReplicas: 1})

	exec.Reconcile(context.Background(), doc)

	if got := mdKubelet(t, dyn, "pool-a"); got != "v1.35.6" {
		t.Fatalf("pool-a not rolled first: %s", got)
	}
	if got := mdKubelet(t, dyn, "pool-b"); got != "v1.34.9" {
		t.Fatalf("pool-b rolled concurrently: %s", got)
	}
	b := findAction(t, store, "pool-b")
	if b.Status != state.ActionPending || !strings.Contains(b.Detail, "one pool rolls at a time") {
		t.Errorf("pool-b action = %+v, want queued pending", b)
	}

	// pool-a converges → pool-b may start.
	if err := dyn.Resource(machines.MachineGVR).Namespace(ns).Delete(context.Background(), "pool-a-m1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Create(context.Background(),
		versionedMachine("pool-a-m2", "pool-a", "v1.35.6", "w-3"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	exec.Reconcile(context.Background(), doc)
	if got := mdKubelet(t, dyn, "pool-b"); got != "v1.35.6" {
		t.Fatalf("pool-b not rolled after pool-a converged: %s", got)
	}
}

// A pool already mid-roll (MD at target, machines catching up — e.g. after an
// agent restart) blocks other pools from starting, and its `from` version is
// re-derived from the machines still on the old version.
func TestUpgrade_MidRollBlocksOthersAndDerivesFrom(t *testing.T) {
	dyn := fakeDyn(
		versionedMD("pool-a", 2, "v1.35.6"), // already declares the target
		versionedMD("pool-b", 1, "v1.34.9"),
		versionedMachine("pool-a-m1", "pool-a", "v1.34.9", "w-1"), // old version straggler
		versionedMachine("pool-a-m2", "pool-a", "v1.35.6", "w-2"),
		versionedMachine("pool-b-m1", "pool-b", "v1.34.9", "w-3"))
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")

	exec.Reconcile(context.Background(), upgradeDoc(5, "v1.35.6",
		desired.WorkerPool{Name: "pool-a", DesiredReplicas: 2},
		desired.WorkerPool{Name: "pool-b", DesiredReplicas: 1}))

	a := findAction(t, store, "pool-a")
	if a.Status != state.ActionInProgress || a.Detail != "v1.34.9 → v1.35.6 (1/2)" {
		t.Errorf("pool-a = %+v, want derived-from in-progress (1/2)", a)
	}
	if got := mdKubelet(t, dyn, "pool-b"); got != "v1.34.9" {
		t.Fatalf("pool-b rolled while pool-a is mid-roll")
	}
}

// Autoscaler-managed pools are refused (unsupported), CP-looking MDs refused;
// neither is patched.
func TestUpgrade_RefusesAutoscalerAndCPPools(t *testing.T) {
	asMD := versionedMD("pool-as", 1, "v1.34.9")
	asMD.SetAnnotations(map[string]string{
		"cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size": "5",
	})
	cpMD := versionedMD("edge-cp-pool", 1, "v1.34.9")
	dyn := fakeDyn(asMD, cpMD)
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")

	exec.Reconcile(context.Background(), upgradeDoc(5, "v1.35.6",
		desired.WorkerPool{Name: "pool-as", DesiredReplicas: 1},
		desired.WorkerPool{Name: "edge-cp-pool", DesiredReplicas: 1}))

	if got := mdKubelet(t, dyn, "pool-as"); got != "v1.34.9" {
		t.Fatalf("autoscaler pool patched")
	}
	if got := mdKubelet(t, dyn, "edge-cp-pool"); got != "v1.34.9" {
		t.Fatalf("CP-looking pool patched")
	}
	if a := findAction(t, store, "pool-as"); a.Status != state.ActionFailed || !strings.Contains(a.Detail, "unsupported") {
		t.Errorf("pool-as = %+v, want unsupported refusal", a)
	}
	if a := findAction(t, store, "edge-cp-pool"); a.Status != state.ActionFailed || !strings.Contains(a.Detail, "control-plane") {
		t.Errorf("edge-cp-pool = %+v, want CP refusal", a)
	}
}

// HALT ON FAILURE: a patch failure fails the pool and queues everything
// behind it; the pass retries next poll.
func TestUpgrade_HaltsOnPatchFailure(t *testing.T) {
	dyn := fakeDyn(
		versionedMD("pool-a", 1, "v1.34.9"),
		versionedMD("pool-b", 1, "v1.34.9"))
	dyn.PrependReactor("patch", "machinedeployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("admission webhook denied")
	})
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")

	retry := exec.Reconcile(context.Background(), upgradeDoc(5, "v1.35.6",
		desired.WorkerPool{Name: "pool-a", DesiredReplicas: 1},
		desired.WorkerPool{Name: "pool-b", DesiredReplicas: 1}))

	if a := findAction(t, store, "pool-a"); a.Status != state.ActionFailed || !strings.Contains(a.Detail, "patch failed") {
		t.Errorf("pool-a = %+v, want failed patch", a)
	}
	if b := findAction(t, store, "pool-b"); b.Status != state.ActionPending || !strings.Contains(b.Detail, "queued") {
		t.Errorf("pool-b = %+v, want queued behind the failure", b)
	}
	if !retry {
		t.Errorf("transient failure must retry")
	}
}

// No pending difference → no upgrade actions at all: a nil/empty desired
// version, or every pool already declaring the target with no roll we drove.
func TestUpgrade_NoActionWhenNothingToDo(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 1, "v1.35.6"),
		versionedMachine("pool-a-m1", "pool-a", "v1.35.6", "w-1"))

	// Already at target everywhere (and never driven by us).
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")
	retry := exec.Reconcile(context.Background(),
		upgradeDoc(5, "v1.35.6", desired.WorkerPool{Name: "pool-a", DesiredReplicas: 1}))
	if got := store.Snapshot(); got != nil {
		t.Errorf("steady state reported actions: %+v", got)
	}
	if retry {
		t.Errorf("steady state must not retry")
	}

	// Nil desired version → upgrades have no target.
	exec2, store2 := newUpgradeExecutor(dyn, "v1.35.6")
	doc := &desired.Doc{Revision: 5, Execution: desired.Execution{Upgrades: true},
		WorkerPools: []desired.WorkerPool{{Name: "pool-a", DesiredReplicas: 1}}}
	exec2.Reconcile(context.Background(), doc)
	if got := store2.Snapshot(); got != nil {
		t.Errorf("nil version reported actions: %+v", got)
	}
}

// A desired pool without a matching MD reports a failed upgrade action.
func TestUpgrade_UnmatchedPoolFails(t *testing.T) {
	dyn := fakeDyn(versionedMD("pool-a", 1, "v1.34.9"))
	exec, store := newUpgradeExecutor(dyn, "v1.35.6")

	exec.Reconcile(context.Background(), upgradeDoc(5, "v1.35.6",
		desired.WorkerPool{Name: "ghost", DesiredReplicas: 1}))

	a := findAction(t, store, "ghost")
	if a.Type != state.ActionUpgrade || a.Status != state.ActionFailed || !strings.Contains(a.Detail, "no MachineDeployment") {
		t.Errorf("action = %+v, want unmatched-pool failure", a)
	}
}

// parseMajorMinor grounds the pre-flight comparison.
func TestParseMajorMinor(t *testing.T) {
	cases := []struct {
		in       string
		maj, min int
		ok       bool
	}{
		{"v1.35.6", 1, 35, true},
		{"1.34.9", 1, 34, true},
		{"v1.35.5+k3s1", 1, 35, true},
		{"v1.35", 1, 35, true},
		{"", 0, 0, false},
		{"v1", 0, 0, false},
		{"vX.Y.Z", 0, 0, false},
	}
	for _, tc := range cases {
		maj, min, ok := parseMajorMinor(tc.in)
		if maj != tc.maj || min != tc.min || ok != tc.ok {
			t.Errorf("parseMajorMinor(%q) = (%d,%d,%v), want (%d,%d,%v)",
				tc.in, maj, min, ok, tc.maj, tc.min, tc.ok)
		}
	}
}
