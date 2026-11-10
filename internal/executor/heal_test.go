package executor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// t0 is "now" for all healing tests; the executor baseline is set an hour
// earlier so the restart-cooldown does not interfere unless a test wants it.
var t0 = time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

// defaultPolicy mirrors the server's HEALING_POLICY_DEFAULTS.
func healingDoc(revision int, maxUnhealthy int) *desired.Doc {
	return &desired.Doc{
		Revision:  revision,
		Execution: desired.Execution{Healing: true},
		Healing: desired.Healing{
			Enabled:                   true,
			MaxUnhealthy:              maxUnhealthy,
			NodeStartupTimeoutSeconds: 600,
			UnhealthyAfterSeconds:     300,
			CooldownSeconds:           900,
		},
	}
}

// selectorMD builds an MD whose selector owns machines labeled
// workerset=<name> (the authoritative pool mapping). It carries no
// spec.replicas, so healing sizes the pool by the machines that exist; use
// selectorMDWithReplicas where the intended size matters.
func selectorMD(name string) *unstructured.Unstructured {
	u := md(name, 2)
	unstructured.RemoveNestedField(u.Object, "spec", "replicas")
	if err := unstructured.SetNestedMap(u.Object,
		map[string]any{"workerset": name}, "spec", "selector", "matchLabels"); err != nil {
		panic(err)
	}
	return u
}

// selectorMDWithReplicas is selectorMD with an intended size.
func selectorMDWithReplicas(name string, replicas int64) *unstructured.Unstructured {
	u := selectorMD(name)
	if err := unstructured.SetNestedField(u.Object, replicas, "spec", "replicas"); err != nil {
		panic(err)
	}
	return u
}

// noTransitionNode builds a not-Ready node that reports no Ready transition
// time: a zero lastTransitionTime, or (noCondition) no Ready condition at all.
func noTransitionNode(name string, noCondition bool) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}}}
	if !noCondition {
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
	}
	return n
}

// workerMachine builds a machine in pool (workerset label), with a nodeRef
// unless node == "".
func workerMachine(name, pool, node string, created time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "Machine",
		"metadata":   map[string]any{"name": name, "namespace": ns},
	}}
	u.SetLabels(map[string]string{"workerset": pool})
	u.SetCreationTimestamp(metav1.NewTime(created))
	if node != "" {
		u.Object["status"] = map[string]any{"nodeRef": map[string]any{"kind": "Node", "name": node}}
	}
	return u
}

// notReadyNode builds a node whose Ready condition is != True since `since`.
func notReadyNode(name string, status corev1.ConditionStatus, since time.Time, cpRole bool) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
			Type:               corev1.NodeReady,
			Status:             status,
			LastTransitionTime: metav1.NewTime(since),
		}}},
	}
	if cpRole {
		n.Labels["node-role.kubernetes.io/control-plane"] = ""
	}
	return n
}

func readyNode(name string) *corev1.Node {
	return notReadyNode(name, corev1.ConditionTrue, t0.Add(-time.Hour), false)
}

// newHealExecutor wires an executor with the injected clock (now = t0,
// baseline = t0-1h so the restart cooldown is already served).
func newHealExecutor(dyn *dynamicfake.FakeDynamicClient, nodes []*corev1.Node) (*Executor, *actions.Store) {
	store := actions.New(nil)
	exec := New(dyn, store, Options{
		Namespace:   ns,
		MaxReplicas: 50,
		Nodes:       func() ([]*corev1.Node, error) { return nodes, nil },
		Now:         func() time.Time { return t0 },
	})
	exec.baseline = t0.Add(-time.Hour)
	return exec, store
}

func machineExists(t *testing.T, dyn *dynamicfake.FakeDynamicClient, name string) bool {
	t.Helper()
	_, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil
}

// Happy path: a worker node NotReady beyond unhealthyAfterSeconds → its
// Machine is deleted, the heal action reports done with the MACHINE name as
// target (contract: stable pre-join, opaque to dashboards) and the node name
// in the detail.
func TestHeal_RemediatesNotReadyWorker(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-abc-1", "pool-a", "worker-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("worker-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false),
	})

	retry := exec.Reconcile(context.Background(), healingDoc(4, 1))

	if !retry {
		t.Errorf("healing enabled must request a re-run every poll tick")
	}
	if machineExists(t, dyn, "pool-a-abc-1") {
		t.Errorf("unhealthy machine not deleted")
	}
	a := findAction(t, store, "pool-a-abc-1")
	if a.Type != state.ActionHeal || a.Status != state.ActionDone || a.Revision != 4 {
		t.Errorf("action = %+v, want done heal at revision 4", a)
	}
	if !strings.Contains(a.Detail, "worker-1") || !strings.Contains(a.Detail, "recreates") {
		t.Errorf("detail must carry the node name and the recreate note: %q", a.Detail)
	}
}

// A node NotReady for LESS than unhealthyAfterSeconds is left alone.
func TestHeal_RespectsUnhealthyAfterWindow(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-abc-1", "pool-a", "worker-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("worker-1", corev1.ConditionFalse, t0.Add(-2*time.Minute), false), // 2m < 5m
	})

	exec.Reconcile(context.Background(), healingDoc(4, 1))

	if !machineExists(t, dyn, "pool-a-abc-1") {
		t.Fatalf("machine deleted before the unhealthy window elapsed")
	}
	if got := store.Snapshot(); got != nil {
		t.Errorf("no action expected inside the window, got %+v", got)
	}
}

// A machine whose node never joined within nodeStartupTimeoutSeconds is
// remediated; a younger joinless machine is not. The pool has four serving
// machines, so the old and the young one together (2 of 6 out of service)
// stay inside the fraction cap.
func TestHeal_RemediatesJoinlessMachine(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-old", "pool-a", "", t0.Add(-20*time.Minute)),
		workerMachine("pool-a-young", "pool-a", "", t0.Add(-2*time.Minute)),
		workerMachine("pool-a-r1", "pool-a", "r-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-r2", "pool-a", "r-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-r3", "pool-a", "r-3", t0.Add(-time.Hour)),
		workerMachine("pool-a-r4", "pool-a", "r-4", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		readyNode("r-1"), readyNode("r-2"), readyNode("r-3"), readyNode("r-4"),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 1))

	if machineExists(t, dyn, "pool-a-old") {
		t.Errorf("joinless machine beyond the startup timeout not deleted")
	}
	if !machineExists(t, dyn, "pool-a-young") {
		t.Errorf("young machine deleted (still inside nodeStartupTimeout)")
	}
	a := findAction(t, store, "pool-a-old")
	if a.Status != state.ActionDone || !strings.Contains(a.Detail, "no node joined") {
		t.Errorf("action = %+v", a)
	}
}

// GUARDRAIL: control-plane nodes are never candidates, even when unhealthy
// beyond every window and machine-backed.
func TestHeal_NeverControlPlaneNode(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("cp-machine", "pool-a", "cp-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("cp-1", corev1.ConditionFalse, t0.Add(-time.Hour), true), // CP role label
	})

	exec.Reconcile(context.Background(), healingDoc(4, 5))

	if !machineExists(t, dyn, "cp-machine") {
		t.Fatalf("control-plane node's machine was deleted")
	}
	if got := store.Snapshot(); got != nil {
		t.Errorf("CP node must be skipped silently, got %+v", got)
	}
}

// GUARDRAIL: a candidate whose owning MD looks control-plane is refused
// loudly (failed action), never deleted.
func TestHeal_RefusesControlPlaneMD(t *testing.T) {
	cpMD := selectorMD("prod-cp-workers") // "cp" segment → isControlPlane heuristic
	dyn := fakeDyn(cpMD,
		workerMachine("prod-cp-workers-1", "prod-cp-workers", "node-x", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("node-x", corev1.ConditionFalse, t0.Add(-time.Hour), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 5))

	if !machineExists(t, dyn, "prod-cp-workers-1") {
		t.Fatalf("machine in CP-looking MD was deleted")
	}
	a := findAction(t, store, "prod-cp-workers-1")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "control-plane") {
		t.Errorf("action = %+v, want CP refusal", a)
	}
}

// GUARDRAIL (storm brake): more unhealthy than maxUnhealthy → NOTHING is
// deleted; every candidate reports failed with the exact refusal detail.
func TestHeal_StormBrake(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-3", "pool-a", "w-3", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionUnknown, t0.Add(-time.Hour), false),
		readyNode("w-3"),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 1)) // 2 unhealthy > 1

	for _, name := range []string{"pool-a-1", "pool-a-2", "pool-a-3"} {
		if !machineExists(t, dyn, name) {
			t.Fatalf("storm brake failed: %s was deleted", name)
		}
	}
	const wantDetail = "unhealthy count 2 exceeds maxUnhealthy 1 — refusing (possible outage)"
	for _, target := range []string{"pool-a-1", "pool-a-2"} {
		a := findAction(t, store, target)
		if a.Status != state.ActionFailed || a.Detail != wantDetail {
			t.Errorf("action %s = %+v, want failed %q", target, a, wantDetail)
		}
	}
	if len(store.Snapshot()) != 2 {
		t.Errorf("healthy machines must not be reported: %+v", store.Snapshot())
	}
}

// maxUnhealthy 0 = "never remediate": any candidate trips the storm brake.
func TestHeal_MaxUnhealthyZeroNeverRemediates(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 0))

	if !machineExists(t, dyn, "pool-a-1") {
		t.Fatalf("maxUnhealthy 0 must never delete")
	}
	if a := findAction(t, store, "pool-a-1"); a.Status != state.ActionFailed {
		t.Errorf("action = %+v, want storm-brake refusal", a)
	}
}

// GUARDRAIL (cooldown): after one remediation the same pool waits
// cooldownSeconds; the second candidate reports pending. A fresh executor
// (restart) starts from a full cooldown baseline.
func TestHeal_CooldownPerPoolAndRestartBaseline(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)))
	nodes := []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		readyNode("w-2"),
	}
	exec, store := newHealExecutor(dyn, nodes)

	// Pass 1: w-1's machine is remediated; lastHeal[pool-a] = t0.
	exec.Reconcile(context.Background(), healingDoc(4, 2))
	if machineExists(t, dyn, "pool-a-1") {
		t.Fatalf("first remediation did not happen")
	}

	// w-2 goes unhealthy immediately after → still in pool-a's cooldown.
	nodes[1] = notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-10*time.Minute), false)
	exec.Reconcile(context.Background(), healingDoc(4, 2))
	if !machineExists(t, dyn, "pool-a-2") {
		t.Fatalf("cooldown violated: second machine deleted inside cooldownSeconds")
	}
	if a := findAction(t, store, "pool-a-2"); a.Status != state.ActionPending || !strings.Contains(a.Detail, "cooldown") {
		t.Errorf("action = %+v, want cooldown pending", a)
	}

	// Advance past the cooldown → the second remediation proceeds.
	exec.now = func() time.Time { return t0.Add(16 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 2))
	if machineExists(t, dyn, "pool-a-2") {
		t.Fatalf("remediation still blocked after cooldown elapsed")
	}

	// RESTART = conservative fresh cooldown: a brand-new executor (baseline =
	// construction time t0) must NOT delete despite the node being long-dead.
	dyn2 := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-9", "pool-a", "w-9", t0.Add(-time.Hour)))
	store2 := actions.New(nil)
	exec2 := New(dyn2, store2, Options{
		Namespace:   ns,
		MaxReplicas: 50,
		Nodes: func() ([]*corev1.Node, error) {
			return []*corev1.Node{notReadyNode("w-9", corev1.ConditionFalse, t0.Add(-time.Hour), false)}, nil
		},
		Now: func() time.Time { return t0 },
	})
	exec2.Reconcile(context.Background(), healingDoc(4, 2))
	if !machineExists(t, dyn2, "pool-a-9") {
		t.Fatalf("restart baseline violated: machine deleted immediately after agent start")
	}
	if a := findAction(t, store2, "pool-a-9"); a.Status != state.ActionPending || !strings.Contains(a.Detail, "cooldown") {
		t.Errorf("action = %+v, want restart-cooldown pending", a)
	}
}

// GUARDRAIL (concurrency budget): machines already being deleted count as
// in-flight disruptions; at budget 0 a new candidate waits (pending).
func TestHeal_ConcurrencyBudgetCountsInFlight(t *testing.T) {
	deleting := workerMachine("pool-b-going", "pool-b", "", t0.Add(-time.Hour))
	delTS := metav1.NewTime(t0)
	deleting.SetDeletionTimestamp(&delTS)
	deleting.SetFinalizers([]string{"machine.cluster.k8s.io"})

	dyn := fakeDyn(selectorMD("pool-a"), selectorMD("pool-b"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		deleting)
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 1)) // budget 1 - 1 in flight = 0

	if !machineExists(t, dyn, "pool-a-1") {
		t.Fatalf("budget violated: deleted while a disruption is in flight")
	}
	if a := findAction(t, store, "pool-a-1"); a.Status != state.ActionPending || !strings.Contains(a.Detail, "in flight") {
		t.Errorf("action = %+v, want in-flight pending", a)
	}
}

// GUARDRAIL: machines in autoscaler-managed pools are refused, never deleted.
func TestHeal_SkipsAutoscalerPools(t *testing.T) {
	asMD := selectorMD("pool-as")
	asMD.SetAnnotations(map[string]string{
		"cluster.k8s.io/cluster-api-autoscaler-node-group-min-size": "1",
	})
	dyn := fakeDyn(asMD,
		workerMachine("pool-as-1", "pool-as", "w-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 5))

	if !machineExists(t, dyn, "pool-as-1") {
		t.Fatalf("autoscaler-owned machine was deleted")
	}
	a := findAction(t, store, "pool-as-1")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "cluster-autoscaler") {
		t.Errorf("action = %+v, want autoscaler refusal", a)
	}
}

// GUARDRAIL: an unowned machine (no MD resolves) is refused — nothing would
// recreate it.
func TestHeal_RefusesUnownedMachine(t *testing.T) {
	dyn := fakeDyn( // machine present, NO MDs at all
		workerMachine("stray-1", "no-such-pool", "w-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 5))

	if !machineExists(t, dyn, "stray-1") {
		t.Fatalf("unowned machine was deleted")
	}
	a := findAction(t, store, "stray-1")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "no owning MachineDeployment") {
		t.Errorf("action = %+v, want unowned refusal", a)
	}
}

// GATE: execution.healing=false (even with healing.enabled=true in the served
// policy) → no healing; and vice versa — BOTH bits are required.
func TestHeal_ServerGateRequiresBothBits(t *testing.T) {
	for name, mutate := range map[string]func(*desired.Doc){
		"execution off": func(d *desired.Doc) { d.Execution.Healing = false },
		"policy off":    func(d *desired.Doc) { d.Healing.Enabled = false },
	} {
		dyn := fakeDyn(selectorMD("pool-a"),
			workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)))
		exec, store := newHealExecutor(dyn, []*corev1.Node{
			notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		})

		doc := healingDoc(4, 5)
		mutate(doc)
		retry := exec.Reconcile(context.Background(), doc)

		if !machineExists(t, dyn, "pool-a-1") {
			t.Fatalf("%s: machine deleted despite the gate", name)
		}
		if got := store.Snapshot(); got != nil {
			t.Errorf("%s: actions reported despite the gate: %+v", name, got)
		}
		if retry {
			t.Errorf("%s: nothing to do must not request retry", name)
		}
	}
}

// A delete failure halts the pass (remaining candidates queue as pending) —
// the §3 halt-on-failure discipline.
func TestHeal_HaltsOnDeleteFailure(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"), selectorMD("pool-b"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-b-1", "pool-b", "w-2", t0.Add(-time.Hour)))
	dyn.PrependReactor("delete", "machines", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("machines.cluster.k8s.io is forbidden")
	})
	// The two failures are STAGGERED on purpose: two nodes failing inside
	// simultaneousFailureWindow is an outage signature and would be refused
	// before any delete is attempted (see
	// TestHeal_RefusesSimultaneousClusterFailure).
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-20*time.Minute), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 5))

	if a := findAction(t, store, "pool-a-1"); a.Status != state.ActionFailed || !strings.Contains(a.Detail, "delete failed") {
		t.Errorf("first action = %+v, want failed delete", a)
	}
	if b := findAction(t, store, "pool-b-1"); b.Status != state.ActionPending || !strings.Contains(b.Detail, "queued") {
		t.Errorf("second action = %+v, want queued behind the failure", b)
	}
}

// A node that recovers on its own retires its stale pending/refused report;
// done outcomes stay visible for the revision. The pool has four serving
// machines, so the two unhealthy ones stay inside the fraction cap (2 of 6).
func TestHeal_PrunesRecoveredReportsKeepsDone(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-r1", "pool-a", "r-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-r2", "pool-a", "r-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-r3", "pool-a", "r-3", t0.Add(-time.Hour)),
		workerMachine("pool-a-r4", "pool-a", "r-4", t0.Add(-time.Hour)))
	nodes := []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-10*time.Minute), false),
		readyNode("r-1"), readyNode("r-2"), readyNode("r-3"), readyNode("r-4"),
	}
	exec, store := newHealExecutor(dyn, nodes)

	// Pass 1: w-1 remediated (done); w-2 blocked by pool cooldown (pending).
	exec.Reconcile(context.Background(), healingDoc(4, 2))
	if a := findAction(t, store, "pool-a-2"); a.Status != state.ActionPending {
		t.Fatalf("precondition: want pending for pool-a-2, got %+v", a)
	}

	// w-2 recovers on its own → its pending report is pruned; the done
	// remediation of pool-a-1 stays.
	nodes[1] = readyNode("w-2")
	exec.Reconcile(context.Background(), healingDoc(4, 2))

	snap := store.Snapshot()
	if len(snap) != 1 || snap[0].Target != "pool-a-1" || snap[0].Status != state.ActionDone {
		t.Errorf("snapshot = %+v, want only the done pool-a-1 heal", snap)
	}
}

// POLICY KNOBS: the served numbers drive the windows — a longer
// unhealthyAfterSeconds defers remediation the agent would otherwise perform,
// and the sub-60s floor clamps a junk policy tighter, never looser.
func TestHeal_PolicyKnobsRespected(t *testing.T) {
	mk := func() (*Executor, *actions.Store, *dynamicfake.FakeDynamicClient) {
		dyn := fakeDyn(selectorMD("pool-a"),
			workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)))
		exec, store := newHealExecutor(dyn, []*corev1.Node{
			notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false), // unhealthy 10m
		})
		return exec, store, dyn
	}

	// unhealthyAfter 30m > 10m observed → no action.
	exec, store, dyn := mk()
	doc := healingDoc(4, 1)
	doc.Healing.UnhealthyAfterSeconds = 1800
	exec.Reconcile(context.Background(), doc)
	if !machineExists(t, dyn, "pool-a-1") || store.Snapshot() != nil {
		t.Errorf("unhealthyAfterSeconds knob ignored")
	}

	// A junk 0s window is FLOORED to 60s, not treated as instant: a node
	// unhealthy for 30s stays untouched.
	dyn2 := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)))
	exec2, store2 := newHealExecutor(dyn2, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-30*time.Second), false),
	})
	doc2 := healingDoc(4, 1)
	doc2.Healing.UnhealthyAfterSeconds = 0
	exec2.Reconcile(context.Background(), doc2)
	if !machineExists(t, dyn2, "pool-a-1") || store2.Snapshot() != nil {
		t.Errorf("zero window not floored to 60s (would flap with kubelet grace periods)")
	}

	// A cooldown of 0 is FLOORED to five minutes: the document cannot make
	// healing delete one pool's machines back to back. Two staggered failures
	// in a pool of six (neither the fraction cap nor the outage refusal
	// interferes): one deletion now, the second only after the floor.
	dyn3 := fakeDyn(selectorMDWithReplicas("pool-a", 6),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-3", "pool-a", "w-3", t0.Add(-time.Hour)),
		workerMachine("pool-a-4", "pool-a", "w-4", t0.Add(-time.Hour)),
		workerMachine("pool-a-5", "pool-a", "w-5", t0.Add(-time.Hour)),
		workerMachine("pool-a-6", "pool-a", "w-6", t0.Add(-time.Hour)))
	exec3, store3 := newHealExecutor(dyn3, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-20*time.Minute), false),
		readyNode("w-3"), readyNode("w-4"), readyNode("w-5"), readyNode("w-6"),
	})
	doc3 := healingDoc(4, 2)
	doc3.Healing.CooldownSeconds = 0
	exec3.Reconcile(context.Background(), doc3)
	if machineExists(t, dyn3, "pool-a-1") {
		t.Fatalf("the first remediation did not happen")
	}
	if !machineExists(t, dyn3, "pool-a-2") {
		t.Fatalf("cooldown 0 deleted a second machine of the pool in the same pass — the floor did not hold")
	}
	if a := findAction(t, store3, "pool-a-2"); a.Status != state.ActionPending || !strings.Contains(a.Detail, "cooldown") {
		t.Errorf("action = %+v, want cooldown pending", a)
	}
	exec3.now = func() time.Time { return t0.Add(4 * time.Minute) }
	exec3.Reconcile(context.Background(), doc3)
	if !machineExists(t, dyn3, "pool-a-2") {
		t.Fatalf("cooldown 0 deleted the second machine before the five-minute floor")
	}
	exec3.now = func() time.Time { return t0.Add(6 * time.Minute) }
	exec3.Reconcile(context.Background(), doc3)
	if machineExists(t, dyn3, "pool-a-2") {
		t.Errorf("the second remediation is still blocked after the floor elapsed")
	}
}

// LOCAL BOUND (partition): when every node of a pool stops reporting inside
// one short window, the agent cannot tell dead hardware from a partitioned
// node — and a partitioned server is alive, running the user's workload.
// Nothing is deleted and the refusal is reported.
//
// The fixture isolates the POOL rule: a healthy worker in another pool keeps
// the cluster-wide rule out of it. The fraction cap stops these deletions
// too (3 of 3 out of service), but it reports a wait; the pool rule names
// the outage and reports a refusal, which is what the test pins.
func TestHeal_RefusesSimultaneousPoolFailure(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"), selectorMD("pool-b"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-3", "pool-a", "w-3", t0.Add(-time.Hour)),
		workerMachine("pool-b-1", "pool-b", "b-1", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionUnknown, t0.Add(-9*time.Minute), false),
		notReadyNode("w-2", corev1.ConditionUnknown, t0.Add(-8*time.Minute), false),
		notReadyNode("w-3", corev1.ConditionUnknown, t0.Add(-6*time.Minute), false),
		readyNode("b-1"),
	})

	// A wrong-but-plausible maxUnhealthy: the storm brake, the budget and the
	// cooldown are all satisfied.
	exec.Reconcile(context.Background(), healingDoc(4, 10))

	const want = "all 3 nodes of pool pool-a went unhealthy within"
	for _, name := range []string{"pool-a-1", "pool-a-2", "pool-a-3"} {
		if !machineExists(t, dyn, name) {
			t.Fatalf("%s deleted during a whole-pool failure — a partition is not hardware failure", name)
		}
		a := findAction(t, store, name)
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, want) {
			t.Errorf("action %s = %+v, want the pool refusal %q", name, a, want)
		}
	}
}

// MISSING EVIDENCE refuses (pool). One not-Ready node of pool-a reports no
// Ready transition time, so the agent cannot tell whether it failed together
// with the candidate. Four nodes serve, so neither the outage rule nor the
// fraction cap (2 of 6 out of service) stops the deletion: only the missing
// evidence does.
func TestHeal_UnknownTransitionTimeRefusesThePool(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)),
		workerMachine("pool-a-3", "pool-a", "w-3", t0.Add(-time.Hour)),
		workerMachine("pool-a-4", "pool-a", "w-4", t0.Add(-time.Hour)),
		workerMachine("pool-a-5", "pool-a", "w-5", t0.Add(-time.Hour)),
		workerMachine("pool-a-6", "pool-a", "w-6", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionUnknown, t0.Add(-9*time.Minute), false),
		readyNode("w-2"), readyNode("w-3"), readyNode("w-4"), readyNode("w-5"),
		noTransitionNode("w-6", false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 10))

	if !machineExists(t, dyn, "pool-a-1") {
		t.Fatalf("pool-a-1 deleted although a node of its pool reports no Ready transition time")
	}
	a := findAction(t, store, "pool-a-1")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "node w-6 of pool pool-a is not Ready and reports no Ready transition time") {
		t.Errorf("action = %+v, want the missing-evidence refusal", a)
	}
}

// MISSING EVIDENCE refuses (cluster). Two one-machine pools: one node failed
// eight minutes ago, the other has no Ready condition at all. The unknown one
// could have failed at the same moment, so the whole worker set reads as an
// outage.
func TestHeal_UnknownTransitionTimeCountsAsSimultaneous(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"), selectorMD("pool-b"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-b-1", "pool-b", "w-2", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionUnknown, t0.Add(-8*time.Minute), false),
		noTransitionNode("w-2", true),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 10))

	if !machineExists(t, dyn, "pool-a-1") {
		t.Fatalf("pool-a-1 deleted while the only other worker reports no Ready transition time")
	}
	a := findAction(t, store, "pool-a-1")
	if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "all 2 worker nodes") {
		t.Errorf("action = %+v, want the cluster-wide outage refusal", a)
	}
}

// LOCAL BOUND (cluster-wide): one-machine pools have nothing to be
// simultaneous with, so the same evidence is read across the whole worker set
// — the shape a control-plane or network outage has on a cluster of small
// pools.
func TestHeal_RefusesSimultaneousClusterFailure(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"), selectorMD("pool-b"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-b-1", "pool-b", "w-2", t0.Add(-time.Hour)))
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionUnknown, t0.Add(-8*time.Minute), false),
		notReadyNode("w-2", corev1.ConditionUnknown, t0.Add(-7*time.Minute), false),
	})

	exec.Reconcile(context.Background(), healingDoc(4, 10))

	for _, name := range []string{"pool-a-1", "pool-b-1"} {
		if !machineExists(t, dyn, name) {
			t.Fatalf("%s deleted while every worker node was unhealthy at once", name)
		}
		a := findAction(t, store, name)
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, "all 2 worker nodes") {
			t.Errorf("action %s = %+v, want the cluster-wide outage refusal", name, a)
		}
	}
}

// LOCAL BOUND (fraction): healing acts on a pool only while at most a third
// of it is out of service. Twelve machines, eleven of them partitioned
// (Unknown at the same moment) and one Ready, so the outage refusal does not
// engage; the document is wrong but plausible (maxUnhealthy 50, cooldown 0).
// Pass after pass nothing is deleted, and every candidate says why it waits.
// Deleting a machine that does not serve never brings the count down, so a
// bound that subtracts the candidates lets healing delete four of them, and
// then more as soon as their replacements fail.
func TestHeal_FractionRefusesAPoolMostlyOutOfService(t *testing.T) {
	objs := []runtime.Object{selectorMDWithReplicas("pool-a", 12),
		workerMachine("pool-a-ready", "pool-a", "w-ready", t0.Add(-time.Hour))}
	nodes := []*corev1.Node{readyNode("w-ready")}
	var sick []string
	for i := 0; i < 11; i++ {
		name := "pool-a-" + string(rune('a'+i))
		node := "w-" + string(rune('a'+i))
		sick = append(sick, name)
		objs = append(objs, workerMachine(name, "pool-a", node, t0.Add(-time.Hour)))
		nodes = append(nodes, notReadyNode(node, corev1.ConditionUnknown, t0.Add(-10*time.Minute), false))
	}
	dyn := fakeDyn(objs...)
	exec, store := newHealExecutor(dyn, nodes)

	doc := healingDoc(4, 50)
	doc.Healing.CooldownSeconds = 0
	// One pass every six minutes, past the cooldown floor each time.
	for pass := 0; pass < 8; pass++ {
		at := t0.Add(time.Duration(pass) * 6 * time.Minute)
		exec.now = func() time.Time { return at }
		exec.Reconcile(context.Background(), doc)
	}
	for _, name := range sick {
		if !machineExists(t, dyn, name) {
			t.Fatalf("%s deleted while 11 of 12 machines of its pool were out of service", name)
		}
		a := findAction(t, store, name)
		if a.Status != state.ActionPending || !strings.Contains(a.Detail, "has 11 of its 12 machines out of service") {
			t.Errorf("action %s = %+v, want the fraction wait", name, a)
		}
	}
}

// LOCAL BOUND (fraction, failed replacements): a replacement that never
// joins becomes a candidate itself once it is older than the startup
// timeout. It must keep counting as out of service: the deletion that caused
// it has not recovered. Twelve machines, four of them failed one after the
// other, eight Ready; a wrong-but-plausible document (maxUnhealthy 50,
// cooldown 0).
//
//  1. Healing deletes the four, one pass at a time (4 of 12 is the cap).
//  2. Their four replacements never join, and two more machines fail.
//     Six of twelve are out of service: nothing more is deleted, whatever
//     the candidates are.
//  3. Two replacements join and serve. Four of twelve are out of service
//     again, and healing acts again.
func TestHeal_FailedReplacementsKeepCounting(t *testing.T) {
	objs := []runtime.Object{selectorMDWithReplicas("pool-a", 12)}
	nodes := make([]*corev1.Node, 0, 16)
	for i := 0; i < 12; i++ {
		name := "pool-a-" + string(rune('a'+i))
		node := "w-" + string(rune('a'+i))
		objs = append(objs, workerMachine(name, "pool-a", node, t0.Add(-2*time.Hour)))
		if i < 4 {
			// Staggered by ten minutes: no outage signature.
			nodes = append(nodes, notReadyNode(node, corev1.ConditionFalse, t0.Add(-time.Duration(60-10*i)*time.Minute), false))
			continue
		}
		nodes = append(nodes, readyNode(node))
	}
	dyn := fakeDyn(objs...)
	exec, store := newHealExecutor(dyn, nil)
	exec.nodes = func() ([]*corev1.Node, error) { return nodes, nil }

	doc := healingDoc(4, 50)
	doc.Healing.CooldownSeconds = 0
	pass := 0
	run := func(n int) {
		for ; n > 0; n-- {
			at := t0.Add(time.Duration(pass) * 6 * time.Minute)
			exec.now = func() time.Time { return at }
			exec.Reconcile(context.Background(), doc)
			pass++
		}
	}
	gone := func(names ...string) int {
		n := 0
		for _, name := range names {
			if !machineExists(t, dyn, name) {
				n++
			}
		}
		return n
	}
	originals := []string{"pool-a-e", "pool-a-f", "pool-a-g", "pool-a-h", "pool-a-i", "pool-a-j", "pool-a-k", "pool-a-l"}

	// 1. The four failed machines go, one per pass.
	run(4)
	if got := gone("pool-a-a", "pool-a-b", "pool-a-c", "pool-a-d"); got != 4 {
		t.Fatalf("phase 1: %d of the 4 failed machines deleted, want 4", got)
	}

	// 2. Four replacements that never join (created one pass ago, older than
	// the startup timeout by the time phase 2 runs), and two more machines
	// fail.
	created := t0.Add(time.Duration(pass-1) * 6 * time.Minute)
	var replacements []string
	for i := 1; i <= 4; i++ {
		name := "pool-a-new" + string(rune('0'+i))
		replacements = append(replacements, name)
		if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Create(context.Background(),
			workerMachine(name, "pool-a", "", created), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	nodes[4] = notReadyNode("w-e", corev1.ConditionFalse, t0.Add(time.Duration(pass-1)*6*time.Minute-20*time.Minute), false)
	nodes[5] = notReadyNode("w-f", corev1.ConditionFalse, t0.Add(time.Duration(pass-1)*6*time.Minute-10*time.Minute), false)
	pass += 2 // the replacements are now 18 minutes old, past the 10-minute startup timeout
	run(4)
	if got := gone(originals...) + gone(replacements...); got != 0 {
		t.Fatalf("phase 2: %d machines deleted while 6 of 12 were out of service (4 replacements never joined), want 0", got)
	}
	for _, name := range append([]string{"pool-a-e", "pool-a-f"}, replacements...) {
		a := findAction(t, store, name)
		if a.Status != state.ActionPending || !strings.Contains(a.Detail, "has 6 of its 12 machines out of service") {
			t.Errorf("phase 2: action %s = %+v, want the fraction wait", name, a)
		}
	}

	// 3. Two replacements join and serve: four of twelve are out of service,
	// and healing acts again.
	for i, name := range replacements[:2] {
		node := "w-new" + string(rune('1'+i))
		m, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		m.Object["status"] = map[string]any{"nodeRef": map[string]any{"kind": "Node", "name": node}}
		if _, err := dyn.Resource(machines.MachineGVR).Namespace(ns).Update(context.Background(), m, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, readyNode(node))
	}
	run(1)
	if got := gone("pool-a-e", "pool-a-f") + gone(replacements[2:]...); got != 1 {
		t.Errorf("phase 3: %d candidates deleted after two replacements served, want 1", got)
	}
	if got := gone(originals[2:]...) + gone(replacements[:2]...); got != 0 {
		t.Errorf("phase 3: %d serving machines deleted, want 0", got)
	}
}
