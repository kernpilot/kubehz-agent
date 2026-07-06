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
// workerset=<name> (the authoritative pool mapping).
func selectorMD(name string) *unstructured.Unstructured {
	u := md(name, 2)
	if err := unstructured.SetNestedMap(u.Object,
		map[string]any{"workerset": name}, "spec", "selector", "matchLabels"); err != nil {
		panic(err)
	}
	return u
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
// remediated; a younger joinless machine is not.
func TestHeal_RemediatesJoinlessMachine(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-old", "pool-a", "", t0.Add(-20*time.Minute)),
		workerMachine("pool-a-young", "pool-a", "", t0.Add(-2*time.Minute)))
	exec, store := newHealExecutor(dyn, nil)

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
	exec, store := newHealExecutor(dyn, []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-time.Hour), false),
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
// done outcomes stay visible for the revision.
func TestHeal_PrunesRecoveredReportsKeepsDone(t *testing.T) {
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)))
	nodes := []*corev1.Node{
		notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
		notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-10*time.Minute), false),
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

	// CooldownSeconds 0 disables the pause: two remediations in the same pool
	// back to back.
	dyn3 := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		workerMachine("pool-a-2", "pool-a", "w-2", t0.Add(-time.Hour)))
	store3 := actions.New(nil)
	exec3 := New(dyn3, store3, Options{
		Namespace: ns, MaxReplicas: 50,
		Nodes: func() ([]*corev1.Node, error) {
			return []*corev1.Node{
				notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false),
				notReadyNode("w-2", corev1.ConditionFalse, t0.Add(-time.Hour), false),
			}, nil
		},
		Now: func() time.Time { return t0 },
	})
	doc3 := healingDoc(4, 2)
	doc3.Healing.CooldownSeconds = 0
	exec3.Reconcile(context.Background(), doc3)
	if machineExists(t, dyn3, "pool-a-1") || machineExists(t, dyn3, "pool-a-2") {
		t.Errorf("cooldown 0 must allow immediate remediation (both machines)")
	}
}
