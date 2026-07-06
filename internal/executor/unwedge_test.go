package executor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// testEvictionTimeout keeps the unwedge trigger explicit in every scenario.
const testEvictionTimeout = 5 * time.Minute

// podFixture builds the same pod twice: typed (the executor's pod source —
// the informer-lister stand-in) and unstructured (the fake dynamic tracker
// the force-deletes hit).
func podFixture(namespace, name, node string, terminating bool) (*corev1.Pod, *unstructured.Unstructured) {
	p := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("uid-" + name)},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	if terminating {
		ts := metav1.NewTime(t0.Add(-3 * time.Minute))
		p.DeletionTimestamp = &ts
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(p)
	if err != nil {
		panic(err)
	}
	return p, &unstructured.Unstructured{Object: obj}
}

// stickyMachineDelete makes machine deletes behave like the real
// machine-controller finalizer: the Machine gets a deletionTimestamp and
// STAYS (deleting) instead of vanishing — the wedge under test.
func stickyMachineDelete(dyn *dynamicfake.FakeDynamicClient) {
	dyn.PrependReactor("delete", "machines", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteAction)
		obj, err := dyn.Tracker().Get(machines.MachineGVR, da.GetNamespace(), da.GetName())
		if err != nil {
			return true, nil, err
		}
		u := obj.(*unstructured.Unstructured).DeepCopy()
		ts := metav1.NewTime(t0)
		u.SetDeletionTimestamp(&ts)
		u.SetFinalizers([]string{"machine-machineset-delete-finalizer"})
		if err := dyn.Tracker().Update(machines.MachineGVR, u, da.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, nil, nil
	})
}

// newUnwedgeExecutor wires an executor with node + pod sources and the test
// eviction timeout (clock t0; baseline an hour earlier so the restart
// cooldown is served, as in the heal tests).
func newUnwedgeExecutor(dyn *dynamicfake.FakeDynamicClient, nodes []*corev1.Node, pods []*corev1.Pod) (*Executor, *actions.Store) {
	store := actions.New(nil)
	exec := New(dyn, store, Options{
		Namespace:       ns,
		MaxReplicas:     50,
		Nodes:           func() ([]*corev1.Node, error) { return nodes, nil },
		Pods:            func() ([]*corev1.Pod, error) { return pods, nil },
		EvictionTimeout: testEvictionTimeout,
		Now:             func() time.Time { return t0 },
	})
	exec.baseline = t0.Add(-time.Hour)
	return exec, store
}

func podExists(t *testing.T, dyn *dynamicfake.FakeDynamicClient, namespace, name string) bool {
	t.Helper()
	_, err := dyn.Resource(podsGVR).Namespace(namespace).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil
}

func podDeleteActions(dyn *dynamicfake.FakeDynamicClient) []k8stesting.DeleteActionImpl {
	var out []k8stesting.DeleteActionImpl
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
			out = append(out, a.(k8stesting.DeleteActionImpl))
		}
	}
	return out
}

// Happy path: a heal-deleted machine still deleting past the eviction
// timeout, node still dead → EXACTLY the stuck-Terminating pods on that node
// are force-deleted (grace 0, UID precondition), once, and the heal action
// detail reports the unwedge.
func TestUnwedge_ForceDeletesStuckPodsOnce(t *testing.T) {
	stuck1t, stuck1u := podFixture("app", "web-0", "w-1", true)
	stuck2t, stuck2u := podFixture("app", "web-1", "w-1", true)
	livet, liveu := podFixture("app", "web-2", "w-1", false)  // NOT terminating
	othert, otheru := podFixture("app", "web-3", "w-2", true) // other node
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		stuck1u, stuck2u, liveu, otheru)
	stickyMachineDelete(dyn)
	exec, store := newUnwedgeExecutor(dyn,
		[]*corev1.Node{notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false)},
		[]*corev1.Pod{stuck1t, stuck2t, livet, othert})

	// Pass 1 (t0): remediation deletes the Machine; it sticks deleting.
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if got := podDeleteActions(dyn); len(got) != 0 {
		t.Fatalf("pods force-deleted during remediation pass: %+v", got)
	}

	// Pass 2 (t0+6m > timeout): the unwedge fires on the two stuck pods only.
	exec.now = func() time.Time { return t0.Add(6 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))

	if podExists(t, dyn, "app", "web-0") || podExists(t, dyn, "app", "web-1") {
		t.Errorf("stuck pods on the dead node were not force-deleted")
	}
	if !podExists(t, dyn, "app", "web-2") {
		t.Errorf("non-terminating pod on the dead node was deleted — unwedge must only confirm existing evictions")
	}
	if !podExists(t, dyn, "app", "web-3") {
		t.Errorf("terminating pod on ANOTHER node was deleted — unwedge must stay node-scoped")
	}
	dels := podDeleteActions(dyn)
	if len(dels) != 2 {
		t.Fatalf("pod deletes = %d, want 2", len(dels))
	}
	for _, d := range dels {
		if d.DeleteOptions.GracePeriodSeconds == nil || *d.DeleteOptions.GracePeriodSeconds != 0 {
			t.Errorf("force-delete without gracePeriodSeconds 0: %+v", d.DeleteOptions)
		}
		if d.DeleteOptions.Preconditions == nil || d.DeleteOptions.Preconditions.UID == nil {
			t.Errorf("force-delete without a UID precondition (same-name replacement unprotected): %+v", d.DeleteOptions)
		}
	}
	a := findAction(t, store, "pool-a-1")
	if a.Type != state.ActionHeal || a.Status != state.ActionDone {
		t.Errorf("action = %+v, want done heal", a)
	}
	if !strings.Contains(a.Detail, "unwedged") || !strings.Contains(a.Detail, "2 pod(s)") || !strings.Contains(a.Detail, "w-1") {
		t.Errorf("detail must report the unwedge (count + node): %q", a.Detail)
	}

	// Pass 3: ONE attempt per machine — nothing fires again, even though the
	// machine is still deleting and the node still dead.
	_, lateStuck := podFixture("app", "web-9", "w-1", true)
	if err := dyn.Tracker().Add(lateStuck); err != nil {
		t.Fatal(err)
	}
	lateTyped, _ := podFixture("app", "web-9", "w-1", true)
	exec.pods = func() ([]*corev1.Pod, error) { return []*corev1.Pod{lateTyped}, nil }
	exec.now = func() time.Time { return t0.Add(20 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if got := podDeleteActions(dyn); len(got) != 2 {
		t.Errorf("pod deletes after attempt burned = %d, want still 2 (one unwedge per machine)", len(got))
	}

	// Teardown completes (machine gone) → the tracking is pruned.
	if err := dyn.Tracker().Delete(machines.MachineGVR, ns, "pool-a-1"); err != nil {
		t.Fatal(err)
	}
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if len(exec.healDeleted) != 0 {
		t.Errorf("tracking not pruned after teardown completed: %+v", exec.healDeleted)
	}
}

// HARD BOUND: a machine the agent did NOT heal-delete (manual delete,
// scale-down, P6 roll) is never unwedged — no matter how stuck it looks.
func TestUnwedge_NeverTouchesUntrackedMachine(t *testing.T) {
	deleting := workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour))
	delTS := metav1.NewTime(t0.Add(-time.Hour))
	deleting.SetDeletionTimestamp(&delTS)
	deleting.SetFinalizers([]string{"machine-machineset-delete-finalizer"})

	stuckT, stuckU := podFixture("app", "web-0", "w-1", true)
	dyn := fakeDyn(selectorMD("pool-a"), deleting, stuckU)
	exec, _ := newUnwedgeExecutor(dyn,
		[]*corev1.Node{notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-time.Hour), false)},
		[]*corev1.Pod{stuckT})

	// Several passes, all far beyond any timeout: tracking is empty, so the
	// unwedge must never fire.
	for _, offset := range []time.Duration{0, 10 * time.Minute, time.Hour} {
		off := offset
		exec.now = func() time.Time { return t0.Add(off) }
		exec.Reconcile(context.Background(), healingDoc(4, 1))
	}
	if got := podDeleteActions(dyn); len(got) != 0 {
		t.Errorf("untracked machine's pods were force-deleted: %+v", got)
	}
	if !podExists(t, dyn, "app", "web-0") {
		t.Errorf("stuck pod of an untracked machine was deleted")
	}
}

// HARD BOUND: a recovered node (Ready=True) confirms its own pods — the
// unwedge does nothing, and the attempt is NOT burned (if the node dies
// again while the machine is still deleting, the unwedge may still fire).
func TestUnwedge_SkipsRecoveredNode(t *testing.T) {
	stuckT, stuckU := podFixture("app", "web-0", "w-1", true)
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		stuckU)
	stickyMachineDelete(dyn)
	nodes := []*corev1.Node{notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false)}
	exec, _ := newUnwedgeExecutor(dyn, nodes, []*corev1.Pod{stuckT})

	// Pass 1: remediate. Node then recovers.
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	nodes[0] = readyNode("w-1")

	// Pass 2, beyond the timeout: node is Ready → do nothing.
	exec.now = func() time.Time { return t0.Add(6 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if got := podDeleteActions(dyn); len(got) != 0 {
		t.Fatalf("unwedge fired on a recovered node: %+v", got)
	}

	// Node goes unreachable again (Unknown counts) while still deleting →
	// the unheld attempt fires now.
	nodes[0] = notReadyNode("w-1", corev1.ConditionUnknown, t0.Add(6*time.Minute), false)
	exec.now = func() time.Time { return t0.Add(7 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if got := podDeleteActions(dyn); len(got) != 1 {
		t.Errorf("unwedge did not fire after the node went unreachable again: %d deletes", len(got))
	}
}

// RBAC fail-soft: without the pods-delete ClusterRole the attempt warns and
// reports — no crash, no retry loop (the attempt is burned; RBAC does not
// heal itself between polls).
func TestUnwedge_RBACDeniedWarnsNoCrash(t *testing.T) {
	stuckT, stuckU := podFixture("app", "web-0", "w-1", true)
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		stuckU)
	stickyMachineDelete(dyn)
	dyn.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "web-0", errors.New("rbac denied"))
	})
	exec, store := newUnwedgeExecutor(dyn,
		[]*corev1.Node{notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false)},
		[]*corev1.Pod{stuckT})

	exec.Reconcile(context.Background(), healingDoc(4, 1))
	exec.now = func() time.Time { return t0.Add(6 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1)) // must not panic

	a := findAction(t, store, "pool-a-1")
	if a.Status != state.ActionDone || !strings.Contains(a.Detail, "unwedge forbidden") || !strings.Contains(a.Detail, "RBAC") {
		t.Errorf("action = %+v, want done heal with a loud RBAC-forbidden unwedge note", a)
	}
	if !podExists(t, dyn, "app", "web-0") {
		t.Errorf("pod vanished despite forbidden delete")
	}

	// The attempt is burned: later passes do not hammer the apiserver.
	before := len(podDeleteActions(dyn))
	exec.now = func() time.Time { return t0.Add(20 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))
	if got := len(podDeleteActions(dyn)); got != before {
		t.Errorf("forbidden unwedge retried: %d → %d delete attempts", before, got)
	}
}

// The timeout is respected exactly: no force-delete while the machine has
// been deleting for less than KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS.
func TestUnwedge_TimeoutRespected(t *testing.T) {
	stuckT, stuckU := podFixture("app", "web-0", "w-1", true)
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-1", "pool-a", "w-1", t0.Add(-time.Hour)),
		stuckU)
	stickyMachineDelete(dyn)
	exec, _ := newUnwedgeExecutor(dyn,
		[]*corev1.Node{notReadyNode("w-1", corev1.ConditionFalse, t0.Add(-10*time.Minute), false)},
		[]*corev1.Pod{stuckT})

	exec.Reconcile(context.Background(), healingDoc(4, 1)) // remediate at t0

	for _, tc := range []struct {
		offset time.Duration
		fired  bool
	}{
		{2 * time.Minute, false},
		{testEvictionTimeout - time.Second, false},
		{testEvictionTimeout, true}, // now-deletedAt == timeout → no longer "before"
	} {
		off := tc.offset
		exec.now = func() time.Time { return t0.Add(off) }
		exec.Reconcile(context.Background(), healingDoc(4, 1))
		fired := len(podDeleteActions(dyn)) > 0
		if fired != tc.fired {
			t.Errorf("at +%s: unwedge fired=%t, want %t", off, fired, tc.fired)
		}
	}
}

// A joinless machine (heal-deleted for nodeStartupTimeout) has no node and no
// pods — the unwedge has nothing to do and must not touch anything.
func TestUnwedge_JoinlessMachineNothingToUnwedge(t *testing.T) {
	stuckT, stuckU := podFixture("app", "web-0", "w-9", true) // unrelated node
	dyn := fakeDyn(selectorMD("pool-a"),
		workerMachine("pool-a-old", "pool-a", "", t0.Add(-20*time.Minute)),
		stuckU)
	stickyMachineDelete(dyn)
	exec, _ := newUnwedgeExecutor(dyn, nil, []*corev1.Pod{stuckT})

	exec.Reconcile(context.Background(), healingDoc(4, 1)) // remediates the joinless machine
	exec.now = func() time.Time { return t0.Add(10 * time.Minute) }
	exec.Reconcile(context.Background(), healingDoc(4, 1))

	if got := podDeleteActions(dyn); len(got) != 0 {
		t.Errorf("joinless machine unwedge touched pods: %+v", got)
	}
}
