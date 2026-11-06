package k3supgrade

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/desired"
)

var ctx = context.Background()

func TestPlanTemplates(t *testing.T) {
	srv := ServerPlan(opID, targetV)
	agt := AgentPlan(opID, targetV)
	for _, p := range []*unstructured.Unstructured{srv, agt} {
		if p.GetNamespace() != Namespace {
			t.Errorf("%s: namespace %q", p.GetName(), p.GetNamespace())
		}
		if l := p.GetLabels(); l[LabelOperation] != opID || l[LabelManagedBy] != ManagedBy {
			t.Errorf("%s: labels %v", p.GetName(), l)
		}
		spec, _, _ := unstructured.NestedMap(p.Object, "spec")
		if spec["concurrency"] != int64(1) || spec["cordon"] != true || spec["version"] != targetV || spec["serviceAccountName"] != ServiceAccount {
			t.Errorf("%s: spec %v", p.GetName(), spec)
		}
		for _, forbidden := range []string{"channel", "drain", "secrets", "tolerations"} {
			if _, ok := spec[forbidden]; ok {
				t.Errorf("%s: spec has %s", p.GetName(), forbidden)
			}
		}
		if img, _, _ := unstructured.NestedString(p.Object, "spec", "upgrade", "image"); img != "rancher/k3s-upgrade" {
			t.Errorf("%s: upgrade image %q", p.GetName(), img)
		}
	}
	if srv.GetName() != "k3s-server" || agt.GetName() != "k3s-agent" {
		t.Fatalf("names %s, %s", srv.GetName(), agt.GetName())
	}
	exprs, _, _ := unstructured.NestedSlice(srv.Object, "spec", "nodeSelector", "matchExpressions")
	if b, _ := json.Marshal(exprs); string(b) != `[{"key":"node-role.kubernetes.io/control-plane","operator":"In","values":["true"]}]` {
		t.Errorf("server selector %s", b)
	}
	if _, ok, _ := unstructured.NestedMap(srv.Object, "spec", "prepare"); ok {
		t.Error("server Plan has a prepare step")
	}
	exprs, _, _ = unstructured.NestedSlice(agt.Object, "spec", "nodeSelector", "matchExpressions")
	if b, _ := json.Marshal(exprs); string(b) != `[{"key":"node-role.kubernetes.io/control-plane","operator":"DoesNotExist"}]` {
		t.Errorf("agent selector %s", b)
	}
	args, _, _ := unstructured.NestedStringSlice(agt.Object, "spec", "prepare", "args")
	if strings.Join(args, " ") != "prepare k3s-server" {
		t.Errorf("agent prepare args %v, want exactly [prepare k3s-server]", args)
	}
}

func TestControllerSettingsTurnRetriesOff(t *testing.T) {
	cm := ControllerConfigMap()
	if cm.Data["SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT"] != "0" {
		t.Errorf("backoff limit %q, want 0", cm.Data["SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT"])
	}
	if cm.Data["SYSTEM_UPGRADE_JOB_TTL_SECONDS_AFTER_FINISH"] != "86400" {
		t.Errorf("job TTL %q, want 86400", cm.Data["SYSTEM_UPGRADE_JOB_TTL_SECONDS_AFTER_FINISH"])
	}
	d := ControllerDeployment(1)
	if img := d.Spec.Template.Spec.Containers[0].Image; !strings.Contains(img, "@sha256:") {
		t.Errorf("controller image %q is not pinned by digest", img)
	}
	if !ours(d.Labels) {
		t.Error("controller Deployment does not carry the managed-by label")
	}
}

func TestNoStateStoreDoesNothing(t *testing.T) {
	f := newFixture(t)
	if f.reconciler().Reconcile(ctx, &desired.Doc{}) {
		t.Fatal("Reconcile asked for a retry")
	}
	if n := len(f.kube.Actions()) + len(f.dyn.Actions()); n != 0 {
		t.Fatalf("%d API calls without a stateStore block", n)
	}
}

func TestMaintenanceOffWritesNothing(t *testing.T) {
	f := newFixture(t, node("cp-1", true, fromV), ourDeployment(0, 0), ControllerConfigMap())
	r := f.reconciler()
	r.Reconcile(ctx, doc(false, f.op(desired.OperationPublished)))
	if len(f.plans()) != 0 {
		t.Fatal("Plans written while maintenance is off")
	}
	for _, res := range []string{"deployments", "configmaps", "validatingadmissionpolicies", "validatingadmissionpolicybindings"} {
		if w := writes(f.kube.Actions(), res); len(w) != 0 {
			t.Errorf("writes on %s while maintenance is off: %s", res, verbs(w))
		}
	}
	if w := writes(f.dyn.Actions(), "plans"); len(w) != 0 {
		t.Errorf("Plan writes while maintenance is off: %s", verbs(w))
	}
}

func TestInactiveOperationWritesNothing(t *testing.T) {
	for _, state := range []string{desired.OperationPending, desired.OperationDumping, desired.OperationDone, desired.OperationFailed, desired.OperationCancelled} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, node("cp-1", true, fromV), ourDeployment(0, 0), ControllerConfigMap())
			f.reconciler().Reconcile(ctx, doc(true, f.op(state)))
			if len(f.plans()) != 0 {
				t.Fatalf("Plans written for an operation in state %s", state)
			}
			if w := writes(f.kube.Actions(), "deployments"); len(w) != 0 {
				t.Fatalf("controller written for an operation in state %s: %s", state, verbs(w))
			}
		})
	}
}

func TestPublishedOperationStartsControllerThenWritesPlans(t *testing.T) {
	f := newFixture(t, node("cp-1", true, fromV), node("w-1", false, fromV), ourDeployment(0, 0), ControllerConfigMap())
	f.apiserver["cp-1"] = fromV
	r := f.reconciler()
	d := doc(true, f.op(desired.OperationPublished))

	if !r.Reconcile(ctx, d) {
		t.Fatal("first pass did not ask for a retry")
	}
	if len(f.plans()) != 0 {
		t.Fatal("Plans written before the controller is available")
	}
	if dep := f.deployment(); replicasOf(dep) != 1 {
		t.Fatalf("controller replicas = %d, want 1", replicasOf(dep))
	}
	for _, a := range writes(f.kube.Actions(), "deployments") {
		if a.GetSubresource() != "scale" {
			t.Fatalf("the agent wrote the controller Deployment itself (%s), not only its scale", a.GetVerb())
		}
	}
	if _, err := f.kube.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, PolicyName, metav1.GetOptions{}); err != nil {
		t.Fatalf("admission policy: %v", err)
	}
	if _, err := f.kube.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Get(ctx, PolicyName, metav1.GetOptions{}); err != nil {
		t.Fatalf("admission policy binding: %v", err)
	}
	if rep := f.rep.last(t); rep.Phase != PhaseRunning || !strings.Contains(rep.Error, "waiting for the system-upgrade-controller") {
		t.Fatalf("report %+v", rep)
	}

	f.markControllerAvailable()
	if !r.Reconcile(ctx, d) {
		t.Fatal("second pass did not ask for a retry")
	}
	plans := f.plans()
	if len(plans) != 2 {
		t.Fatalf("%d Plans, want 2", len(plans))
	}
	for _, p := range plans {
		want := ServerPlan(opID, targetV)
		if p.GetName() == AgentPlanName {
			want = AgentPlan(opID, targetV)
		}
		if !samePlan(&p, want) {
			t.Errorf("Plan %s differs from the template", p.GetName())
		}
	}
	rep := f.rep.last(t)
	if rep.Phase != PhaseRunning || len(rep.Servers) != 1 || len(rep.Agents) != 1 {
		t.Fatalf("report %+v", rep)
	}
	if s := rep.Servers[0]; s.Node != "cp-1" || s.KubeletVersion != fromV || s.APIServerVersion != fromV || !s.Ready {
		t.Fatalf("server report %+v", s)
	}
	if f.rep.ops[len(f.rep.ops)-1] != opID {
		t.Fatalf("report for operation %q", f.rep.ops[len(f.rep.ops)-1])
	}
}

func TestStaleDumpIsNotStarted(t *testing.T) {
	f := newFixture(t, node("cp-1", true, fromV))
	op := f.op(desired.OperationPublished)
	op.DumpTakenAt = f.now.Add(-2 * time.Hour).Format(time.RFC3339)
	f.reconciler().Reconcile(ctx, doc(true, op))
	if len(f.plans()) != 0 {
		t.Fatal("Plans written for an operation with a stale dump")
	}
	if w := writes(f.kube.Actions(), "deployments"); len(w) != 0 {
		t.Fatalf("controller written for a stale dump: %s", verbs(w))
	}
	rep := f.rep.last(t)
	if rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "old, more than 1h0m0s") {
		t.Fatalf("report %+v", rep)
	}
	cm, err := f.kube.CoreV1().ConfigMaps(Namespace).Get(ctx, LatchConfigMap, metav1.GetOptions{})
	if err != nil || cm.Data[latchKeyOp] != opID {
		t.Fatalf("failure latch = %v, %v", cm, err)
	}
}

func TestRestartContinuesRunningOperation(t *testing.T) {
	f := newFixture(t,
		node("cp-1", true, fromV),
		ourDeployment(1, 1), ControllerConfigMap(),
		ServerPlan(opID, targetV), AgentPlan(opID, targetV),
		job("apply-k3s-server-on-cp-1-with-hash-1", "cp-1", ""),
		jobPod("apply-pod", "apply-k3s-server-on-cp-1-with-hash-1", "cp-1", corev1.PodRunning),
	)
	op := f.op(desired.OperationRunning)
	// The dump is old, but the operation runs already: the age check is
	// for the start only.
	op.DumpTakenAt = f.now.Add(-3 * time.Hour).Format(time.RFC3339)

	if !f.reconciler().Reconcile(ctx, doc(true, op)) {
		t.Fatal("a running operation must ask for a retry")
	}
	if w := writes(f.dyn.Actions(), "plans"); len(w) != 0 {
		t.Fatalf("Plan writes after a restart: %s", verbs(w))
	}
	if w := writes(f.kube.Actions(), "jobs"); len(w) != 0 {
		t.Fatalf("job writes after a restart: %s", verbs(w))
	}
	if rep := f.rep.last(t); rep.Phase != PhaseRunning {
		t.Fatalf("report %+v", rep)
	}
}

func TestRunningOperationWithoutPlansWritesThem(t *testing.T) {
	// The agent restarted between the controller install and the Plans.
	f := newFixture(t, node("cp-1", true, fromV), ourDeployment(1, 1), ControllerConfigMap())
	op := f.op(desired.OperationRunning)
	op.DumpTakenAt = ""
	f.reconciler().Reconcile(ctx, doc(true, op))
	if len(f.plans()) != 2 {
		t.Fatalf("%d Plans, want 2", len(f.plans()))
	}
}

func TestFailedOperationIsNeverRetried(t *testing.T) {
	jobName := "apply-k3s-server-on-cp-1-with-hash-1"
	f := newFixture(t,
		node("cp-1", true, fromV),
		ourDeployment(1, 1), ControllerConfigMap(),
		ServerPlan(opID, targetV), AgentPlan(opID, targetV),
		job(jobName, "cp-1", batchv1.JobFailed),
		jobPod("apply-pod", jobName, "cp-1", corev1.PodFailed),
	)
	d := doc(true, f.op(desired.OperationRunning))
	r := f.reconciler()

	// Pass 1: the failure is recorded and the controller stops first.
	r.Reconcile(ctx, d)
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "upgrade job "+jobName+" failed on node cp-1") {
		t.Fatalf("report %+v", rep)
	}
	if cm, err := f.kube.CoreV1().ConfigMaps(Namespace).Get(ctx, LatchConfigMap, metav1.GetOptions{}); err != nil || cm.Data[latchKeyOp] != opID {
		t.Fatalf("failure latch = %v, %v", cm, err)
	}
	if replicasOf(f.deployment()) != 0 {
		t.Fatal("the controller was not stopped first")
	}
	if len(f.plans()) != 2 {
		t.Fatal("Plans deleted while the controller may still run")
	}

	// Pass 2: the controller is down; Plans and jobs go.
	r.Reconcile(ctx, d)
	if len(f.plans()) != 0 {
		t.Fatalf("%d Plans left after the failure", len(f.plans()))
	}
	if _, err := f.kube.BatchV1().Jobs(Namespace).Get(ctx, jobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed job still there: %v", err)
	}

	// Restart: a new agent sees the same document. It never writes the
	// Plans again and never starts the controller.
	f.kube.ClearActions()
	f.dyn.ClearActions()
	r2 := f.reconciler()
	for range 3 {
		r2.Reconcile(ctx, d)
	}
	if w := writes(f.dyn.Actions(), "plans"); len(w) != 0 {
		t.Fatalf("Plan writes after a restart: %s", verbs(w))
	}
	if w := writes(f.kube.Actions(), "deployments"); len(w) != 0 {
		t.Fatalf("controller writes after a restart: %s", verbs(w))
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed {
		t.Fatalf("report after restart %+v", rep)
	}
}

func TestOptOutRemovesEverything(t *testing.T) {
	jobName := "apply-k3s-server-on-cp-1-with-hash-1"
	f := newFixture(t,
		node("cp-1", true, fromV),
		ourDeployment(1, 1), ControllerConfigMap(),
		controllerPod(),
		AdmissionPolicy(), AdmissionPolicyBinding(),
		ServerPlan(opID, targetV), AgentPlan(opID, targetV),
		job(jobName, "cp-1", ""),
	)
	d := doc(false, f.op(desired.OperationRunning))
	r := f.reconciler()

	// Pass 1: scale the controller to zero, delete nothing yet.
	if !r.Reconcile(ctx, d) {
		t.Fatal("cleanup in progress must ask for a retry")
	}
	if replicasOf(f.deployment()) != 0 {
		t.Fatal("controller not scaled to zero")
	}
	if w := writes(f.dyn.Actions(), "plans"); len(w) != 0 {
		t.Fatalf("Plans deleted before the controller stopped: %s", verbs(w))
	}
	// Pass 2: a controller pod still terminates: wait.
	r.Reconcile(ctx, d)
	if w := writes(f.kube.Actions(), "jobs"); len(w) != 0 {
		t.Fatalf("jobs deleted while a controller pod exists: %s", verbs(w))
	}
	// Pass 3: the controller is gone: delete explicitly, then uninstall.
	f.deletePod("system-upgrade-controller-abc")
	if r.Reconcile(ctx, d) {
		t.Fatal("finished cleanup asked for a retry")
	}
	jobDeletes := writes(f.kube.Actions(), "jobs")
	if len(jobDeletes) != 1 || deletePolicy(t, jobDeletes[0]) != metav1.DeletePropagationBackground {
		t.Fatalf("job deletes: %s", verbs(jobDeletes))
	}
	planDeletes := writes(f.dyn.Actions(), "plans")
	if len(planDeletes) != 2 {
		t.Fatalf("Plan deletes: %s", verbs(planDeletes))
	}
	for _, a := range planDeletes {
		if deletePolicy(t, a) != metav1.DeletePropagationOrphan {
			t.Fatalf("Plan delete with policy %q", deletePolicy(t, a))
		}
	}
	if d := f.deployment(); d == nil || replicasOf(d) != 0 {
		t.Fatal("the overlay's controller must stay, at zero replicas")
	}
	if _, err := f.kube.AdmissionregistrationV1().ValidatingAdmissionPolicies().Get(ctx, PolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("admission policy not removed: %v", err)
	}
	if _, err := f.kube.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Get(ctx, PolicyName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("admission policy binding not removed: %v", err)
	}
	if rep := f.rep.last(t); rep.Phase != PhaseCancelled {
		t.Fatalf("report %+v", rep)
	}
}

func TestOptOutKeepsForeignObjects(t *testing.T) {
	foreign := ServerPlan("x", targetV)
	foreign.SetLabels(map[string]string{"team": "ops"})
	foreign.SetName("ops-plan")
	f := newFixture(t, node("cp-1", true, fromV), foreign)
	f.reconciler().Reconcile(ctx, doc(false, nil))
	if len(f.plans()) != 1 {
		t.Fatal("a Plan that the agent did not write was deleted")
	}
}

func TestCleanupOrphansAnInflightJob(t *testing.T) {
	jobName := "apply-k3s-server-on-cp-1-with-hash-1"
	f := newFixture(t,
		node("cp-1", true, fromV),
		ourDeployment(0, 0),
		ServerPlan(opID, targetV),
		job(jobName, "cp-1", ""),
		jobPod("apply-pod", jobName, "cp-1", corev1.PodRunning),
	)
	f.reconciler().Reconcile(ctx, doc(false, f.op(desired.OperationRunning)))
	jobDeletes := writes(f.kube.Actions(), "jobs")
	if len(jobDeletes) != 1 || deletePolicy(t, jobDeletes[0]) != metav1.DeletePropagationOrphan {
		t.Fatalf("an in-flight job must be deleted with the Orphan policy: %s", verbs(jobDeletes))
	}
	if rep := f.rep.last(t); !strings.Contains(rep.Error, "already ran on node cp-1") {
		t.Fatalf("report does not name the in-flight node: %+v", rep)
	}
}

func TestFenceStopsAndFails(t *testing.T) {
	f := newFixture(t, node("cp-1", true, fromV), ourDeployment(0, 0), ServerPlan(opID, targetV), AgentPlan(opID, targetV))
	d := doc(true, f.op(desired.OperationRunning))
	d.StateStore.Fenced = true
	f.reconciler().Reconcile(ctx, d)
	if len(f.plans()) != 0 {
		t.Fatal("Plans kept while the cluster is fenced")
	}
	if f.deployment() == nil {
		t.Fatal("a fence must not uninstall the controller (maintenance stays on)")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "fenced") {
		t.Fatalf("report %+v", rep)
	}
}

func doneNode(name string, server bool) *corev1.Node {
	n := node(name, server, targetV)
	n.Labels[planLabelPrefix+ServerPlanName] = "hash-1"
	return n
}

func TestDoneCleansUpAndReports(t *testing.T) {
	f := newFixture(t,
		doneNode("cp-1", true),
		ourDeployment(1, 1), ControllerConfigMap(),
		ServerPlan(opID, targetV), AgentPlan(opID, targetV),
		job("apply-k3s-server-on-cp-1-with-hash-1", "cp-1", batchv1.JobComplete),
	)
	f.apiserver["cp-1"] = targetV
	d := doc(true, f.op(desired.OperationRunning))
	r := f.reconciler()
	r.Reconcile(ctx, d) // scales the controller to zero
	if r.Reconcile(ctx, d) {
		t.Fatal("a done operation asked for a retry")
	}
	if len(f.plans()) != 0 {
		t.Fatal("Plans kept after the operation is done")
	}
	if replicasOf(f.deployment()) != 0 {
		t.Fatal("controller still runs after the operation is done")
	}
	rep := f.rep.last(t)
	if rep.Phase != PhaseDone || rep.Servers[0].APIServerVersion != targetV {
		t.Fatalf("report %+v", rep)
	}

	// A node that flaps after the end does not bring the Plans back.
	n, _ := f.kube.CoreV1().Nodes().Get(ctx, "cp-1", metav1.GetOptions{})
	n.Status.Conditions[0].Status = corev1.ConditionFalse
	if _, err := f.kube.CoreV1().Nodes().UpdateStatus(ctx, n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.Reconcile(ctx, d)
	if len(f.plans()) != 0 {
		t.Fatal("Plans written again after the operation was done")
	}
}

func TestNotDoneUntilTheApiserverServesTheTarget(t *testing.T) {
	f := newFixture(t, doneNode("cp-1", true), ourDeployment(1, 1), ControllerConfigMap(), ServerPlan(opID, targetV), AgentPlan(opID, targetV))
	f.apiserver["cp-1"] = fromV // kubelet new, apiserver still old
	if !f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationRunning))) {
		t.Fatal("not done yet, but no retry")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseRunning {
		t.Fatalf("report %+v", rep)
	}
}

func TestNotDoneWhileTheNodeIsCordoned(t *testing.T) {
	n := doneNode("cp-1", true)
	n.Spec.Unschedulable = true
	f := newFixture(t, n, ourDeployment(1, 1), ControllerConfigMap(), ServerPlan(opID, targetV), AgentPlan(opID, targetV),
		job("apply-k3s-server-on-cp-1-with-hash-1", "cp-1", batchv1.JobComplete))
	f.apiserver["cp-1"] = targetV
	f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationRunning)))
	if rep := f.rep.last(t); rep.Phase != PhaseRunning {
		t.Fatalf("done before the controller uncordoned the node: %+v", rep)
	}
}

func TestStepRefused(t *testing.T) {
	f := newFixture(t, node("cp-1", true, "v1.32.9+k3s1"))
	f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	if len(f.plans()) != 0 {
		t.Fatal("Plans written for a two-minor step")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "more than one minor step") {
		t.Fatalf("report %+v", rep)
	}
}

func TestMissingOverlayIsRefused(t *testing.T) {
	f := newFixture(t, node("cp-1", true, fromV))
	f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	if len(f.plans()) != 0 {
		t.Fatal("Plans written without the controller")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "apply install-maintenance.yaml") {
		t.Fatalf("report %+v", rep)
	}
}

func TestControllerWithRetriesIsRefused(t *testing.T) {
	cm := ControllerConfigMap()
	cm.Data["SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT"] = "99"
	f := newFixture(t, node("cp-1", true, fromV), ourDeployment(0, 0), cm)
	f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	if replicasOf(f.deployment()) != 0 {
		t.Fatal("controller started with job retries on")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT") {
		t.Fatalf("report %+v", rep)
	}
}

func TestForeignControllerIsRefused(t *testing.T) {
	foreign := ourDeployment(1, 1)
	foreign.Labels = map[string]string{"app": "suc"}
	f := newFixture(t, node("cp-1", true, fromV), foreign)
	f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	if len(f.plans()) != 0 {
		t.Fatal("Plans written next to a foreign controller")
	}
	if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "did not install") {
		t.Fatalf("report %+v", rep)
	}
}

func TestNoMaintenanceRBAC(t *testing.T) {
	forbid := func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "upgrade.cattle.io", Resource: "plans"}, "", errors.New("RBAC"))
	}
	t.Run("active operation fails with the next step", func(t *testing.T) {
		f := newFixture(t, node("cp-1", true, fromV))
		f.dyn.PrependReactor("list", "plans", forbid)
		f.reconciler().Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
		if rep := f.rep.last(t); rep.Phase != PhaseFailed || !strings.Contains(rep.Error, "install-maintenance.yaml") {
			t.Fatalf("report %+v", rep)
		}
	})
	t.Run("maintenance off stays quiet", func(t *testing.T) {
		f := newFixture(t, node("cp-1", true, fromV))
		f.dyn.PrependReactor("list", "plans", forbid)
		if f.reconciler().Reconcile(ctx, doc(false, nil)) {
			t.Fatal("asked for a retry without RBAC")
		}
		if len(f.rep.reports) != 0 {
			t.Fatal("report sent without an operation")
		}
	})
}

func TestStaleOperationObjectsGoFirst(t *testing.T) {
	old := ServerPlan("op-0", fromV)
	f := newFixture(t, node("cp-1", true, fromV), ourDeployment(0, 0), ControllerConfigMap(), old)
	r := f.reconciler()
	r.Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	if len(f.plans()) != 0 {
		t.Fatal("the Plan of the older operation is still there")
	}
	f.markControllerAvailable()
	r.Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	r.Reconcile(ctx, doc(true, f.op(desired.OperationPublished)))
	for _, p := range f.plans() {
		if p.GetLabels()[LabelOperation] != opID {
			t.Fatalf("Plan %s of operation %s", p.GetName(), p.GetLabels()[LabelOperation])
		}
	}
}

func TestReportDedupe(t *testing.T) {
	f := newFixture(t)
	r := f.reconciler()
	rep := Report{Phase: PhaseRunning, Servers: []NodeReport{}, Agents: []NodeReport{}}
	r.send(ctx, opID, rep)
	r.send(ctx, opID, rep)
	if len(f.rep.reports) != 1 {
		t.Fatalf("%d reports, want 1 (same report within the refresh time)", len(f.rep.reports))
	}
	f.now = f.now.Add(reportRefresh + time.Second)
	r.send(ctx, opID, rep)
	if len(f.rep.reports) != 2 {
		t.Fatalf("%d reports, want 2 (refresh)", len(f.rep.reports))
	}
	f.rep.err = errors.New("down")
	if r.send(ctx, opID, Report{Phase: PhaseDone}) {
		t.Fatal("send reported success on an error")
	}
}
