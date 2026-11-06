// Package k3supgrade is the k3s upgrade executor of a self-hosted cluster
// with managed state (design "self-hosted with managed state", section
// 4.3). It upgrades the cluster's own k3s through the
// system-upgrade-controller, PULL-ONLY: the platform publishes an upgrade
// operation in the desired document, and this executor acts on it in the
// cluster when the customer switched maintenance on.
//
// Rules (each one is tested):
//
//   - It writes the two Plans (plans.go) only when maintenance is on, the
//     cluster is not fenced, and the operation is published or running.
//   - It writes the admission policy (policy.go) only when maintenance is
//     on, and removes it when maintenance goes off. The controller comes
//     from the maintenance overlay with zero replicas; the agent scales it
//     to one only while an operation is active, and only after it checked
//     that the controller settings turn job retries off.
//   - It does not start an operation whose dump is older than one hour.
//   - It reconciles from the operation state, the Plans, the jobs and the
//     node versions on every pass, so a restart continues a running
//     operation.
//   - A failed operation stays failed. The executor records the failure in
//     a ConfigMap before it removes anything, and never writes a Plan for
//     that operation again, also after a restart.
//   - On a failure, an opt-out or a fence it stops the controller, then
//     deletes the Plans and their jobs EXPLICITLY (no owner references). A
//     job whose node is already cordoned keeps its pod: the pod finishes or
//     fails on that node, and the report says which.
//   - It reports each server's kubelet version, the version that the
//     apiserver ON THAT NODE serves, and readiness.
//   - While its Plans exist, the host renewer (kubehz-agent state-renew)
//     does not restart k3s for a routine certificate renewal: the Plans are
//     the pause signal on the host.
package k3supgrade

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/kernpilot/kubehz-agent/internal/desired"
)

const (
	// LatchConfigMap records a failed operation in the cluster, so that a
	// restarted agent never writes its Plans again.
	LatchConfigMap = "kubehz-k3s-upgrade"
	latchKeyOp     = "failedOperation"
	latchKeyReason = "reason"

	// maxDumpAge: the agent does not start an operation whose dump is
	// older than this (design 4.4).
	maxDumpAge = time.Hour
	// reportRefresh re-sends an unchanged report this often, so that the
	// platform sees the agent is alive during a long operation.
	reportRefresh = 5 * time.Minute
	// callTimeout bounds each apiserver call.
	callTimeout = 15 * time.Second

	// Labels the controller sets on its jobs (upstream constants).
	jobLabelPlan     = "upgrade.cattle.io/plan"
	jobLabelNode     = "upgrade.cattle.io/node"
	planLabelPrefix  = "plan.upgrade.cattle.io/"
	jobNameLabel     = "batch.kubernetes.io/job-name"
	jobNameLabelOld  = "job-name"
	planCondComplete = "Complete"
)

// Options configures New.
type Options struct {
	Kube     kubernetes.Interface
	Dyn      dynamic.Interface
	Reporter Reporter
	// APIServerVersion asks one server node for its apiserver version.
	// nil: the version stays unknown and no operation can finish.
	APIServerVersion APIServerVersionFunc
	Now              func() time.Time
	Logger           *slog.Logger
}

// Reconciler implements desired.Actor for the stateStore block.
type Reconciler struct {
	kube     kubernetes.Interface
	dyn      dynamic.Interface
	reporter Reporter
	probe    APIServerVersionFunc
	now      func() time.Time
	log      *slog.Logger

	// sent is the last report delivered per operation (dedupe).
	sent map[string]sentReport
	// failed is the in-memory copy of the failure latch.
	failed map[string]string
	// done holds the operations this executor finished: a node that flaps
	// after the end never brings their Plans back.
	done map[string]bool
}

type sentReport struct {
	hash string
	at   time.Time
}

// New builds a Reconciler.
func New(opts Options) *Reconciler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Reconciler{
		kube:     opts.Kube,
		dyn:      opts.Dyn,
		reporter: opts.Reporter,
		probe:    opts.APIServerVersion,
		now:      opts.Now,
		log:      opts.Logger,
		sent:     make(map[string]sentReport),
		failed:   make(map[string]string),
		done:     make(map[string]bool),
	}
}

// stopReason says why the executor stops.
type stopReason int

const (
	stopOptOut   stopReason = iota // maintenance off, or managed state off
	stopFenced                     // the cluster is fenced for a rollback
	stopInactive                   // the operation is not published or running
)

// Reconcile implements desired.Actor. It returns true while it has work to
// observe (an active operation, a cleanup in progress, or a report that did
// not reach the platform).
func (r *Reconciler) Reconcile(ctx context.Context, doc *desired.Doc) bool {
	if doc == nil || doc.StateStore == nil {
		// Not a managed-state cluster: this executor has nothing to do.
		return false
	}
	ss := doc.StateStore
	switch {
	case !ss.Enabled || !ss.Maintenance:
		return r.stop(ctx, ss.Operation, stopOptOut)
	case ss.Fenced:
		return r.stop(ctx, ss.Operation, stopFenced)
	case !ss.Operation.Active():
		return r.stop(ctx, ss.Operation, stopInactive)
	default:
		return r.run(ctx, ss.Operation)
	}
}

// refusal is a persistent reason to fail the operation (a retry cannot
// help: a foreign controller, missing RBAC, a policy denial, a bad step).
type refusal struct{ msg string }

func (e *refusal) Error() string { return e.msg }

func refuse(format string, a ...any) error { return &refusal{msg: fmt.Sprintf(format, a...)} }

func isRefusal(err error) bool {
	var r *refusal
	return errors.As(err, &r)
}

// observed is one read of the objects this executor works with.
type observed struct {
	plans          []unstructured.Unstructured // every Plan in Namespace
	planCRDMissing bool                        // the Plan resource is not served yet
	jobs           []batchv1.Job               // jobs that carry LabelOperation
	pods           []corev1.Pod                // pods that carry LabelOperation
	controller     *appsv1.Deployment          // nil when absent
	controllerPods int                         // controller pods, terminating ones too
	latch          map[string]string           // nil when absent
}

func (r *Reconciler) observe(ctx context.Context) (*observed, error) {
	st := &observed{}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	list, err := r.dyn.Resource(PlanGVR).Namespace(Namespace).List(cctx, metav1.ListOptions{})
	switch {
	case apierrors.IsNotFound(err):
		st.planCRDMissing = true
	case err != nil:
		return nil, fmt.Errorf("list Plans in %s: %w", Namespace, err)
	default:
		st.plans = list.Items
	}

	withOp := metav1.ListOptions{LabelSelector: LabelOperation}
	jobs, err := r.kube.BatchV1().Jobs(Namespace).List(cctx, withOp)
	if err != nil {
		return nil, fmt.Errorf("list upgrade jobs in %s: %w", Namespace, err)
	}
	st.jobs = jobs.Items
	pods, err := r.kube.CoreV1().Pods(Namespace).List(cctx, withOp)
	if err != nil {
		return nil, fmt.Errorf("list upgrade pods in %s: %w", Namespace, err)
	}
	st.pods = pods.Items

	dep, err := r.kube.AppsV1().Deployments(Namespace).Get(cctx, ControllerName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return nil, fmt.Errorf("get Deployment %s/%s: %w", Namespace, ControllerName, err)
	default:
		st.controller = dep
	}
	cpods, err := r.kube.CoreV1().Pods(Namespace).List(cctx, metav1.ListOptions{LabelSelector: controllerPodLabel + "=" + ControllerName})
	if err != nil {
		return nil, fmt.Errorf("list controller pods in %s: %w", Namespace, err)
	}
	st.controllerPods = len(cpods.Items)

	cm, err := r.kube.CoreV1().ConfigMaps(Namespace).Get(cctx, LatchConfigMap, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return nil, fmt.Errorf("get ConfigMap %s/%s: %w", Namespace, LatchConfigMap, err)
	default:
		st.latch = cm.Data
	}
	return st, nil
}

func ours(labels map[string]string) bool { return labels[LabelManagedBy] == ManagedBy }

// run drives an active operation.
func (r *Reconciler) run(ctx context.Context, op *desired.Operation) bool {
	st, err := r.observe(ctx)
	if err != nil {
		if apierrors.IsForbidden(err) {
			// Without the maintenance RBAC nothing can be read or written.
			// A retry does not help: fail the operation and say what to do.
			return r.fail(ctx, nil, op, nil, "the agent may not read the upgrade objects ("+errDetail(err)+"); apply the maintenance RBAC (install-maintenance.yaml of the agent release), then start a new upgrade")
		}
		r.log.Warn("k3s upgrade: cannot read the cluster; retrying next poll", "operation", op.ID, "error", err.Error())
		return true
	}

	if reason, ok := r.latched(st, op.ID); ok {
		// This agent failed the operation before (maybe before a restart).
		// Never write its Plans again: clean up and repeat the report.
		return r.fail(ctx, st, op, nil, reason)
	}
	if r.done[op.ID] {
		var nodes []nodeState
		if nl, err := r.listNodes(ctx); err == nil {
			nodes = observeNodes(ctx, nl, r.probe)
		}
		return !r.send(ctx, op.ID, buildReport(PhaseDone, nodes, ""))
	}

	// Objects of an older operation go first.
	if stale := func(l map[string]string) bool { return l[LabelOperation] != op.ID }; r.hasOurs(st, stale) {
		pending, _, err := r.cleanup(ctx, st, stale)
		if err != nil {
			r.log.Warn("k3s upgrade: cannot remove the objects of an older operation", "operation", op.ID, "error", err.Error())
		} else if !pending {
			r.log.Info("k3s upgrade: removed the objects of an older operation", "operation", op.ID)
		}
		return true
	}

	nodeList, err := r.listNodes(ctx)
	if err != nil {
		r.log.Warn("k3s upgrade: cannot list nodes; retrying next poll", "operation", op.ID, "error", err.Error())
		return true
	}
	nodes := observeNodes(ctx, nodeList, r.probe)
	target := op.TargetVersion

	if err := checkStep(target, nodes); err != nil {
		return r.fail(ctx, st, op, nodes, err.Error())
	}
	if msg := failedJob(st, op.ID); msg != "" {
		return r.fail(ctx, st, op, nodes, msg)
	}

	isOp := func(l map[string]string) bool { return l[LabelOperation] == op.ID }
	if allAtTarget(nodes, target) && jobsSettled(st, op.ID, nodeList) {
		pending, _, err := r.cleanup(ctx, st, isOp)
		if err != nil || pending {
			return true
		}
		if err := r.scaleController(ctx, st, 0); err != nil {
			r.log.Warn("k3s upgrade: cannot stop the controller after the operation", "operation", op.ID, "error", err.Error())
			return true
		}
		r.done[op.ID] = true
		r.log.Info("k3s upgrade: operation done", "operation", op.ID, "version", target)
		return !r.send(ctx, op.ID, buildReport(PhaseDone, nodes, ""))
	}

	if !r.hasOurs(st, isOp) && op.State == desired.OperationPublished {
		if err := r.checkDump(op); err != nil {
			return r.fail(ctx, st, op, nodes, err.Error())
		}
	}

	ready, err := r.ensureController(ctx, st)
	if err != nil {
		if isRefusal(err) {
			return r.fail(ctx, st, op, nodes, err.Error())
		}
		r.log.Warn("k3s upgrade: cannot install the controller; retrying next poll", "operation", op.ID, "error", err.Error())
		return true
	}
	if !ready {
		r.send(ctx, op.ID, buildReport(PhaseRunning, nodes, joinNotes("waiting for the system-upgrade-controller to start", probeNotes(nodes))))
		return true
	}
	if err := r.ensurePlans(ctx, st, op); err != nil {
		if isRefusal(err) {
			return r.fail(ctx, st, op, nodes, err.Error())
		}
		r.log.Warn("k3s upgrade: cannot write the Plans; retrying next poll", "operation", op.ID, "error", err.Error())
		return true
	}
	r.send(ctx, op.ID, buildReport(PhaseRunning, nodes, probeNotes(nodes)))
	return true
}

// fail records the failure, stops the controller, deletes the operation's
// Plans and jobs, and reports. st and nodes may be nil (unreadable
// cluster). It returns true: the executor keeps reporting until the
// platform marks the operation failed.
func (r *Reconciler) fail(ctx context.Context, st *observed, op *desired.Operation, nodes []nodeState, msg string) bool {
	if _, known := r.failed[op.ID]; !known {
		r.log.Error("k3s upgrade: operation failed; the agent stops it and never retries it", "operation", op.ID, "reason", msg)
	}
	r.failed[op.ID] = msg
	if st != nil && st.latch[latchKeyOp] != op.ID {
		if err := r.writeLatch(ctx, op.ID, msg); err != nil {
			r.log.Warn("k3s upgrade: cannot record the failure in the cluster", "operation", op.ID, "error", err.Error())
		}
	}
	text := msg
	if st != nil {
		pending, inflight, err := r.cleanup(ctx, st, func(l map[string]string) bool { return l[LabelOperation] == op.ID })
		switch {
		case err != nil:
			r.log.Warn("k3s upgrade: cannot remove the failed operation's Plans and jobs", "operation", op.ID, "error", err.Error())
		case pending:
			text = joinNotes(text, "the agent is stopping the controller to remove the Plans and jobs")
		}
		text = joinNotes(text, inflightNote(inflight), orphanNote(st, op.ID))
	}
	if nodes == nil {
		if nl, err := r.listNodes(ctx); err == nil {
			nodes = observeNodes(ctx, nl, r.probe)
		}
	}
	r.send(ctx, op.ID, buildReport(PhaseFailed, nodes, text))
	return true
}

// stop removes what this executor started, for an opt-out, a fence or an
// operation that is no longer active.
func (r *Reconciler) stop(ctx context.Context, op *desired.Operation, reason stopReason) bool {
	st, err := r.observe(ctx)
	if err != nil {
		if apierrors.IsForbidden(err) {
			// No maintenance RBAC: this agent cannot have written anything
			// here, or it lost the right to remove it. Nothing to do.
			r.log.Debug("k3s upgrade: no access to the upgrade objects; nothing to stop", "error", err.Error())
			return false
		}
		r.log.Warn("k3s upgrade: cannot read the cluster to stop; retrying next poll", "error", err.Error())
		return true
	}
	all := func(map[string]string) bool { return true }
	pending, inflight, err := r.cleanup(ctx, st, all)
	if err != nil {
		r.log.Warn("k3s upgrade: cannot remove the Plans and jobs", "error", err.Error())
		return true
	}
	if pending {
		return true
	}
	if err := r.scaleController(ctx, st, 0); err != nil {
		r.log.Warn("k3s upgrade: cannot stop the controller", "error", err.Error())
		return true
	}
	retry := false
	if reason == stopOptOut {
		if err := r.removePolicy(ctx); err != nil {
			r.log.Warn("k3s upgrade: cannot remove the admission policy", "error", err.Error())
			retry = true
		}
	}

	// Report only for an operation that this stop interrupts.
	if op.Active() && reason != stopInactive {
		phase, why := PhaseCancelled, "maintenance is off: the agent removed the upgrade Plans"
		if reason == stopFenced {
			phase, why = PhaseFailed, "the cluster is fenced for a rollback: the agent removed the upgrade Plans"
		}
		var nodes []nodeState
		if nl, err := r.listNodes(ctx); err == nil {
			nodes = observeNodes(ctx, nl, r.probe)
		}
		if !r.send(ctx, op.ID, buildReport(phase, nodes, joinNotes(why, inflightNote(inflight), orphanNote(st, op.ID)))) {
			retry = true
		}
	}
	// Keep observing while an orphaned upgrade pod still runs, so that its
	// end reaches the report.
	if op != nil && hasRunningOrphan(st, op.ID) {
		retry = true
	}
	return retry
}

// hasOurs reports whether a Plan or job of this executor matches sel.
func (r *Reconciler) hasOurs(st *observed, sel func(map[string]string) bool) bool {
	for i := range st.plans {
		if l := st.plans[i].GetLabels(); ours(l) && sel(l) {
			return true
		}
	}
	for i := range st.jobs {
		if l := st.jobs[i].Labels; ours(l) && sel(l) {
			return true
		}
	}
	return false
}

// cleanup stops the controller, then deletes the selected Plans and jobs.
// It returns pending=true while the controller still runs (the next pass
// continues). A job whose pod already passed its cordon step is deleted
// with the Orphan policy: its pod finishes or fails on that node, and
// inflight names those nodes.
func (r *Reconciler) cleanup(ctx context.Context, st *observed, sel func(map[string]string) bool) (pending bool, inflight []string, err error) {
	var plans []*unstructured.Unstructured
	for i := range st.plans {
		if l := st.plans[i].GetLabels(); ours(l) && sel(l) {
			plans = append(plans, &st.plans[i])
		}
	}
	var jobs []*batchv1.Job
	for i := range st.jobs {
		if l := st.jobs[i].Labels; ours(l) && sel(l) {
			jobs = append(jobs, &st.jobs[i])
		}
	}
	if len(plans) == 0 && len(jobs) == 0 {
		return false, nil, nil
	}

	// 1. Stop the controller. Without it nothing re-creates a job, and
	// nothing deletes a job (and its pod) whose Plan is gone.
	if st.controller != nil && ours(st.controller.Labels) {
		if replicasOf(st.controller) > 0 {
			if err := r.scaleController(ctx, st, 0); err != nil {
				return true, nil, err
			}
			r.log.Info("k3s upgrade: stopping the controller before the cleanup")
			return true, nil, nil
		}
		if st.controllerPods > 0 {
			return true, nil, nil
		}
	}

	// 2. Jobs, explicitly.
	for _, j := range jobs {
		policy := metav1.DeletePropagationBackground
		if jobInflight(j, st.pods) {
			policy = metav1.DeletePropagationOrphan
			inflight = append(inflight, j.Labels[jobLabelNode])
		}
		dctx, cancel := context.WithTimeout(ctx, callTimeout)
		err := r.kube.BatchV1().Jobs(Namespace).Delete(dctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
		cancel()
		if err != nil && !apierrors.IsNotFound(err) {
			return false, inflight, fmt.Errorf("delete job %s/%s: %w", Namespace, j.Name, err)
		}
		r.log.Info("k3s upgrade: deleted upgrade job", "job", j.Name, "node", j.Labels[jobLabelNode], "propagation", string(policy))
	}

	// 3. Plans. Their jobs are gone, so no owner reference matters.
	orphan := metav1.DeletePropagationOrphan
	for _, p := range plans {
		dctx, cancel := context.WithTimeout(ctx, callTimeout)
		err := r.dyn.Resource(PlanGVR).Namespace(Namespace).Delete(dctx, p.GetName(), metav1.DeleteOptions{PropagationPolicy: &orphan})
		cancel()
		if err != nil && !apierrors.IsNotFound(err) {
			return false, inflight, fmt.Errorf("delete Plan %s/%s: %w", Namespace, p.GetName(), err)
		}
		r.log.Info("k3s upgrade: deleted Plan", "plan", p.GetName(), "operation", p.GetLabels()[LabelOperation])
	}
	sort.Strings(inflight)
	return false, inflight, nil
}

// jobInflight reports whether a pod of the job passed its cordon step and
// is not finished: killing it could stop an upgrade half way, or leave its
// node cordoned with nothing to undo it.
func jobInflight(j *batchv1.Job, pods []corev1.Pod) bool {
	for i := range pods {
		p := &pods[i]
		if p.Labels[jobNameLabel] != j.Name && p.Labels[jobNameLabelOld] != j.Name {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if p.Status.Phase == corev1.PodRunning {
			return true
		}
		for _, cs := range p.Status.InitContainerStatuses {
			if (cs.Name == "cordon" || cs.Name == "drain") && (cs.State.Running != nil || cs.State.Terminated != nil) {
				return true
			}
		}
	}
	return false
}

// orphans are the upgrade pods of an operation whose job is gone.
func orphans(st *observed, opID string) []corev1.Pod {
	jobs := make(map[string]bool, len(st.jobs))
	for i := range st.jobs {
		jobs[st.jobs[i].Name] = true
	}
	var out []corev1.Pod
	for i := range st.pods {
		p := st.pods[i]
		if p.Labels[LabelOperation] != opID {
			continue
		}
		name := p.Labels[jobNameLabel]
		if name == "" {
			name = p.Labels[jobNameLabelOld]
		}
		if !jobs[name] {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func hasRunningOrphan(st *observed, opID string) bool {
	for _, p := range orphans(st, opID) {
		if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
			return true
		}
	}
	return false
}

func inflightNote(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	return "an upgrade job already ran on node " + strings.Join(nodes, ", ") + ": its pod finishes or fails on that node"
}

func orphanNote(st *observed, opID string) string {
	var parts []string
	for _, p := range orphans(st, opID) {
		node := p.Labels[jobLabelNode]
		switch p.Status.Phase {
		case corev1.PodSucceeded:
			parts = append(parts, fmt.Sprintf("the upgrade pod on node %s succeeded; the node stays cordoned until you run kubectl uncordon %s", node, node))
		case corev1.PodFailed:
			parts = append(parts, fmt.Sprintf("the upgrade pod on node %s failed; the node stays cordoned until you run kubectl uncordon %s", node, node))
		default:
			parts = append(parts, fmt.Sprintf("the upgrade pod on node %s still runs", node))
		}
	}
	return strings.Join(parts, "; ")
}

// failedJob returns why the operation failed, from a failed job or from
// the controller's JobFailed condition on a Plan. "" when nothing failed.
func failedJob(st *observed, opID string) string {
	for i := range st.jobs {
		j := &st.jobs[i]
		if j.Labels[LabelOperation] != opID {
			continue
		}
		for _, c := range j.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				return fmt.Sprintf("upgrade job %s failed on node %s: %s: %s", j.Name, j.Labels[jobLabelNode], c.Reason, c.Message)
			}
		}
	}
	for i := range st.plans {
		p := &st.plans[i]
		if l := p.GetLabels(); !ours(l) || l[LabelOperation] != opID {
			continue
		}
		conds, _, _ := unstructured.NestedSlice(p.Object, "status", "conditions")
		for _, c := range conds {
			m, ok := c.(map[string]any)
			if !ok {
				continue
			}
			if m["type"] == planCondComplete && m["status"] == "False" && m["reason"] == "JobFailed" {
				msg, _ := m["message"].(string)
				return fmt.Sprintf("Plan %s: %s", p.GetName(), msg)
			}
		}
	}
	return ""
}

// jobsSettled reports whether every job of the operation completed AND the
// controller marked its node done (the plan label it writes in the same
// update that uncordons the node).
func jobsSettled(st *observed, opID string, nodes []corev1.Node) bool {
	byName := make(map[string]*corev1.Node, len(nodes))
	for i := range nodes {
		byName[nodes[i].Name] = &nodes[i]
	}
	for i := range st.jobs {
		j := &st.jobs[i]
		if j.Labels[LabelOperation] != opID {
			continue
		}
		complete := false
		for _, c := range j.Status.Conditions {
			if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
				complete = true
			}
		}
		if !complete {
			return false
		}
		plan := j.Labels[jobLabelPlan]
		n := byName[j.Labels[jobLabelNode]]
		if n == nil {
			continue // the node is gone: nothing left to uncordon
		}
		if hash := j.Labels[planLabelPrefix+plan]; hash == "" || n.Labels[planLabelPrefix+plan] != hash || n.Spec.Unschedulable {
			return false
		}
	}
	return true
}

func (r *Reconciler) checkDump(op *desired.Operation) error {
	if op.DumpTakenAt == "" {
		return refuse("operation %s has no dump time; the agent does not start an upgrade without a recent dump; start a new upgrade", op.ID)
	}
	t, err := time.Parse(time.RFC3339, op.DumpTakenAt)
	if err != nil {
		return refuse("operation %s has an invalid dump time %q; start a new upgrade", op.ID, op.DumpTakenAt)
	}
	if age := r.now().Sub(t); age > maxDumpAge {
		return refuse("the dump of operation %s is %s old, more than %s; the agent does not start it; start a new upgrade", op.ID, age.Round(time.Second), maxDumpAge)
	}
	return nil
}

func (r *Reconciler) latched(st *observed, opID string) (string, bool) {
	if reason, ok := r.failed[opID]; ok {
		return reason, true
	}
	if st.latch[latchKeyOp] == opID {
		reason := st.latch[latchKeyReason]
		if reason == "" {
			reason = "the agent failed this operation before it restarted"
		}
		r.failed[opID] = reason
		return reason, true
	}
	return "", false
}

func (r *Reconciler) writeLatch(ctx context.Context, opID, reason string) error {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	data := map[string]string{
		latchKeyOp:     opID,
		latchKeyReason: truncate(reason, 1024),
		"failedAt":     r.now().UTC().Format(time.RFC3339),
	}
	cms := r.kube.CoreV1().ConfigMaps(Namespace)
	cm, err := cms.Get(cctx, LatchConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(cctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: LatchConfigMap, Namespace: Namespace, Labels: managedLabels()},
			Data:       data,
		}, metav1.CreateOptions{FieldManager: ManagedBy})
		return err
	}
	if err != nil {
		return err
	}
	cm.Data = data
	_, err = cms.Update(cctx, cm, metav1.UpdateOptions{FieldManager: ManagedBy})
	return err
}

func (r *Reconciler) listNodes(ctx context.Context) ([]corev1.Node, error) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	list, err := r.kube.CoreV1().Nodes().List(cctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// send delivers a report unless the same report reached the platform less
// than reportRefresh ago. It returns true when the platform has it.
func (r *Reconciler) send(ctx context.Context, opID string, rep Report) bool {
	if r.reporter == nil {
		return true
	}
	b, _ := json.Marshal(rep)
	sum := sha256.Sum256(b)
	h := hex.EncodeToString(sum[:])
	if last, ok := r.sent[opID]; ok && last.hash == h && r.now().Sub(last.at) < reportRefresh {
		return true
	}
	if err := r.reporter.Report(ctx, opID, rep); err != nil {
		r.log.Warn("k3s upgrade: report not delivered; retrying next poll", "operation", opID, "phase", rep.Phase, "error", err.Error())
		return false
	}
	r.sent[opID] = sentReport{hash: h, at: r.now()}
	return true
}

func buildReport(phase string, nodes []nodeState, errText string) Report {
	rep := Report{Phase: phase, Servers: []NodeReport{}, Agents: []NodeReport{}, Error: errText}
	for _, n := range nodes {
		nr := NodeReport{Node: n.name, KubeletVersion: n.kubelet, Ready: n.ready}
		if n.server {
			nr.APIServerVersion = n.apiserver
			rep.Servers = append(rep.Servers, nr)
		} else {
			rep.Agents = append(rep.Agents, nr)
		}
	}
	return rep
}

func probeNotes(nodes []nodeState) string {
	var parts []string
	for _, n := range nodes {
		if n.probeErr != "" {
			parts = append(parts, "apiserver version of node "+n.name+" unknown: "+truncate(n.probeErr, 200))
		}
	}
	return strings.Join(parts, "; ")
}

func joinNotes(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "; ")
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func errDetail(err error) string {
	return truncate(strings.ReplaceAll(err.Error(), "\n", " "), 200)
}
