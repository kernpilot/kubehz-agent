// P5 EVICTION UNWEDGE — the bounded follow-through to a heal deletion.
//
// Dogfood finding (2026-07-05, kubehz.in.net): after healing deletes a Machine
// whose node is truly dead (kubelet stopped), machine-controller retries
// eviction indefinitely (~11s loop) because the non-DaemonSet pods stuck
// Terminating on the unreachable node never confirm deletion — the Machine
// sat deleting ~12 minutes (a zombie Hetzner server, billing the user) until
// the pods were force-deleted manually. Availability was never at risk (the
// replacement machine was already up); this is purely a stuck-teardown /
// billing problem, and this file fixes exactly that and nothing more.
//
// HARD BOUNDS (every one refusal-biased and unit-tested):
//
//   - ONLY machines THIS agent heal-deleted (the in-memory healDeleted
//     tracking written by remediate) — never a manually-deleted machine, never
//     a scale-down, never a P6 roll replacement. A restart forgets the
//     tracking, which DISABLES the unwedge for in-flight teardowns
//     (conservative: a stuck teardown is a billing nuisance, not an outage).
//   - ONLY after the machine has been deleting for
//     KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS (default 300) — normal eviction
//     gets every chance to finish first.
//   - ONLY while the node is still unreachable (Ready == Unknown/False). A
//     recovered node (Ready=True) confirms its own pods — do nothing. A node
//     OBJECT that is gone needs no unwedge either (pod GC handles it).
//   - ONLY pods ALREADY Terminating (deletionTimestamp set) on THAT node —
//     the unwedge never initiates an eviction, it only confirms ones the
//     dead kubelet cannot. Each delete carries the pod's UID as a
//     precondition, so a same-name replacement (StatefulSet) that landed on
//     a healthy node in the meantime can never be hit.
//   - ONE unwedge attempt per machine, ever (attempted flag) — force-deleting
//     pods is not a loop the agent may run.
//   - RBAC fail-soft: without the pods-delete ClusterRole
//     (deploy/managed/rbac-managed.yaml) the attempt warns loudly, reports,
//     and never crashes or retries — healing itself keeps working.
//
// The outcome rides the existing `heal` action for the machine (a detail
// update, status stays done — the remediation DID happen), so dashboards see
// "eviction unwedged (N pods)" without a new action type.
package executor

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// podsGVR is the core/v1 pods resource the unwedge force-deletes through the
// dynamic client (same client every other executor write uses).
var podsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

// maxUnwedgePods bounds one attempt's force-deletes (defensive, in the spirit
// of maxPoolsPerPass): a node hosting more stuck pods than this is not a
// wedged eviction, it is something an operator should look at.
const maxUnwedgePods = 100

// healDeletion tracks one machine this agent heal-deleted (remediate writes
// it; unwedgePass consumes and prunes it).
type healDeletion struct {
	node      string    // node name at remediation time ("" = joinless: nothing to unwedge)
	deletedAt time.Time // when OUR delete was accepted
	attempted bool      // one unwedge attempt per machine, ever
}

// unwedgePass checks every tracked heal-deleted machine and unwedges the ones
// whose teardown is provably stuck. Called from healPass (so it runs only
// while healing is server-armed) with the machine/node state that pass
// already read. Never returns an error — every failure mode degrades to
// "teardown stays stuck", which is where we started.
func (e *Executor) unwedgePass(ctx context.Context, revision int, machineList []unstructured.Unstructured, nodes []*corev1.Node, now time.Time) {
	if len(e.healDeleted) == 0 {
		return
	}

	byName := make(map[string]*unstructured.Unstructured, len(machineList))
	for i := range machineList {
		byName[machineList[i].GetName()] = &machineList[i]
	}
	nodeByName := make(map[string]*corev1.Node, len(nodes))
	for _, n := range nodes {
		if n != nil {
			nodeByName[n.Name] = n
		}
	}

	for name, tr := range e.healDeleted {
		m := byName[name]
		if m == nil || !machines.Deleting(m) {
			// Teardown finished (gone), or the name is no longer OUR deleting
			// machine — either way the tracking is stale. Forget it.
			delete(e.healDeleted, name)
			continue
		}
		if tr.attempted || tr.node == "" {
			continue // one attempt per machine; joinless machines have no pods
		}
		if now.Sub(tr.deletedAt) < e.evictionTimeout {
			continue // normal eviction still gets its chance
		}
		node := nodeByName[tr.node]
		if node == nil {
			continue // node object gone: pod GC confirms the pods, not us
		}
		if status, _ := nodeReadyCondition(node); status == corev1.ConditionTrue {
			continue // node recovered: its kubelet confirms evictions itself
		}
		if e.pods == nil {
			e.log.Warn("heal: eviction appears wedged but no pod source is wired; unwedge disabled",
				"machine", name, "node", tr.node)
			continue
		}
		stuck, err := e.stuckPodsOn(tr.node)
		if err != nil {
			// Fail-soft WITHOUT burning the attempt: a lister hiccup is
			// transient; the next poll re-evaluates.
			e.log.Warn("heal: pod read failed; unwedge deferred to next poll",
				"machine", name, "node", tr.node, "error", err.Error())
			continue
		}
		if len(stuck) == 0 {
			continue // nothing Terminating: the wedge (if any) is not pods
		}

		tr.attempted = true // ONE attempt, whatever its outcome
		e.unwedgeOne(ctx, revision, name, tr, stuck, now)
	}
}

// stuckPodsOn returns the pods on node that already carry a deletionTimestamp
// (Terminating), sorted for deterministic behavior, capped at maxUnwedgePods.
func (e *Executor) stuckPodsOn(node string) ([]*corev1.Pod, error) {
	pods, err := e.pods()
	if err != nil {
		return nil, err
	}
	var stuck []*corev1.Pod
	for _, p := range pods {
		if p == nil || p.Spec.NodeName != node || p.DeletionTimestamp == nil {
			continue
		}
		stuck = append(stuck, p)
	}
	sort.Slice(stuck, func(i, j int) bool {
		if stuck[i].Namespace != stuck[j].Namespace {
			return stuck[i].Namespace < stuck[j].Namespace
		}
		return stuck[i].Name < stuck[j].Name
	})
	if len(stuck) > maxUnwedgePods {
		e.log.Warn("heal: unwedge pod set truncated", "node", node,
			"stuck", len(stuck), "max", maxUnwedgePods)
		stuck = stuck[:maxUnwedgePods]
	}
	return stuck, nil
}

// unwedgeOne force-deletes the stuck pods (gracePeriodSeconds 0, UID
// precondition) and updates the machine's heal action detail with the
// outcome. Status stays done — the remediation itself DID happen.
func (e *Executor) unwedgeOne(ctx context.Context, revision int, machineName string, tr *healDeletion, stuck []*corev1.Pod, now time.Time) {
	var zero int64
	deleted, failed := 0, 0
	for _, p := range stuck {
		uid := p.UID
		opts := metav1.DeleteOptions{GracePeriodSeconds: &zero}
		if uid != "" {
			// The UID precondition makes "force-delete THIS stuck pod" exact:
			// a same-name replacement scheduled elsewhere fails the
			// precondition instead of dying.
			opts.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		dctx, cancel := context.WithTimeout(ctx, callTimeout)
		err := e.dyn.Resource(podsGVR).Namespace(p.Namespace).Delete(dctx, p.Name, opts)
		cancel()
		switch {
		case err == nil:
			deleted++
		case apierrors.IsNotFound(err) || apierrors.IsConflict(err):
			// Already gone, or the UID no longer matches (pod replaced) —
			// both mean there is nothing left to unwedge for this pod.
		case apierrors.IsForbidden(err):
			// RBAC fail-soft: warn loudly, report, never crash. All further
			// deletes would fail identically, so stop here.
			detail := fmt.Sprintf(
				"deleted Machine; eviction unwedge forbidden after %s (pods delete RBAC missing — deploy/managed unwedge ClusterRole); teardown may stay stuck",
				now.Sub(tr.deletedAt).Truncate(time.Second))
			e.reportHeal(revision, machineName, state.ActionDone, detail)
			e.log.Warn("heal: eviction unwedge forbidden — is the deploy/managed pods-delete ClusterRole applied?",
				"machine", machineName, "node", tr.node, "error", err.Error())
			return
		default:
			failed++
			e.log.Warn("heal: unwedge pod force-delete failed",
				"pod", p.Namespace+"/"+p.Name, "node", tr.node, "error", err.Error())
		}
	}

	detail := fmt.Sprintf(
		"deleted Machine; eviction unwedged after %s: force-deleted %d pod(s) stuck terminating on unreachable node %s",
		now.Sub(tr.deletedAt).Truncate(time.Second), deleted, tr.node)
	if failed > 0 {
		detail += fmt.Sprintf(" (%d failed)", failed)
	}
	e.reportHeal(revision, machineName, state.ActionDone, detail)
	e.log.Info("heal: eviction unwedged",
		"machine", machineName, "node", tr.node,
		"podsForceDeleted", deleted, "podsFailed", failed,
		"deletingFor", now.Sub(tr.deletedAt).Truncate(time.Second).String())
}
