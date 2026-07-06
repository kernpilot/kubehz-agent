// P5 SELF-HEALING (managed-platform-spec §4): remediate unhealthy WORKER
// machines by DELETING the backing Machine — machine-controller/MachineSet
// then recreates it with the cluster's own credentials. Deleting a Machine is
// the sharpest permission this agent holds, so every guardrail here is hard
// and refusal-biased:
//
//   - SERVER-GATED twice over: execution.healing AND healing.enabled (the
//     doc's policy mirror) must both be true; the policy NUMBERS (maxUnhealthy,
//     timeouts, cooldown) are server-owned — the agent obeys, never computes.
//   - NEVER control-plane: CP nodes are skipped at detection (node role
//     labels), CP-labeled machines are skipped, and a candidate whose owning
//     MachineDeployment looks CP (same isControlPlane heuristic the scale
//     executor uses) is refused loudly. On KubeOne, CP nodes are static (no
//     Machine at all) — these are belt and braces for other topologies.
//   - STORM BRAKE: when the total unhealthy worker count exceeds
//     maxUnhealthy, NOTHING is deleted — a mass outage is a network/hcloud/
//     control-plane problem, not per-node hardware failure, and deleting
//     machines into it makes it worse. Every candidate is reported failed
//     with the refusal so the outage is visible on the dashboard.
//   - CONCURRENCY BUDGET: at most maxUnhealthy remediations in flight
//     cluster-wide; ANY already-deleting machine (ours, a scale-down, a P6
//     roll replacement) counts against the budget — conservative on purpose.
//   - PER-POOL COOLDOWN: cooldownSeconds between remediations in the same
//     pool, with the executor's construction time as a baseline — an agent
//     RESTART starts a fresh cooldown, so a crash-looping agent can never
//     become a machine-delete loop.
//   - AUTOSCALER POOLS are skipped (refused loudly): the cluster-autoscaler
//     owns their machine lifecycle; fighting it is worse than a NotReady node.
//   - UNOWNED machines are refused: deleting a Machine no MachineSet/
//     MachineDeployment recreates would permanently remove capacity.
//
// Detection (both windows floored at 60s agent-side — clamping a guardrail
// TIGHTER is safe; the server's PATCH validation enforces ≥60 anyway):
//
//   - a node whose Ready condition is != True continuously for
//     unhealthyAfterSeconds (duration from the condition's own
//     lastTransitionTime — no agent-side bookkeeping to lose on restart);
//   - a machine whose node NEVER appeared within nodeStartupTimeoutSeconds
//     of creation (status.nodeRef still unset).
//
// Every remediation and every refusal is a `heal` action riding the
// heartbeat. The action TARGET is the MACHINE name (contract decision: stable
// pre-join — the startup-timeout case has no node name at all; dashboards
// treat the target as an opaque identifier); the node name, when known, is in
// the detail. Stale pending/refused reports are pruned once a node recovers
// on its own; done reports stay visible for the revision.
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

	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// minDetectionWindow is the agent-side FLOOR for both detection windows: a
// junk-proofed server row could legally carry 0, and sub-minute remediation
// would fight the kubelet's own grace periods.
const minDetectionWindow = 60 * time.Second

// healCandidate is one unhealthy worker machine.
type healCandidate struct {
	machineName string
	pool        string                     // "" = unresolved (refused)
	md          *unstructured.Unstructured // nil = unresolved (refused)
	cause       string                     // human cause for the action detail
}

// healPass runs one remediation evaluation. It never returns an error — a
// failed read fails soft (healing re-evaluates on the next poll tick), and
// every acted-on or refused candidate is reported through the actions store.
func (e *Executor) healPass(ctx context.Context, doc *desired.Doc) {
	pol := doc.Healing
	now := e.now()
	unhealthyAfter := flooredWindow(pol.UnhealthyAfterSeconds)
	startupTimeout := flooredWindow(pol.NodeStartupTimeoutSeconds)
	cooldown := time.Duration(pol.CooldownSeconds) * time.Second

	lctx, cancel := context.WithTimeout(ctx, callTimeout)
	mds, err := machines.ListMachineDeployments(lctx, e.dyn, e.namespace)
	cancel()
	if err != nil {
		e.log.Warn("heal: machinedeployment list failed (fail-soft; retrying next poll)",
			"namespace", e.namespace, "error", err.Error())
		return
	}
	lctx, cancel = context.WithTimeout(ctx, callTimeout)
	machineList, err := machines.ListMachines(lctx, e.dyn, e.namespace)
	cancel()
	if err != nil {
		e.log.Warn("heal: machine list failed (fail-soft; is the managed overlay's machines RBAC applied?)",
			"namespace", e.namespace, "error", err.Error())
		return
	}
	nodes, err := e.nodes()
	if err != nil {
		e.log.Warn("heal: node read failed (fail-soft; retrying next poll)", "error", err.Error())
		return
	}

	resolver := machines.NewPoolResolver(mds)

	// In-flight disruptions: ANY machine already being deleted counts —
	// whether we deleted it, a scale-down did, or a P6 roll is replacing it.
	// Conservative: healing never stacks disruption on top of disruption.
	inFlight := 0
	byNodeName := make(map[string]*unstructured.Unstructured, len(machineList))
	for i := range machineList {
		m := &machineList[i]
		if machines.Deleting(m) {
			inFlight++
			continue
		}
		if n := machines.NodeRefName(m); n != "" {
			byNodeName[n] = m
		}
	}

	// ── Detection ────────────────────────────────────────────────────────────
	var cands []healCandidate
	seen := make(map[string]bool)
	add := func(m *unstructured.Unstructured, cause string) {
		name := m.GetName()
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		pool, md := resolver.PoolFor(m)
		cands = append(cands, healCandidate{machineName: name, pool: pool, md: md, cause: cause})
	}

	// (a) Machines whose node never joined within the startup timeout.
	for i := range machineList {
		m := &machineList[i]
		if machines.Deleting(m) || machines.NodeRefName(m) != "" {
			continue
		}
		age := machines.Age(m, now)
		if age <= 0 || age < startupTimeout {
			continue
		}
		if isControlPlaneMachine(m) {
			e.log.Warn("heal: control-plane-labeled machine without a node ignored", "machine", m.GetName())
			continue
		}
		add(m, fmt.Sprintf("no node joined within %s of creation (nodeStartupTimeout %s)",
			age.Truncate(time.Second), startupTimeout))
	}

	// (b) Nodes continuously NotReady/Unknown beyond the unhealthy window.
	for _, node := range nodes {
		if node == nil {
			continue
		}
		status, since := nodeReadyCondition(node)
		if status == corev1.ConditionTrue {
			continue
		}
		if since.IsZero() || now.Sub(since) < unhealthyAfter {
			continue // can't establish the duration, or not unhealthy long enough
		}
		if isControlPlaneNode(node) {
			continue // NEVER a control-plane node — not this agent's domain
		}
		m := byNodeName[node.Name]
		if m == nil {
			continue // no backing machine (static worker / CP): not healable
		}
		add(m, fmt.Sprintf("node %s %s for %s (unhealthyAfter %s)",
			node.Name, readyStateText(status), now.Sub(since).Truncate(time.Second), unhealthyAfter))
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].machineName < cands[j].machineName })

	// active collects every target reported THIS pass; stale heal reports
	// (recovered nodes) are pruned afterwards — done outcomes are kept.
	active := make(map[string]bool, len(cands))
	defer func() { e.store.Prune(state.ActionHeal, active) }()

	if len(cands) == 0 {
		return
	}

	// ── STORM BRAKE: more unhealthy than maxUnhealthy → refuse EVERYTHING.
	if len(cands) > pol.MaxUnhealthy {
		detail := fmt.Sprintf("unhealthy count %d exceeds maxUnhealthy %d — refusing (possible outage)",
			len(cands), pol.MaxUnhealthy)
		for _, c := range cands {
			active[c.machineName] = true
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed, detail)
		}
		e.log.Warn("heal: storm brake engaged", "unhealthy", len(cands), "maxUnhealthy", pol.MaxUnhealthy)
		return
	}

	budget := pol.MaxUnhealthy - inFlight
	halted := false
	for _, c := range cands {
		active[c.machineName] = true
		switch {
		case halted:
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				"queued: an earlier remediation failed; retrying next poll")
		case c.md == nil:
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed,
				"refusing to heal: no owning MachineDeployment resolved — deleting an unowned machine would remove capacity permanently")
		case isControlPlane(c.md):
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed,
				"refusing to heal: owning MachineDeployment looks like a control-plane pool (label/name heuristic)")
		case hasAutoscalerKey(c.md):
			key, _ := hasAutoscalerAnnotations(c.md)
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed,
				"refusing to heal: pool "+c.pool+" is managed by cluster-autoscaler ("+key+" present)")
		case budget <= 0:
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				fmt.Sprintf("waiting: %d disruption(s) already in flight (maxUnhealthy %d)", inFlight, pol.MaxUnhealthy))
		case now.Sub(e.lastHealTime(c.pool)) < cooldown:
			wait := (cooldown - now.Sub(e.lastHealTime(c.pool))).Truncate(time.Second)
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				fmt.Sprintf("cooldown: next remediation in pool %s allowed in %s", c.pool, wait))
		default:
			if !e.remediate(ctx, doc.Revision, c, now) {
				halted = true // §3 halt-on-failure: queue the rest, retry next poll
			} else {
				inFlight++
				budget--
			}
		}
	}
}

// remediate deletes one candidate's Machine, reporting in-progress →
// done/failed. Returns false on failure (halts the pass).
func (e *Executor) remediate(ctx context.Context, revision int, c healCandidate, now time.Time) bool {
	e.reportHeal(revision, c.machineName, state.ActionInProgress, "deleting Machine ("+c.cause+")")

	dctx, cancel := context.WithTimeout(ctx, callTimeout)
	err := e.dyn.Resource(machines.MachineGVR).Namespace(e.namespace).Delete(dctx, c.machineName, metav1.DeleteOptions{})
	cancel()
	if err != nil && !apierrors.IsNotFound(err) { // already gone = converged
		detail := "delete failed: " + errDetail(err)
		if apierrors.IsForbidden(err) {
			detail += " — is the managed RBAC overlay (deploy/managed) applied?"
		}
		e.reportHeal(revision, c.machineName, state.ActionFailed, detail)
		e.log.Warn("heal: machine delete failed",
			"machine", c.machineName, "pool", c.pool, "namespace", e.namespace, "error", err.Error())
		return false
	}

	e.lastHeal[c.pool] = now
	e.reportHeal(revision, c.machineName, state.ActionDone,
		fmt.Sprintf("deleted Machine (%s); machine-controller/MachineSet recreates it with the cluster's own credentials", c.cause))
	e.log.Info("heal: machine remediated",
		"machine", c.machineName, "pool", c.pool, "namespace", e.namespace, "cause", c.cause)
	return true
}

// lastHealTime returns the pool's last remediation time, never earlier than
// the executor's construction baseline (restart = conservative fresh cooldown).
func (e *Executor) lastHealTime(pool string) time.Time {
	if t, ok := e.lastHeal[pool]; ok && t.After(e.baseline) {
		return t
	}
	return e.baseline
}

func (e *Executor) reportHeal(revision int, target, status, detail string) {
	e.store.Upsert(state.Action{
		Type:     state.ActionHeal,
		Target:   target,
		Status:   status,
		Detail:   detail,
		Revision: revision,
	})
}

// flooredWindow converts a policy seconds value to a duration, flooring at
// minDetectionWindow (tightening a guardrail is safe; loosening never is).
func flooredWindow(seconds int) time.Duration {
	d := time.Duration(seconds) * time.Second
	if d < minDetectionWindow {
		return minDetectionWindow
	}
	return d
}

// nodeReadyCondition extracts the node's Ready condition status and its
// lastTransitionTime. A node WITHOUT a Ready condition reports Unknown with a
// zero time — the caller skips it (no way to establish a duration).
func nodeReadyCondition(node *corev1.Node) (corev1.ConditionStatus, time.Time) {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status, c.LastTransitionTime.Time
		}
	}
	return corev1.ConditionUnknown, time.Time{}
}

// readyStateText renders a non-True Ready status for action details.
func readyStateText(s corev1.ConditionStatus) string {
	if s == corev1.ConditionUnknown {
		return "Unknown"
	}
	return "NotReady"
}

// isControlPlaneNode reports whether the node carries a control-plane role
// label — such nodes are never remediation candidates.
func isControlPlaneNode(node *corev1.Node) bool {
	for _, key := range controlPlaneLabelKeys {
		if _, ok := node.Labels[key]; ok {
			return true
		}
	}
	return false
}

// isControlPlaneMachine reports whether the machine itself is CP-labeled
// (metadata labels or the node-template labels in spec.metadata).
func isControlPlaneMachine(m *unstructured.Unstructured) bool {
	for _, path := range [][]string{
		{"metadata", "labels"},
		{"spec", "metadata", "labels"},
	} {
		lbls, found, _ := unstructured.NestedStringMap(m.Object, path...)
		if !found {
			continue
		}
		for _, key := range controlPlaneLabelKeys {
			if _, ok := lbls[key]; ok {
				return true
			}
		}
	}
	return false
}

// hasAutoscalerKey adapts hasAutoscalerAnnotations for switch-case use.
func hasAutoscalerKey(md *unstructured.Unstructured) bool {
	_, ok := hasAutoscalerAnnotations(md)
	return ok
}
