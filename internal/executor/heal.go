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
//   - LOCAL SANITY BOUNDS, owed to no server number: one pass remediates at
//     most a THIRD of a pool (floored at one machine), and a pool — or the
//     whole worker set — whose nodes ALL went unhealthy inside one short
//     window is refused outright. Simultaneous failure is an outage or a
//     partition, and a partitioned node is indistinguishable from a dead one
//     from here. See localBoundsFor.
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
// FOLLOW-THROUGH: a remediated Machine whose node is truly dead can wedge on
// eviction (pods stuck Terminating on an unreachable node never confirm) and
// sit deleting indefinitely — a zombie server billing the user. The bounded
// eviction unwedge in unwedge.go force-deletes exactly those stuck pods, only
// for machines THIS agent heal-deleted, once per machine, after
// KUBEHZ_HEAL_EVICTION_TIMEOUT_SECONDS.
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

// LOCAL SANITY BOUNDS. maxUnhealthy, the cooldown and the in-flight budget are
// all SERVER-owned numbers, and a wrong-but-plausible document (maxUnhealthy
// 50 on a 12-machine cluster) satisfies every one of them while real servers
// are torn down one cooldown apart. The two bounds below need no server input:
// they are computed from what the agent sees in its own cluster.
const (
	// healFractionNumerator/healFractionDenominator cap what ONE pass may
	// remediate as a FRACTION of the pool — rounded down, floored at one so a
	// small pool still heals. The excess waits for the next pass and is
	// reported, so the blast radius of a single pass never scales with a
	// number the platform sent.
	healFractionNumerator   = 1
	healFractionDenominator = 3

	// simultaneousFailureWindow: nodes that ALL stop reporting inside this
	// window failed TOGETHER. Independent hardware does not do that; a network
	// partition, an apiserver outage or a control-plane restart does — and in
	// those the servers are alive and still running the user's workload while
	// their nodes read Unknown. The agent cannot tell a dead node from a
	// partitioned one, so it does not guess: it refuses and reports.
	//
	// Five minutes, not seconds: the node controller flips every unreachable
	// node within one grace period, and a partition that spreads over a few
	// minutes is still a partition. The cost of the wide window is a delayed
	// remediation, which the cluster survives; the cost of a narrow one is a
	// deleted healthy server.
	simultaneousFailureWindow = 5 * time.Minute

	// minSimultaneousNodes: simultaneity needs at least two nodes to mean
	// anything. A one-node pool has nothing to fail together with, and
	// refusing there would disable healing for exactly the case it exists for
	// (the cluster-wide check still covers such pools).
	minSimultaneousNodes = 2
)

// poolCensus is the LOCAL evidence one pass has about a pool: how many
// machines it owns, how many of them joined a node, when each unhealthy node
// last changed its Ready condition, and how many candidates it produced.
type poolCensus struct {
	machines   int
	nodes      int
	unhealthy  []time.Time
	candidates int
}

// localBounds is what the cluster's own state says one pass may do:
// refuseEverything (an outage signature across the whole worker set),
// refusePool (the same signature inside one pool) and perPool (the fraction
// cap per pool).
type localBounds struct {
	refuseEverything string
	refusePool       map[string]string
	perPool          map[string]int
}

// capFor is how many machines of one pool this pass may remediate.
func (b localBounds) capFor(pool string) int {
	if n, ok := b.perPool[pool]; ok {
		return n
	}
	return 1
}

// healCandidate is one unhealthy worker machine.
type healCandidate struct {
	machineName string
	pool        string                     // "" = unresolved (refused)
	md          *unstructured.Unstructured // nil = unresolved (refused)
	node        string                     // node name ("" = never joined)
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

	// Post-heal follow-through: unwedge evictions stuck on machines THIS agent
	// heal-deleted whose node is still unreachable (see unwedge.go). Runs
	// before detection so it fires even when there is no new candidate.
	e.unwedgePass(ctx, doc.Revision, machineList, nodes, now)

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
	add := func(m *unstructured.Unstructured, node, cause string) {
		name := m.GetName()
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		pool, md := resolver.PoolFor(m)
		cands = append(cands, healCandidate{machineName: name, pool: pool, md: md, node: node, cause: cause})
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
		add(m, "", fmt.Sprintf("no node joined within %s of creation (nodeStartupTimeout %s)",
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
		add(m, node.Name, fmt.Sprintf("node %s %s for %s (unhealthyAfter %s)",
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

	// ── LOCAL SANITY BOUNDS (independent of the server's numbers).
	bounds := e.localBoundsFor(cands, machineList, nodes, byNodeName, resolver)
	// remediated counts THIS pass's deletions per pool, against the fraction cap.
	remediated := make(map[string]int, len(cands))

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
		case bounds.refuseEverything != "":
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed, bounds.refuseEverything)
		case bounds.refusePool[c.pool] != "":
			e.reportHeal(doc.Revision, c.machineName, state.ActionFailed, bounds.refusePool[c.pool])
		case budget <= 0:
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				fmt.Sprintf("waiting: %d disruption(s) already in flight (maxUnhealthy %d)", inFlight, pol.MaxUnhealthy))
		case now.Sub(e.lastHealTime(c.pool)) < cooldown:
			wait := (cooldown - now.Sub(e.lastHealTime(c.pool))).Truncate(time.Second)
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				fmt.Sprintf("cooldown: next remediation in pool %s allowed in %s", c.pool, wait))
		case remediated[c.pool] >= bounds.capFor(c.pool):
			e.reportHeal(doc.Revision, c.machineName, state.ActionPending,
				fmt.Sprintf("waiting: pool %s already had %d remediation(s) this pass — one pass remediates at most %d/%d of a pool",
					c.pool, remediated[c.pool], healFractionNumerator, healFractionDenominator))
		default:
			if !e.remediate(ctx, doc.Revision, c, now) {
				halted = true // §3 halt-on-failure: queue the rest, retry next poll
			} else {
				inFlight++
				budget--
				remediated[c.pool]++
			}
		}
	}
}

// localBoundsFor reads the cluster's OWN evidence and returns what this pass
// may do. Two rules, both computed locally, both refusal-biased:
//
//  1. SIMULTANEOUS FAILURE → refuse. Every node of a pool (or of the whole
//     worker set) unhealthy, with the Ready transitions inside
//     simultaneousFailureWindow, is an outage or a partition, not hardware.
//     Deleting those machines would destroy healthy capacity to "fix" a
//     reachability problem. Needs at least minSimultaneousNodes nodes — one
//     node has nothing to be simultaneous with.
//  2. FRACTION → cap. One pass remediates at most healFraction of a pool
//     (floored at one machine); the rest waits and is reported. maxUnhealthy
//     cannot express this: it is one absolute number for clusters of any size.
func (e *Executor) localBoundsFor(
	cands []healCandidate,
	machineList []unstructured.Unstructured,
	nodes []*corev1.Node,
	byNodeName map[string]*unstructured.Unstructured,
	resolver *machines.PoolResolver,
) localBounds {
	census := make(map[string]*poolCensus)
	entry := func(pool string) *poolCensus {
		c, ok := census[pool]
		if !ok {
			c = &poolCensus{}
			census[pool] = c
		}
		return c
	}

	for i := range machineList {
		m := &machineList[i]
		if machines.Deleting(m) {
			continue
		}
		if pool, _ := resolver.PoolFor(m); pool != "" {
			entry(pool).machines++
		}
	}

	clusterNodes := 0
	var clusterUnhealthy []time.Time
	for _, node := range nodes {
		if node == nil || isControlPlaneNode(node) {
			continue
		}
		m := byNodeName[node.Name]
		if m == nil {
			continue // no backing machine (static worker): not healable, not evidence
		}
		pool, _ := resolver.PoolFor(m)
		if pool == "" {
			continue
		}
		c := entry(pool)
		c.nodes++
		clusterNodes++
		status, since := nodeReadyCondition(node)
		if status == corev1.ConditionTrue || since.IsZero() {
			continue
		}
		c.unhealthy = append(c.unhealthy, since)
		clusterUnhealthy = append(clusterUnhealthy, since)
	}

	for _, c := range cands {
		if c.pool != "" {
			entry(c.pool).candidates++
		}
	}

	bounds := localBounds{
		refusePool: make(map[string]string, len(census)),
		perPool:    make(map[string]int, len(census)),
	}
	for pool, c := range census {
		bounds.perPool[pool] = maxPerPass(c.machines)
	}

	if clusterNodes >= minSimultaneousNodes && len(clusterUnhealthy) == clusterNodes && withinWindow(clusterUnhealthy) {
		bounds.refuseEverything = fmt.Sprintf(
			"refusing to heal: all %d worker nodes went unhealthy within %s — an outage or a partition, not node hardware",
			clusterNodes, simultaneousFailureWindow)
		e.log.Warn("heal: every worker node failed at once; refusing all remediation",
			"nodes", clusterNodes, "window", simultaneousFailureWindow.String())
		return bounds
	}

	for _, pool := range sortedKeys(census) {
		c := census[pool]
		if c.candidates == 0 {
			continue
		}
		if c.nodes >= minSimultaneousNodes && len(c.unhealthy) == c.nodes && withinWindow(c.unhealthy) {
			bounds.refusePool[pool] = fmt.Sprintf(
				"refusing to heal: all %d nodes of pool %s went unhealthy within %s — an outage or a partition, not node hardware",
				c.nodes, pool, simultaneousFailureWindow)
			e.log.Warn("heal: every node of a pool failed at once; refusing its remediation",
				"pool", pool, "nodes", c.nodes, "window", simultaneousFailureWindow.String())
		}
	}
	return bounds
}

// maxPerPass is how many machines of a pool of this size one pass may
// remediate: the configured fraction, rounded down, floored at one.
func maxPerPass(poolMachines int) int {
	n := poolMachines * healFractionNumerator / healFractionDenominator
	if n < 1 {
		return 1
	}
	return n
}

// withinWindow reports whether every transition time lies inside
// simultaneousFailureWindow (first to last).
func withinWindow(times []time.Time) bool {
	if len(times) == 0 {
		return false
	}
	first, last := times[0], times[0]
	for _, t := range times[1:] {
		if t.Before(first) {
			first = t
		}
		if t.After(last) {
			last = t
		}
	}
	return last.Sub(first) <= simultaneousFailureWindow
}

// sortedKeys returns a map's keys in a deterministic order (log and report
// order must not depend on map iteration).
func sortedKeys(m map[string]*poolCensus) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
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
	if err == nil {
		// Track OUR deletion for the eviction unwedge (unwedge.go): only a
		// machine the agent itself heal-deleted is ever unwedge-eligible. A
		// NotFound is not tracked — nothing was deleted by us just now.
		e.healDeleted[c.machineName] = &healDeletion{node: c.node, deletedAt: now}
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
