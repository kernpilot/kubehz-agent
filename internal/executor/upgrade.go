// P6 WORKER UPGRADES (managed-platform-spec §3): roll each desired worker
// pool's kubelet toward the desired kubernetesVersion by patching the pool
// MachineDeployment's spec.template.spec.versions.kubelet (the exact
// machine-controller v1.65.0 field path: MachineDeploymentSpec.Template
// .Spec.Versions.Kubelet) — machine-controller then performs the rolling
// machine REPLACEMENT itself, with the cluster's own credentials. The agent
// never touches nodes, never drains, never SSHes; one field of one resource.
//
// Scope, deliberately narrow (v1):
//   - WORKERS ONLY. Control-plane upgrades stay user-driven (lo provision /
//     kubeone apply); the pre-flight below makes workers-first impossible.
//   - PRE-FLIGHT: the OBSERVED control-plane version (discovery /version —
//     the same value the heartbeat reports) must be at or above the target
//     minor, else every pending pool reports failed "control plane not yet
//     at target". Unknown observed version → refuse (never guess).
//   - ONE POOL ROLLS AT A TIME (maxUnhealthy-style disruption discipline):
//     a pool is patched only when no other pool is mid-roll; the rest queue
//     as pending. A roll is "in flight" until every non-deleting machine of
//     the pool runs the target version AND has a joined node.
//   - Autoscaler-managed pools are refused (reported unsupported): their
//     machine lifecycle belongs to the cluster-autoscaler.
//   - Control-plane-looking MDs are refused (same heuristic as scaling).
//   - HALT ON FAILURE: the first patch failure stops the pass; later pools
//     stay pending and the pass retries on the next poll tick.
//
// Progress reporting: one `upgrade` action per pool, target = the POOL name,
// detail "vFROM → vTO (n/m)" where n = machines at the target version with a
// joined node and m = the pool's machine count (floored at spec.replicas, so
// a mid-roll machine gap can never fake convergence).
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// poolStatus is one desired pool's observed upgrade state.
type poolStatus struct {
	pool      desired.WorkerPool
	md        *unstructured.Unstructured
	current   string // MD's spec.template.spec.versions.kubelet
	atTarget  bool   // MD already declares the target
	n, m      int    // machines at target (joined) / pool machine count
	converged bool   // atTarget && n >= m
}

// upgradePools drives the desired kubernetesVersion into the worker MDs.
// Returns true when a re-run on the next poll tick is needed (mid-roll
// progress, a waiting pre-flight, or a transient failure).
func (e *Executor) upgradePools(ctx context.Context, doc *desired.Doc) (retry bool) {
	if doc.KubernetesVersion == nil {
		return false
	}
	target := strings.TrimSpace(*doc.KubernetesVersion)
	if target == "" {
		return false
	}

	pools := make([]desired.WorkerPool, 0, len(doc.WorkerPools))
	for _, p := range doc.WorkerPools {
		if p.Name == "" || len(validation.IsDNS1123Subdomain(p.Name)) > 0 {
			continue // the scaling pass reports malformed names; nothing to roll
		}
		pools = append(pools, p)
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	if len(pools) > maxPoolsPerPass {
		pools = pools[:maxPoolsPerPass]
	}
	if len(pools) == 0 {
		return false
	}

	// One MD list + one Machine list per pass (no informers: read fresh state
	// only when the server authorizes acting).
	lctx, cancel := context.WithTimeout(ctx, callTimeout)
	mds, err := machines.ListMachineDeployments(lctx, e.dyn, e.namespace)
	cancel()
	if err != nil {
		detail := "cannot list MachineDeployments in " + e.namespace + ": " + errDetail(err)
		for _, p := range pools {
			e.reportUpgrade(doc.Revision, p.Name, state.ActionFailed, detail)
		}
		return true
	}
	lctx, cancel = context.WithTimeout(ctx, callTimeout)
	machineList, err := machines.ListMachines(lctx, e.dyn, e.namespace)
	cancel()
	if err != nil {
		detail := "cannot list Machines in " + e.namespace + " (progress counting needs the managed overlay's machines read): " + errDetail(err)
		for _, p := range pools {
			e.reportUpgrade(doc.Revision, p.Name, state.ActionFailed, detail)
		}
		return true
	}
	resolver := machines.NewPoolResolver(mds)
	byName := make(map[string]*unstructured.Unstructured, len(mds))
	for i := range mds {
		byName[mds[i].GetName()] = &mds[i]
	}

	// Phase 1: observe every pool. rollingExists guards the one-at-a-time rule
	// even when the mid-roll pool sorts AFTER a pool that wants to start.
	statuses := make([]poolStatus, 0, len(pools))
	needsRoll := false
	rollingExists := false
	for _, p := range pools {
		st := poolStatus{pool: p, md: byName[p.Name]}
		if st.md != nil {
			st.current = machines.MDKubeletVersion(st.md)
			st.atTarget = st.current != "" && sameVersion(st.current, target)
			st.n, st.m = e.rollProgress(machineList, resolver, st.md, p.Name, target)
			st.converged = st.atTarget && st.n >= st.m
			if st.atTarget && !st.converged && !isControlPlane(st.md) {
				if _, autoscaled := hasAutoscalerAnnotations(st.md); !autoscaled {
					rollingExists = true
				}
			}
			if !st.atTarget && st.current != "" {
				needsRoll = true
			}
		}
		statuses = append(statuses, st)
	}

	// PRE-FLIGHT (only when something actually needs rolling): the observed
	// control-plane version must be ≥ the target minor — workers never outrun
	// the CP, and the OBSERVED version is the in-cluster truth (the server's
	// own skew gate fails open before the first heartbeat; this one never
	// guesses). retry=true: the CP upgrading is exactly what we wait for.
	if needsRoll {
		tMaj, tMin, ok := parseMajorMinor(target)
		if !ok {
			for _, st := range statuses {
				if st.md != nil && !st.atTarget {
					e.reportUpgrade(doc.Revision, st.pool.Name, state.ActionFailed,
						fmt.Sprintf("invalid desired version %q; refusing", target))
				}
			}
			return retry
		}
		observed := e.observedVersion()
		oMaj, oMin, obsOK := parseMajorMinor(observed)
		switch {
		case !obsOK:
			for _, st := range statuses {
				if st.md != nil && !st.atTarget {
					e.reportUpgrade(doc.Revision, st.pool.Name, state.ActionFailed,
						"control plane version unknown — refusing to roll workers until it is observed")
				}
			}
			return true
		case oMaj < tMaj || (oMaj == tMaj && oMin < tMin):
			for _, st := range statuses {
				if st.md != nil && !st.atTarget {
					e.reportUpgrade(doc.Revision, st.pool.Name, state.ActionFailed,
						fmt.Sprintf("control plane not yet at target: observed %s < target %s — upgrade the control plane first (lo provision / kubeone apply)",
							observed, target))
				}
			}
			return true
		}
	}

	// Phase 2: act, one pool at a time, halt on failure.
	halted := false
	for _, st := range statuses {
		name := st.pool.Name
		switch {
		case halted:
			e.reportUpgrade(doc.Revision, name, state.ActionPending,
				"queued: an earlier action failed; retrying next poll")
		case st.md == nil:
			e.reportUpgrade(doc.Revision, name, state.ActionFailed,
				fmt.Sprintf("no MachineDeployment named %q in namespace %q (pools are matched to MDs by name)", name, e.namespace))
		case st.current == "":
			e.reportUpgrade(doc.Revision, name, state.ActionFailed,
				"MachineDeployment has no spec.template.spec.versions.kubelet; refusing to invent one")
		case isControlPlane(st.md):
			if !st.atTarget {
				e.reportUpgrade(doc.Revision, name, state.ActionFailed,
					"refusing to upgrade: MachineDeployment looks like a control-plane pool (label/name heuristic)")
			}
		case hasAutoscalerKey(st.md):
			if !st.atTarget {
				e.reportUpgrade(doc.Revision, name, state.ActionFailed,
					"unsupported: pool is managed by cluster-autoscaler — roll it via the autoscaler's node-group tooling")
			}
		case st.converged:
			// Report done only for a roll THIS process drove or observed
			// mid-flight (rollFrom entry) — a pool that was always at target
			// has no upgrade to report.
			if from, ok := e.rollFrom[name]; ok {
				e.reportUpgrade(doc.Revision, name, state.ActionDone,
					fmt.Sprintf("%s → %s (%d/%d)", from, target, st.n, st.m))
			}
		case st.atTarget: // mid-roll: MD declares the target, machines catching up
			from, ok := e.rollFrom[name]
			if !ok {
				from = deriveFromVersion(machineList, resolver, name, target)
				e.rollFrom[name] = from
			}
			e.reportUpgrade(doc.Revision, name, state.ActionInProgress,
				fmt.Sprintf("%s → %s (%d/%d)", from, target, st.n, st.m))
			retry = true
		case rollingExists:
			e.reportUpgrade(doc.Revision, name, state.ActionPending,
				"queued: one pool rolls at a time")
			retry = true
		default: // start this pool's roll
			if e.patchKubelet(ctx, doc.Revision, st, target) {
				rollingExists = true
				retry = true
			} else {
				halted = true
				retry = true
			}
		}
	}
	return retry
}

// patchKubelet patches one MD's kubelet version and reports the outcome.
// Returns false on failure (halts the pass).
func (e *Executor) patchKubelet(ctx context.Context, revision int, st poolStatus, target string) bool {
	name := st.pool.Name
	e.reportUpgrade(revision, name, state.ActionInProgress,
		fmt.Sprintf("%s → %s (%d/%d)", st.current, target, st.n, st.m))

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"versions": map[string]any{"kubelet": target},
				},
			},
		},
	})
	if err != nil { // unreachable for this shape; guard anyway
		e.reportUpgrade(revision, name, state.ActionFailed, "internal: marshal patch: "+errDetail(err))
		return false
	}
	pctx, cancel := context.WithTimeout(ctx, callTimeout)
	_, err = e.dyn.Resource(machines.MachineDeploymentGVR).Namespace(e.namespace).Patch(
		pctx, name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: FieldManager})
	cancel()
	if err != nil {
		detail := "patch failed: " + errDetail(err)
		if apierrors.IsForbidden(err) {
			detail += " — is the managed RBAC overlay (deploy/managed) applied?"
		}
		e.reportUpgrade(revision, name, state.ActionFailed, detail)
		e.log.Warn("upgrade: machinedeployment kubelet patch failed",
			"pool", name, "namespace", e.namespace, "error", err.Error())
		return false
	}

	e.rollFrom[name] = st.current
	e.reportUpgrade(revision, name, state.ActionInProgress,
		fmt.Sprintf("%s → %s (%d/%d)", st.current, target, st.n, st.m))
	e.log.Info("upgrade: worker pool kubelet roll started",
		"pool", name, "namespace", e.namespace, "from", st.current, "to", target)
	return true
}

// rollProgress counts the pool's machines: n = at the target version WITH a
// joined node, m = all non-deleting machines, floored at the MD's declared
// replicas so a mid-roll machine gap (old deleted, new not created yet) can
// never fake convergence.
func (e *Executor) rollProgress(machineList []unstructured.Unstructured, resolver *machines.PoolResolver, md *unstructured.Unstructured, pool, target string) (n, m int) {
	for i := range machineList {
		mach := &machineList[i]
		if machines.Deleting(mach) {
			continue
		}
		if p, _ := resolver.PoolFor(mach); p != pool {
			continue
		}
		m++
		if sameVersion(machines.MachineKubeletVersion(mach), target) && machines.NodeRefName(mach) != "" {
			n++
		}
	}
	if replicas, ok := currentReplicas(md); ok && int(replicas) > m {
		m = int(replicas)
	}
	return n, m
}

// deriveFromVersion recovers the pre-roll version after a restart mid-roll:
// the (deterministically first) machine still running a non-target version.
// Falls back to the target itself when every machine already declares it.
func deriveFromVersion(machineList []unstructured.Unstructured, resolver *machines.PoolResolver, pool, target string) string {
	bestName, bestVersion := "", ""
	for i := range machineList {
		mach := &machineList[i]
		if p, _ := resolver.PoolFor(mach); p != pool {
			continue
		}
		v := machines.MachineKubeletVersion(mach)
		if v == "" || sameVersion(v, target) {
			continue
		}
		if bestName == "" || mach.GetName() < bestName {
			bestName = mach.GetName()
			bestVersion = v
		}
	}
	if bestVersion == "" {
		return target
	}
	return bestVersion
}

func (e *Executor) reportUpgrade(revision int, pool, status, detail string) {
	e.store.Upsert(state.Action{
		Type:     state.ActionUpgrade,
		Target:   pool,
		Status:   status,
		Detail:   detail,
		Revision: revision,
	})
}

// parseMajorMinor parses "v1.35.6" / "1.35" / "v1.35.5+build" → (1, 35, true).
func parseMajorMinor(v string) (major, minor int, ok bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil || major < 0 {
		return 0, 0, false
	}
	// The minor may carry a suffix in exotic builds; take leading digits.
	digits := parts[1]
	for i, r := range digits {
		if r < '0' || r > '9' {
			digits = digits[:i]
			break
		}
	}
	minor, err = strconv.Atoi(digits)
	if err != nil || minor < 0 {
		return 0, 0, false
	}
	return major, minor, true
}
