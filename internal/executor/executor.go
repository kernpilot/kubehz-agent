// Package executor is the ACTING side of the desired-state loop
// (managed-platform-spec §1.3.3): P3 worker scaling (MachineDeployment
// spec.replicas merge patches) and P5 self-healing (deleting unhealthy worker
// Machines, see heal.go) — so that the cluster's OWN machine-controller
// (which already holds the cluster's own hcloud credentials) does all
// provisioning/deprovisioning. The agent builds no provisioner and carries no
// cloud credential; its entire acting surface is patches/deletes on the
// machine-controller's namespaced custom resources.
//
// Safety posture (every rule enforced here, in one place):
//
//   - SERVER-GATED: acting requires the /desired doc's server-computed
//     execution flags (scaling / upgrades / healing). There is no agent-side
//     override to enable any of them; when a flag is off the corresponding
//     loop touches nothing, and with everything off the executor clears its
//     reports entirely (fail toward report-only, §1.3.4).
//   - Pools are matched to MachineDeployments BY NAME in one configurable
//     namespace (KUBEHZ_MD_NAMESPACE, KubeOne default kube-system). The
//     platform's worker_pools.name is assumed to equal the MD's
//     metadata.name — the lok8s/KubeOne provisioning path creates both from
//     the same spec. An unmatched pool is skipped and reported failed.
//   - Control-plane protection: an MD that LOOKS like a control-plane pool
//     (label or name heuristics, see isControlPlane) is refused. KubeOne
//     control-plane nodes are static (not MDs at all), so this guards other
//     topologies and mislabeled pools — belt and braces.
//   - Autoscaler-conflict refusal (spec P3): an MD carrying
//     cluster-autoscaler node-group min/max annotations belongs to the
//     autoscaler; scaling it manually would fight a control loop. Refused.
//   - Replica bounds: desiredReplicas outside 0..KUBEHZ_MAX_REPLICAS is
//     REFUSED and reported failed — never rewritten to the bound and
//     applied. Writing a number the server did not ask for would silently
//     diverge intent from outcome.
//   - One action at a time: pools are processed sequentially in name order,
//     and the first TRANSIENT failure halts the pass (remaining pools report
//     pending and are retried on the next poll) — the §3 halt-on-failure
//     discipline, sized to P3.
//   - Idempotent: a pool already at desiredReplicas reports done without
//     patching, so restarts/re-polls reconverge with zero cluster writes.
//   - MD edits are replica edits ONLY: no MD creates, no MD deletes, no
//     machineType changes. An authorized-but-unbuilt capability
//     (execution.upgrades with a pending version) is reported as an
//     unsupported/failed action rather than improvised.
//   - The P5 healer's guardrails (CP refusal, storm brake, cooldown,
//     concurrency budget, autoscaler skip) are documented in heal.go.
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

// MachineDeploymentGVR is re-exported from internal/machines (the single
// source of truth for the machine-controller API surface). KubeOne's bundled
// kubermatic machine-controller serves MachineDeployment / MachineSet /
// Machine on cluster.k8s.io/v1alpha1 — the LEGACY machine-api group, NOT
// cluster.x-k8s.io (real CAPI). Grounded in spec §0: the pilot runs
// quay.io/kubermatic/machine-controller v1.65.0 on exactly this group; the
// old manifests' cluster.x-k8s.io RBAC was one of the fictions P0 retired.
var MachineDeploymentGVR = machines.MachineDeploymentGVR

// FieldManager identifies the agent's patches in managedFields/audit logs.
const FieldManager = "kubehz-agent"

// callTimeout bounds each apiserver call so a wedged connection can never
// stall the poll loop.
const callTimeout = 15 * time.Second

// maxPoolsPerPass bounds how many desired pools one pass processes — the
// platform is authenticated but still external input; a runaway document
// must not turn into an unbounded patch storm. (The server's own per-cluster
// pool count is far below this.)
const maxPoolsPerPass = 100

// autoscalerAnnotationKeys mark an MD as cluster-autoscaler-managed. The
// clusterapi provider reads its node-group bounds from these annotations —
// both the legacy cluster.k8s.io and the cluster.x-k8s.io spellings are
// treated as ownership claims (heuristic, deliberately broad: when in doubt,
// do not fight another controller).
var autoscalerAnnotationKeys = []string{
	"cluster.k8s.io/cluster-api-autoscaler-node-group-min-size",
	"cluster.k8s.io/cluster-api-autoscaler-node-group-max-size",
	"cluster.x-k8s.io/cluster-api-autoscaler-node-group-min-size",
	"cluster.x-k8s.io/cluster-api-autoscaler-node-group-max-size",
}

// controlPlaneLabelKeys mark a control-plane machine template.
var controlPlaneLabelKeys = []string{
	"node-role.kubernetes.io/control-plane",
	"node-role.kubernetes.io/master",
}

// NodeSource supplies the current nodes (the agent wires the node informer
// lister; tests wire fixtures). nil disables node-health-based healing —
// only the joinless-machine case can then fire.
type NodeSource func() ([]*corev1.Node, error)

// Executor implements desired.Actor. One instance, driven by the Poller.
type Executor struct {
	dyn         dynamic.Interface
	namespace   string
	maxReplicas int
	store       *actions.Store
	// observedVersion returns the cluster's current server version (may be
	// empty when unknown) — the P6 pre-flight's control-plane truth and the
	// upgrade-report gate.
	observedVersion func() string
	// nodes supplies node health for the P5 healer.
	nodes NodeSource
	log   *slog.Logger

	// now is the healer's clock (injectable for tests).
	now func() time.Time
	// baseline is the executor's construction time: a RESTART starts a fresh,
	// conservative cooldown — an agent crash-looping for any reason can never
	// turn into a machine-delete loop (spec §4 "restart = fresh cooldown").
	baseline time.Time
	// lastHeal tracks the last remediation per pool (in-memory only, by
	// design: the docs' guardrails plus baseline make persistence unnecessary
	// and a PV/CRD would be state the user has to clean up).
	lastHeal map[string]time.Time
}

// Options configures New. Namespace and MaxReplicas are required by the
// caller (config defaults them); everything else may be zero.
type Options struct {
	Namespace       string
	MaxReplicas     int
	ObservedVersion func() string
	Nodes           NodeSource
	Now             func() time.Time // injectable clock (tests)
	Logger          *slog.Logger
}

// New builds an Executor.
func New(dyn dynamic.Interface, store *actions.Store, opts Options) *Executor {
	if opts.ObservedVersion == nil {
		opts.ObservedVersion = func() string { return "" }
	}
	if opts.Nodes == nil {
		opts.Nodes = func() ([]*corev1.Node, error) { return nil, nil }
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Executor{
		dyn:             dyn,
		namespace:       opts.Namespace,
		maxReplicas:     opts.MaxReplicas,
		store:           store,
		observedVersion: opts.ObservedVersion,
		nodes:           opts.Nodes,
		log:             opts.Logger,
		now:             opts.Now,
		baseline:        opts.Now(),
		lastHeal:        make(map[string]time.Time),
	}
}

// Reconcile drives the cluster toward doc, reporting every outcome into the
// actions store (the heartbeat carries the store's snapshot on every beat).
// Returns true when a re-run on the next poll tick is needed: after a
// transient failure, and ALWAYS while healing is enabled — healing is a
// continuous control loop (nodes fail independently of revision changes), so
// it re-evaluates at the poll cadence rather than only on a new revision.
func (e *Executor) Reconcile(ctx context.Context, doc *desired.Doc) (retry bool) {
	if doc == nil {
		return false
	}
	// Healing requires BOTH the execution flag and the policy's enabled
	// mirror (defense in depth against a half-updated server).
	healing := doc.Execution.Healing && doc.Healing.Enabled
	if !doc.Execution.Scaling && !doc.Execution.Upgrades && !healing {
		// Report-only posture: the server (tier gate × access × kill switch)
		// says no acting. Clear any previous reports — the next beat's absent
		// actions[] is the server's "clear" signal — and touch NOTHING.
		e.store.Clear()
		return false
	}

	e.store.Begin(doc.Revision)

	if doc.Execution.Upgrades {
		e.reportUpgradeUnsupported(doc)
	}
	if doc.Execution.Scaling {
		retry = e.scalePools(ctx, doc)
	}
	if healing {
		e.healPass(ctx, doc)
		retry = true // continuous loop: re-evaluate health every poll tick
	}
	return retry
}

// reportUpgradeUnsupported reports an authorized upgrade this agent version
// cannot execute ("report anything else as unsupported" — never improvise).
// Nothing is reported when there is no pending upgrade (version null/empty,
// already at target, or the observed version is unknown).
func (e *Executor) reportUpgradeUnsupported(doc *desired.Doc) {
	if doc.KubernetesVersion == nil {
		return
	}
	want := strings.TrimSpace(*doc.KubernetesVersion)
	if want == "" {
		return
	}
	observed := e.observedVersion()
	if observed == "" || sameVersion(observed, want) {
		return
	}
	e.store.Upsert(state.Action{
		Type:     state.ActionUpgrade,
		Target:   want,
		Status:   state.ActionFailed,
		Detail:   "upgrade execution is not supported by this agent version (P3 executes replica scaling only); upgrade via lo provision / kubeone apply",
		Revision: doc.Revision,
	})
	e.log.Warn("desired kubernetes version differs but upgrades are unsupported",
		"observed", observed, "desired", want, "revision", doc.Revision)
}

// scalePools processes the desired pools sequentially (name order, one action
// at a time). Returns true if any pool needs a retry.
func (e *Executor) scalePools(ctx context.Context, doc *desired.Doc) (retry bool) {
	pools := make([]desired.WorkerPool, len(doc.WorkerPools))
	copy(pools, doc.WorkerPools)
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	if len(pools) > maxPoolsPerPass {
		e.log.Warn("desired doc has more pools than one pass processes; truncating",
			"pools", len(pools), "max", maxPoolsPerPass)
		pools = pools[:maxPoolsPerPass]
	}
	if len(pools) == 0 {
		return false
	}

	// One List per pass: the executor deliberately runs NO informer on MDs —
	// it reads fresh state only when the server authorizes acting.
	lctx, cancel := context.WithTimeout(ctx, callTimeout)
	list, err := e.dyn.Resource(MachineDeploymentGVR).Namespace(e.namespace).List(lctx, metav1.ListOptions{})
	cancel()
	if err != nil {
		// Nothing can be diffed: every pool fails transiently, retry next poll.
		detail := "cannot list MachineDeployments in " + e.namespace + ": " + errDetail(err)
		for _, pool := range pools {
			e.report(doc.Revision, pool.Name, state.ActionFailed, detail)
		}
		e.log.Warn("machinedeployment list failed", "namespace", e.namespace, "error", err.Error())
		return true
	}
	byName := make(map[string]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		byName[list.Items[i].GetName()] = &list.Items[i]
	}

	halted := false
	for _, pool := range pools {
		if halted {
			// Halt-on-failure: everything behind the failed action stays
			// pending and is retried on the next poll.
			e.report(doc.Revision, pool.Name, state.ActionPending, "queued: an earlier action failed; retrying next poll")
			continue
		}
		outcome := e.scaleOne(ctx, doc.Revision, pool, byName[pool.Name])
		switch outcome {
		case outcomeTransient:
			halted = true
			retry = true
		case outcomeRefused:
			// A policy refusal is a final verdict for this revision — it does
			// not halt the pass and does not warrant a retry.
		case outcomeConverged:
		}
	}
	return retry
}

// scaleOne outcomes.
type outcome int

const (
	outcomeConverged outcome = iota // done (patched or already at desired)
	outcomeRefused                  // policy refusal — final for this revision
	outcomeTransient                // transient failure — retry next poll
)

// scaleOne applies one pool's desired replicas to its MD, reporting
// pending → in-progress → done/failed as it goes.
func (e *Executor) scaleOne(ctx context.Context, revision int, pool desired.WorkerPool, md *unstructured.Unstructured) outcome {
	// Boundary validation: the pool name becomes an action target and an API
	// request path. A name the server should never send (empty, oversized,
	// not a DNS subdomain) is refused outright; an EMPTY name cannot even be
	// reported (no valid target), so it is logged only.
	if pool.Name == "" {
		e.log.Warn("desired pool with empty name ignored", "revision", revision)
		return outcomeRefused
	}
	if len(validation.IsDNS1123Subdomain(pool.Name)) > 0 {
		e.report(revision, pool.Name, state.ActionFailed, "invalid pool name (not a DNS-1123 subdomain); refusing")
		return outcomeRefused
	}

	e.report(revision, pool.Name, state.ActionPending, "")

	if md == nil {
		e.report(revision, pool.Name, state.ActionFailed,
			fmt.Sprintf("no MachineDeployment named %q in namespace %q (pools are matched to MDs by name)", pool.Name, e.namespace))
		return outcomeRefused
	}
	if isControlPlane(md) {
		e.report(revision, pool.Name, state.ActionFailed,
			"refusing to scale: MachineDeployment looks like a control-plane pool (label/name heuristic)")
		return outcomeRefused
	}
	if key, ok := hasAutoscalerAnnotations(md); ok {
		e.report(revision, pool.Name, state.ActionFailed,
			"refusing to scale: pool is managed by cluster-autoscaler ("+key+" present)")
		return outcomeRefused
	}
	if pool.DesiredReplicas < 0 || pool.DesiredReplicas > e.maxReplicas {
		e.report(revision, pool.Name, state.ActionFailed,
			fmt.Sprintf("desiredReplicas %d outside the agent's allowed range 0..%d (KUBEHZ_MAX_REPLICAS); refusing — bounds are never rewritten and applied", pool.DesiredReplicas, e.maxReplicas))
		return outcomeRefused
	}

	current, known := currentReplicas(md)
	if known && current == int64(pool.DesiredReplicas) {
		// Idempotent no-op: already converged; confirm without a write.
		e.report(revision, pool.Name, state.ActionDone,
			fmt.Sprintf("already at %d replicas", pool.DesiredReplicas))
		return outcomeConverged
	}

	e.report(revision, pool.Name, state.ActionInProgress,
		fmt.Sprintf("patching spec.replicas to %d", pool.DesiredReplicas))

	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": pool.DesiredReplicas}})
	if err != nil { // unreachable for this shape; guard anyway
		e.report(revision, pool.Name, state.ActionFailed, "internal: marshal patch: "+errDetail(err))
		return outcomeTransient
	}
	pctx, cancel := context.WithTimeout(ctx, callTimeout)
	_, err = e.dyn.Resource(MachineDeploymentGVR).Namespace(e.namespace).Patch(
		pctx, pool.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: FieldManager})
	cancel()
	if err != nil {
		detail := "patch failed: " + errDetail(err)
		if apierrors.IsForbidden(err) {
			detail += " — is the managed RBAC overlay (deploy/managed) applied?"
		}
		e.report(revision, pool.Name, state.ActionFailed, detail)
		e.log.Warn("machinedeployment patch failed",
			"pool", pool.Name, "namespace", e.namespace, "error", err.Error())
		return outcomeTransient
	}

	from := "unknown"
	if known {
		from = fmt.Sprintf("%d", current)
	}
	e.report(revision, pool.Name, state.ActionDone,
		fmt.Sprintf("replicas %s to %d; machine-controller reconciles the machines with the cluster's own credentials", from, pool.DesiredReplicas))
	e.log.Info("machinedeployment scaled",
		"pool", pool.Name, "namespace", e.namespace, "from", from, "to", pool.DesiredReplicas, "revision", revision)
	return outcomeConverged
}

func (e *Executor) report(revision int, target, status, detail string) {
	e.store.Upsert(state.Action{
		Type:     state.ActionScale,
		Target:   target,
		Status:   status,
		Detail:   detail,
		Revision: revision,
	})
}

// currentReplicas reads spec.replicas tolerantly (int64 from the apiserver's
// JSON decoding, but float64 survives some merge-patch paths and fakes).
func currentReplicas(md *unstructured.Unstructured) (int64, bool) {
	v, found, _ := unstructured.NestedFieldNoCopy(md.Object, "spec", "replicas")
	if !found {
		return 0, false
	}
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// isControlPlane decides whether an MD looks like a control-plane pool.
// HEURISTIC, deliberately biased toward refusal (a false positive means "we
// won't scale it" — safe; a false negative on a real control plane would be
// dangerous): control-plane/master node-role labels on the MD or its machine
// template, or a name containing "control-plane"/"controlplane"/"master", or
// a delimiter-separated "cp" segment (so "prod-cp-1" matches but "cpx31" —
// a Hetzner machine type — does not).
func isControlPlane(md *unstructured.Unstructured) bool {
	labelPaths := [][]string{
		{"metadata", "labels"},
		{"spec", "template", "metadata", "labels"},
		{"spec", "template", "spec", "metadata", "labels"},
		{"spec", "template", "spec", "labels"},
	}
	for _, path := range labelPaths {
		m, found, _ := unstructured.NestedStringMap(md.Object, path...)
		if !found {
			continue
		}
		for _, key := range controlPlaneLabelKeys {
			if _, ok := m[key]; ok {
				return true
			}
		}
	}

	name := strings.ToLower(md.GetName())
	if strings.Contains(name, "control-plane") || strings.Contains(name, "controlplane") || strings.Contains(name, "master") {
		return true
	}
	for _, seg := range strings.FieldsFunc(name, func(r rune) bool {
		return r == '-' || r == '.' || r == '_'
	}) {
		if seg == "cp" {
			return true
		}
	}
	return false
}

// hasAutoscalerAnnotations reports whether the MD carries cluster-autoscaler
// node-group annotations (and which key matched, for the refusal detail).
func hasAutoscalerAnnotations(md *unstructured.Unstructured) (string, bool) {
	ann := md.GetAnnotations()
	for _, key := range autoscalerAnnotationKeys {
		if _, ok := ann[key]; ok {
			return key, true
		}
	}
	return "", false
}

// errDetail renders an error for an action detail: single line, bounded (the
// payload caps clamp again — this just keeps logs/reports readable). The
// truncation is rune-safe so a multi-byte apiserver message never yields
// invalid UTF-8.
func errDetail(err error) string {
	s := strings.ReplaceAll(err.Error(), "\n", " ")
	if r := []rune(s); len(r) > 160 {
		s = string(r[:160]) + "…"
	}
	return s
}

// sameVersion compares two Kubernetes versions ignoring a leading "v".
func sameVersion(a, b string) bool {
	return strings.TrimPrefix(a, "v") == strings.TrimPrefix(b, "v")
}
