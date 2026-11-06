// Package desired implements the P3 desired-state PULL loop
// (managed-platform-spec §3): the agent periodically GETs the platform's
// desired-state document and hands it to a local Actor. The §1.3.1
// outbound-only invariant holds by construction — the agent OPENS this
// connection too; the platform never pushes a command, never holds an inbound
// credential, and expresses intent only through this one read-only,
// agent-token-authed endpoint. Whatever acting follows happens LOCALLY with
// the cluster's own machinery (see internal/executor).
package desired

import (
	"fmt"
	"regexp"
)

// Doc is the 200 body of GET /api/clusters/{id}/desired — wire-identical to
// kubehz-api (server/api/clusters/[id]/desired.ts, d57c206). Build to exactly
// this; nothing here is speculative.
//
// NOTE on the ETag: since P5 the server's ETag format is
// "<revision>-<scaling><upgrades><healing>" (THREE flag bits; P3/P4 emitted
// two). The client treats the ETag as an OPAQUE token — cache the header,
// echo it verbatim in If-None-Match, never parse it — so format changes cost
// exactly one extra 200 after a server deploy and nothing else.
type Doc struct {
	// Revision is the server's desired_revision — bumped on every desired-state
	// write (worker-pool create/patch/delete, version PATCH).
	Revision int `json:"revision"`
	// KubernetesVersion is the DECLARED target version, or null when never
	// declared (the server maps its v0.0.0 registration placeholder to null so
	// an agent can never read a phantom downgrade).
	KubernetesVersion *string `json:"kubernetesVersion"`
	// WorkerPools is the desired pool set (name, machineType, desiredReplicas).
	WorkerPools []WorkerPool `json:"workerPools"`
	// Execution is the SERVER-computed acting authorization (tier gate x
	// cluster access x per-feature kill switch). The agent OBEYS it and never
	// computes or overrides it — there is deliberately no client-side knob that
	// can turn acting ON (spec §1.4 door 3; fail toward report-only).
	Execution Execution `json:"execution"`
	// Healing is the P5 self-healing policy: the SERVER-owned guardrails the
	// remediation loop must obey (defaults ⊕ the cluster's stored override).
	// Enabled mirrors execution.healing — the policy numbers are served even
	// while healing is off, so the agent can pre-arm without acting.
	Healing Healing `json:"healing"`
	// StateStore is the managed-state block of a self-hosted k3s cluster
	// (managed-state build contract 4.4). nil on every other cluster. The
	// k3s upgrade executor (internal/k3supgrade) acts on it only when
	// Maintenance is true and the operation is published or running.
	StateStore *StateStore `json:"stateStore,omitempty"`
}

// StateStore is the managed-state block of the desired document.
type StateStore struct {
	// Enabled is true when the cluster's datastore is managed state.
	Enabled bool `json:"enabled"`
	// Maintenance is the customer's opt-in for k3s upgrades. Off: the agent
	// writes no Plan and removes the Plans it wrote.
	Maintenance bool `json:"maintenance"`
	// RenewPaused is true while an upgrade operation runs. The host renewer
	// gets the same fact from the renewal check; the agent only logs it.
	RenewPaused bool `json:"renewPaused"`
	// Fenced is true while the cluster is fenced for a rollback (design 4.4
	// step 1). The executor then writes nothing and removes its Plans.
	// Not in the build contract yet: absent means false.
	Fenced bool `json:"fenced,omitempty"`
	// Operation is the current upgrade operation, or nil.
	Operation *Operation `json:"operation,omitempty"`
}

// Operation is one k3s upgrade operation of the platform.
type Operation struct {
	ID            string `json:"id"`
	TargetVersion string `json:"targetVersion"`
	// State is pending, dumping, published, running, failed, done or
	// cancelled. The agent acts only on published and running.
	State string `json:"state"`
	// DumpTakenAt is the RFC 3339 time of the logical dump taken for this
	// operation. The agent does not start an operation whose dump is older
	// than one hour.
	DumpTakenAt string `json:"dumpTakenAt,omitempty"`
}

// Operation states (build contract 4.1).
const (
	OperationPending   = "pending"
	OperationDumping   = "dumping"
	OperationPublished = "published"
	OperationRunning   = "running"
	OperationFailed    = "failed"
	OperationDone      = "done"
	OperationCancelled = "cancelled"
)

// Active reports whether the agent may act on the operation: published or
// running. Every other state, a future one too, means "write nothing".
func (o *Operation) Active() bool {
	return o != nil && (o.State == OperationPublished || o.State == OperationRunning)
}

// operationIDRE is the shape of an operation id: a valid label value (the
// executor puts it in a label on each Plan) that is also safe in a URL path.
var operationIDRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$`)

// k3sVersionRE is an exact k3s release version, for example v1.37.2+k3s1.
// A channel name or a range never matches.
var k3sVersionRE = regexp.MustCompile(`^v1\.[0-9]{1,3}\.[0-9]{1,3}\+k3s[0-9]{1,3}$`)

// ValidK3sVersion reports whether v is an exact k3s release version.
func ValidK3sVersion(v string) bool { return k3sVersionRE.MatchString(v) }

// ValidOperationID reports whether id is a valid operation id.
func ValidOperationID(id string) bool { return operationIDRE.MatchString(id) }

// WorkerPool is one desired pool row.
type WorkerPool struct {
	Name            string `json:"name"`
	MachineType     string `json:"machineType"`
	DesiredReplicas int    `json:"desiredReplicas"`
}

// Execution mirrors the server's ExecutionFlags (utils/desired.ts).
type Execution struct {
	Scaling  bool `json:"scaling"`
	Upgrades bool `json:"upgrades"`
	Healing  bool `json:"healing"`
}

// Healing mirrors the server's healing block (utils/desired.ts,
// HEALING_POLICY_DEFAULTS ⊕ clusters.healing_policy): every numeric field is
// whole seconds / a count, server-validated to 0..86400 (PATCH path) and
// junk-proofed to non-negative integers (effectiveHealingPolicy).
type Healing struct {
	// Enabled mirrors execution.healing (never stored server-side). The agent
	// requires BOTH this and Execution.Healing — defense in depth against a
	// half-updated server.
	Enabled bool `json:"enabled"`
	// MaxUnhealthy: refuse remediation when MORE than this many worker
	// nodes/machines are unhealthy (a mass outage is not per-node hardware
	// failure), and never run more than this many remediations concurrently.
	// 0 = never remediate.
	MaxUnhealthy int `json:"maxUnhealthy"`
	// NodeStartupTimeoutSeconds: grace for a fresh machine's node to join
	// before the machine counts as failed.
	NodeStartupTimeoutSeconds int `json:"nodeStartupTimeoutSeconds"`
	// UnhealthyAfterSeconds: how long a node must be continuously
	// NotReady/Unknown before remediation.
	UnhealthyAfterSeconds int `json:"unhealthyAfterSeconds"`
	// CooldownSeconds: minimum pause between two remediations in the same
	// pool (storm brake).
	CooldownSeconds int `json:"cooldownSeconds"`
}

// Validate checks the boundary invariants of a freshly decoded doc. The
// platform is authenticated but still an EXTERNAL input to the cluster: a
// nonsensical document is rejected here, before any acting code sees it.
// Per-pool checks (name shape, replica bounds) stay in the executor, which
// reports them as per-pool action outcomes instead of dropping the whole doc.
func (d *Doc) Validate() error {
	if d.Revision < 0 {
		return fmt.Errorf("desired doc invalid: negative revision %d", d.Revision)
	}
	// Healing guardrails are safety-critical numbers: a negative or absurd
	// value can only come from a broken/hostile server, and acting on it
	// (e.g. maxUnhealthy 2^31 disabling the storm brake) is worse than
	// rejecting the whole document (fail toward report-only).
	if d.Healing.MaxUnhealthy < 0 || d.Healing.MaxUnhealthy > maxHealingMaxUnhealthy {
		return fmt.Errorf("desired doc invalid: healing.maxUnhealthy %d outside 0..%d", d.Healing.MaxUnhealthy, maxHealingMaxUnhealthy)
	}
	for _, f := range []struct {
		name string
		v    int
	}{
		{"nodeStartupTimeoutSeconds", d.Healing.NodeStartupTimeoutSeconds},
		{"unhealthyAfterSeconds", d.Healing.UnhealthyAfterSeconds},
		{"cooldownSeconds", d.Healing.CooldownSeconds},
	} {
		if f.v < 0 || f.v > maxHealingSeconds {
			return fmt.Errorf("desired doc invalid: healing.%s %d outside 0..%d", f.name, f.v, maxHealingSeconds)
		}
	}
	// The operation id goes into a Plan label and a request path, and the
	// target version goes into a privileged upgrade job. Both are checked
	// here, before any acting code sees them.
	if ss := d.StateStore; ss != nil && ss.Operation != nil {
		op := ss.Operation
		if !operationIDRE.MatchString(op.ID) {
			return fmt.Errorf("desired doc invalid: stateStore.operation.id %q is not a label value (letters, digits, '-', '_', '.'; max 63)", op.ID)
		}
		if !k3sVersionRE.MatchString(op.TargetVersion) {
			return fmt.Errorf("desired doc invalid: stateStore.operation.targetVersion %q is not an exact k3s version (want v1.N.N+k3sN)", op.TargetVersion)
		}
	}
	return nil
}

// Agent-side boundary bounds for the healing policy. The server's PATCH path
// caps everything at 86400 (24h) and maxUnhealthy at 100; the agent allows
// slack over those (a junk-proofed out-of-band row can legally exceed the
// PATCH bounds) but rejects the absurd.
const (
	maxHealingMaxUnhealthy = 1_000
	maxHealingSeconds      = 1_000_000 // ≈11.5 days
)
