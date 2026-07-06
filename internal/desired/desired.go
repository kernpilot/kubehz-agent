// Package desired implements the P3 desired-state PULL loop
// (managed-platform-spec §3): the agent periodically GETs the platform's
// desired-state document and hands it to a local Actor. The §1.3.1
// outbound-only invariant holds by construction — the agent OPENS this
// connection too; the platform never pushes a command, never holds an inbound
// credential, and expresses intent only through this one read-only,
// agent-token-authed endpoint. Whatever acting follows happens LOCALLY with
// the cluster's own machinery (see internal/executor).
package desired

import "fmt"

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
}

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
