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
// kubehz-api (server/api/clusters/[id]/desired.ts, dfa9b7a). Build to exactly
// this; nothing here is speculative.
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
	return nil
}
