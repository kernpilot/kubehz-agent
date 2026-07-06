// Package state defines the schema-2 live-view payload the agent POSTs to
// kubehz-api, plus the caps that keep every field within the server's
// HeartbeatSchema bounds.
//
// Wire compatibility (grounded against kubehz-api server/utils/validation.ts,
// server/utils/heartbeat.ts on 2026-07-05):
//
//   - The live HeartbeatSchema is a NON-strict zod object, so the additive
//     schema-2 fields (schema, agent, workloads, events, pools, desired) and
//     the extra per-node fields (ready, kubeletVersion, capacity) are ACCEPTED
//     by today's API — unknown keys are stripped, the base fields persist. A
//     schema-2 payload therefore degrades gracefully to schema-1 storage until
//     the API is extended (managed-platform-spec §2). Nothing here breaks the
//     deployed endpoint.
//   - The base fields (clusterId, timestamp, kubernetes.version, nodes[]{name,
//     status, roles, instanceType?}, components[]{name,status}, certificates)
//     match the live schema exactly, including its caps: nodes<=500,
//     components<=50, name<=253, status<=63, roles<=253, instanceType<=64,
//     version/timestamp<=64. ApplyCaps enforces those so a well-behaved agent
//     is never 400'd for an oversized payload.
package state

import "sort"

// SchemaVersion marks this as the operator-mode live-view payload. Schema-1
// (the bash CronJob) omits the field; the server treats its absence as v1.
const SchemaVersion = 2

// Caps mirror kubehz-api's HeartbeatSchema (validation.ts). Keep in sync.
const (
	MaxNodes      = 500
	MaxComponents = 50
	MaxEvents     = 50 // agent-side cap on reported warnings
	MaxNameLen    = 253
	MaxStatusLen  = 63
	MaxRolesLen   = 253
	MaxTypeLen    = 64
	MaxVersionLen = 64
	MaxNoteLen    = 256
	// MaxNamespaces bounds the opt-in byNamespace pod-count map — the one
	// otherwise-unbounded payload dimension (a many-tenant cluster can carry
	// thousands of namespaces).
	MaxNamespaces = 500
	// MaxActions mirrors HB_MAX_ACTIONS (kubehz-api dfa9b7a): the P3
	// desired-state action reports are capped at 20 per beat.
	MaxActions = 20
	// MaxMachineIssues mirrors HB_MAX_MACHINE_ISSUES (kubehz-api d57c206):
	// machine-controller provisioning failures are capped at 20 per beat.
	MaxMachineIssues = 20
	// MaxDetailLen: an action detail shares the event-note cap (HB_MAX_NOTE_LEN).
	MaxDetailLen = MaxNoteLen
	// MaxRevision mirrors HB_MAX_COUNT — the server bounds every count-like
	// integer, including the acted desired-state revision.
	MaxRevision = 10_000_000
)

// AgentMode distinguishes the two agents that speak this contract.
const (
	ModeOperator = "operator" // this long-running informer agent (managed tier)
	ModeCronjob  = "cronjob"  // the lightweight bash heartbeat (registered tier)
)

// Action type/status enums — the exact wire values kubehz-api's actions[] zod
// schema accepts (z.enum(['scale','upgrade','heal']) / z.enum(['pending',
// 'in-progress','done','failed']), d57c206). Any other value 400s the WHOLE
// beat, so nothing outside this set may ever be assigned.
const (
	ActionScale   = "scale"
	ActionUpgrade = "upgrade"
	ActionHeal    = "heal"

	ActionPending    = "pending"
	ActionInProgress = "in-progress"
	ActionDone       = "done"
	ActionFailed     = "failed"
)

// Payload is the full live-view snapshot. It is built fresh from the informer
// listers on every push (latest-wins, matching the server's JSONB snapshot) —
// "incremental" refers to the change-TRIGGERED cadence, not delta-encoded
// content. Field-level deltas are a future optimization (see README).
type Payload struct {
	Schema     int          `json:"schema"`
	ClusterID  string       `json:"clusterId"`
	Timestamp  string       `json:"timestamp"` // RFC3339 UTC
	Agent      AgentMeta    `json:"agent"`
	Kubernetes KubeInfo     `json:"kubernetes"`
	Nodes      []NodeState  `json:"nodes"`
	Components []Component  `json:"components,omitempty"`
	Workloads  Workloads    `json:"workloads"`
	Events     []EventState `json:"events,omitempty"`

	// Actions is the P3 desired-state progress report (the pull-loop ack the
	// API ingests since dfa9b7a): what the agent DID with the desired doc it
	// pulled from GET /clusters/{id}/desired. The server persists it
	// latest-wins — a beat WITHOUT actions CLEARS them — so the agent keeps
	// reporting the current revision's actions on every beat while they are
	// relevant (the actions.Store owns that lifecycle). omitempty is
	// load-bearing: an empty store must serialize as an ABSENT key, which is
	// exactly the server's "clear" signal.
	Actions []Action `json:"actions,omitempty"`

	// MachineIssues surfaces machine-controller provisioning failures read off
	// Machine objects (kubehz-api d57c206) — e.g. hcloud rejecting a
	// webhook-accepted server type ("unsupported location for server type") —
	// so a pool that NEVER converges is visible even though its nodes never
	// join. Latest-wins server-side, exactly like actions: omitempty is
	// load-bearing (an empty list must serialize as an ABSENT key = clear).
	MachineIssues []MachineIssue `json:"machineIssues,omitempty"`

	// Pools and Desired are DESIGNED, forward-compat fields (spec §2/§3): the
	// observed MachineDeployment pools and the desired{revision,state} ack.
	// The current HeartbeatSchema accepts-and-strips them (only actions[] is
	// ingested), so the agent leaves them empty rather than inventing contract
	// values the server would later ingest with different semantics.
	Pools   []Pool      `json:"pools,omitempty"`
	Desired *DesiredAck `json:"desired,omitempty"`

	// Certificates preserves the schema-1 field. Not populated by this scaffold
	// (no CSR informer); kept optional for wire compatibility.
	Certificates *CertInfo `json:"certificates,omitempty"`
}

// AgentMeta identifies the reporting agent. version is stamped at build time.
type AgentMeta struct {
	Version string `json:"version"`
	Mode    string `json:"mode"`
}

// KubeInfo carries the SERVER gitVersion (discovery /version), NOT a client
// build tag — the correctness fix at the cluster level (the bash agent's
// `kubectl version` first-match grabbed the client tag).
type KubeInfo struct {
	Version string `json:"version"`
}

// NodeState is the per-node view, extracted from a typed corev1.Node.
type NodeState struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "Ready" | "NotReady" | "Unknown"
	// Ready is the unambiguous machine field (schema-2). The dashboard should
	// key off this rather than substring-matching Status (see README: the
	// existing readyNodeCount regex substring-matches "NotReady" as ready).
	Ready          bool          `json:"ready"`
	Roles          string        `json:"roles"` // comma-joined, e.g. "control-plane"
	InstanceType   string        `json:"instanceType,omitempty"`
	KubeletVersion string        `json:"kubeletVersion,omitempty"` // per-node SERVER version (NodeInfo)
	Capacity       *NodeCapacity `json:"capacity,omitempty"`
}

// NodeCapacity is the allocatable-facing node size, as reported by the node
// itself (strings, e.g. "4" / "8148Mi", to avoid lossy int coercion).
type NodeCapacity struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// Component is a control-plane component health line (schema-1 compatible).
type Component struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Workloads is the summary-only workload view: counts, never names or specs
// (spec §2: "workload visibility without workload contents").
type Workloads struct {
	Pods PodSummary `json:"pods"`
	// Deployments is populated when the apps informer is added (spec §2). It is
	// defined here so the payload shape is stable across that follow-up.
	Deployments *DeploymentSummary `json:"deployments,omitempty"`
}

// PodSummary counts pods by phase. ByNamespace is included ONLY when the
// deployer opts into ReportNamespaces (privacy-gated).
type PodSummary struct {
	Total       int            `json:"total"`
	Running     int            `json:"running"`
	Pending     int            `json:"pending"`
	Failed      int            `json:"failed"`
	Succeeded   int            `json:"succeeded"`
	Unknown     int            `json:"unknown"`
	ByNamespace map[string]int `json:"byNamespace,omitempty"`
}

// DeploymentSummary is the forward-compat apps-workload rollup (spec §2).
type DeploymentSummary struct {
	Total       int `json:"total"`
	Unavailable int `json:"unavailable"`
}

// EventState is one recent Warning event. Namespace and Note are privacy-gated
// (they can carry object names / free text) and only present when
// ReportNamespaces is enabled.
type EventState struct {
	Reason    string `json:"reason"`
	Kind      string `json:"kind"`
	Count     int32  `json:"count"`
	LastSeen  string `json:"lastSeen"` // RFC3339 UTC
	Namespace string `json:"namespace,omitempty"`
	Note      string `json:"note,omitempty"`
}

// Action is one desired-state progress report, wire-identical to kubehz-api's
// HeartbeatSchema actions[] entry (d57c206): Type ∈ {scale, upgrade, heal},
// Status ∈ {pending, in-progress, done, failed}; Target names the acted-on
// object (a worker-pool / MachineDeployment name for scale AND for per-pool
// upgrade progress, the remediated Machine's name for heal); Revision is the
// desired-state revision the action executed against, so the dashboard can
// correlate intent → outcome.
type Action struct {
	Type     string `json:"type"`
	Target   string `json:"target"`
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Revision int    `json:"revision"`
}

// MachineIssue is one machine-controller failure line, wire-identical to
// kubehz-api's HeartbeatSchema machineIssues[] entry (d57c206): Pool is the
// owning MachineDeployment (required, ≤63); Machine the failing Machine's name
// when one exists (optional — a rejected create may have none, and zod's
// min(1) means an EMPTY machine would 400, so omitempty is load-bearing);
// Reason a short machine-readable cause (required, ≤63); Message the human
// detail (≤256); Since the agent-observed first-seen timestamp (RFC3339, ≤64).
type MachineIssue struct {
	Pool    string `json:"pool"`
	Machine string `json:"machine,omitempty"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Since   string `json:"since"`
}

// Pool is an observed worker MachineDeployment (forward-compat, spec §2/§3).
type Pool struct {
	Name        string `json:"name"`
	MachineType string `json:"machineType,omitempty"`
	Replicas    int    `json:"replicas"`
	Ready       int    `json:"ready"`
	APIGroup    string `json:"apiGroup,omitempty"`
}

// DesiredAck is the pull-loop acknowledgement (forward-compat, spec §3).
type DesiredAck struct {
	Revision int    `json:"revision"`
	State    string `json:"state,omitempty"`
}

// CertInfo is the schema-1 certificate expiry field.
type CertInfo struct {
	ExpiresAt string `json:"expiresAt,omitempty"`
}

// ApplyCaps clamps every field to the server's HeartbeatSchema bounds so a
// well-behaved agent is never rejected for an oversized payload. It mutates p
// in place and is idempotent.
func ApplyCaps(p *Payload) {
	p.ClusterID = clamp(p.ClusterID, MaxNameLen)
	p.Timestamp = clamp(p.Timestamp, MaxVersionLen)
	p.Kubernetes.Version = clamp(p.Kubernetes.Version, MaxVersionLen)
	p.Agent.Version = clamp(p.Agent.Version, MaxTypeLen)
	p.Agent.Mode = clamp(p.Agent.Mode, MaxStatusLen)

	if len(p.Nodes) > MaxNodes {
		p.Nodes = p.Nodes[:MaxNodes]
	}
	for i := range p.Nodes {
		p.Nodes[i].Name = clamp(p.Nodes[i].Name, MaxNameLen)
		p.Nodes[i].Status = clamp(p.Nodes[i].Status, MaxStatusLen)
		p.Nodes[i].Roles = clamp(p.Nodes[i].Roles, MaxRolesLen)
		p.Nodes[i].InstanceType = clamp(p.Nodes[i].InstanceType, MaxTypeLen)
		p.Nodes[i].KubeletVersion = clamp(p.Nodes[i].KubeletVersion, MaxVersionLen)
	}

	if len(p.Components) > MaxComponents {
		p.Components = p.Components[:MaxComponents]
	}
	for i := range p.Components {
		p.Components[i].Name = clamp(p.Components[i].Name, MaxNameLen)
		p.Components[i].Status = clamp(p.Components[i].Status, MaxStatusLen)
	}

	if len(p.Events) > MaxEvents {
		p.Events = p.Events[:MaxEvents]
	}
	for i := range p.Events {
		p.Events[i].Reason = clamp(p.Events[i].Reason, MaxStatusLen)
		p.Events[i].Kind = clamp(p.Events[i].Kind, MaxStatusLen)
		p.Events[i].Namespace = clamp(p.Events[i].Namespace, MaxNameLen)
		p.Events[i].Note = clamp(p.Events[i].Note, MaxNoteLen)
	}

	// Actions: the server requires target to be 1..253 chars, so an entry with
	// an EMPTY target would 400 the whole beat — drop such entries (defense in
	// depth; the executor never produces them) instead of shipping a poisoned
	// payload. Then cap the list and clamp every string/int to the schema
	// bounds. An emptied list becomes nil so omitempty drops the key (= the
	// server's "clear actions" signal).
	kept := p.Actions[:0]
	for _, a := range p.Actions {
		if a.Target != "" {
			kept = append(kept, a)
		}
	}
	p.Actions = kept
	if len(p.Actions) > MaxActions {
		p.Actions = p.Actions[:MaxActions]
	}
	for i := range p.Actions {
		p.Actions[i].Type = clamp(p.Actions[i].Type, MaxStatusLen)
		p.Actions[i].Target = clamp(p.Actions[i].Target, MaxNameLen)
		p.Actions[i].Status = clamp(p.Actions[i].Status, MaxStatusLen)
		p.Actions[i].Detail = clamp(p.Actions[i].Detail, MaxDetailLen)
		if p.Actions[i].Revision < 0 {
			p.Actions[i].Revision = 0
		}
		if p.Actions[i].Revision > MaxRevision {
			p.Actions[i].Revision = MaxRevision
		}
	}
	if len(p.Actions) == 0 {
		p.Actions = nil
	}

	// MachineIssues: the server requires pool and reason to be non-empty (zod
	// min(1)), so an entry missing either would 400 the whole beat — drop such
	// entries (defense in depth; the collector never produces them). Then cap
	// the list and clamp every string to the schema bounds; an emptied list
	// becomes nil so omitempty drops the key (= the server's "clear" signal).
	keptIssues := p.MachineIssues[:0]
	for _, mi := range p.MachineIssues {
		if mi.Pool != "" && mi.Reason != "" {
			keptIssues = append(keptIssues, mi)
		}
	}
	p.MachineIssues = keptIssues
	if len(p.MachineIssues) > MaxMachineIssues {
		p.MachineIssues = p.MachineIssues[:MaxMachineIssues]
	}
	for i := range p.MachineIssues {
		p.MachineIssues[i].Pool = clamp(p.MachineIssues[i].Pool, MaxStatusLen)
		p.MachineIssues[i].Machine = clamp(p.MachineIssues[i].Machine, MaxNameLen)
		p.MachineIssues[i].Reason = clamp(p.MachineIssues[i].Reason, MaxStatusLen)
		p.MachineIssues[i].Message = clamp(p.MachineIssues[i].Message, MaxNoteLen)
		p.MachineIssues[i].Since = clamp(p.MachineIssues[i].Since, MaxVersionLen)
	}
	if len(p.MachineIssues) == 0 {
		p.MachineIssues = nil
	}

	// Keep the first MaxNamespaces in lexicographic order: deterministic, so
	// consecutive snapshots of an over-limit cluster agree on which entries
	// survive instead of flapping with map iteration order.
	if len(p.Workloads.Pods.ByNamespace) > MaxNamespaces {
		keys := make([]string, 0, len(p.Workloads.Pods.ByNamespace))
		for k := range p.Workloads.Pods.ByNamespace {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		trimmed := make(map[string]int, MaxNamespaces)
		for _, k := range keys[:MaxNamespaces] {
			trimmed[k] = p.Workloads.Pods.ByNamespace[k]
		}
		p.Workloads.Pods.ByNamespace = trimmed
	}
}

// clamp truncates s to at most n runes (rune-safe, never splits a codepoint).
func clamp(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
