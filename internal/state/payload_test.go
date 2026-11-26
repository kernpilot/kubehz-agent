package state

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
)

func TestApplyCaps_TruncatesArraysAndStrings(t *testing.T) {
	p := &Payload{
		ClusterID:  strings.Repeat("d", MaxNameLen+50),
		Timestamp:  strings.Repeat("t", MaxVersionLen+10),
		Kubernetes: KubeInfo{Version: strings.Repeat("v", MaxVersionLen+10)},
		Agent:      AgentMeta{Version: strings.Repeat("a", MaxTypeLen+10), Mode: ModeOperator},
	}
	for i := 0; i < MaxNodes+25; i++ {
		p.Nodes = append(p.Nodes, NodeState{
			Name:   strings.Repeat("n", MaxNameLen+5),
			Status: strings.Repeat("s", MaxStatusLen+5),
			Roles:  strings.Repeat("r", MaxRolesLen+5),
		})
	}
	for i := 0; i < MaxComponents+10; i++ {
		p.Components = append(p.Components, Component{Name: "c", Status: "ok"})
	}
	for i := 0; i < MaxEvents+10; i++ {
		p.Events = append(p.Events, EventState{Reason: "r", Kind: "Pod", Note: strings.Repeat("x", MaxNoteLen+50)})
	}

	ApplyCaps(p)

	if len(p.ClusterID) > MaxNameLen {
		t.Errorf("clusterID not clamped: %d", len(p.ClusterID))
	}
	if len(p.Timestamp) > MaxVersionLen {
		t.Errorf("timestamp not clamped: %d", len(p.Timestamp))
	}
	if len(p.Kubernetes.Version) > MaxVersionLen {
		t.Errorf("version not clamped: %d", len(p.Kubernetes.Version))
	}
	if len(p.Agent.Version) > MaxTypeLen {
		t.Errorf("agent version not clamped: %d", len(p.Agent.Version))
	}
	if len(p.Nodes) != MaxNodes {
		t.Errorf("nodes not truncated to %d, got %d", MaxNodes, len(p.Nodes))
	}
	for _, n := range p.Nodes {
		if len(n.Name) > MaxNameLen || len(n.Status) > MaxStatusLen || len(n.Roles) > MaxRolesLen {
			t.Errorf("node field not clamped: %+v", n)
			break
		}
	}
	if len(p.Components) != MaxComponents {
		t.Errorf("components not truncated: %d", len(p.Components))
	}
	if len(p.Events) != MaxEvents {
		t.Errorf("events not truncated: %d", len(p.Events))
	}
	for _, e := range p.Events {
		if len(e.Note) > MaxNoteLen {
			t.Errorf("event note not clamped: %d", len(e.Note))
			break
		}
	}
}

// The opt-in byNamespace map grows with the number of namespaces, and the api
// refuses the whole beat over its bound. The cap must be deterministic
// (lexicographic keep-set, stable across consecutive snapshots), and each kept
// name keeps its own count.
func TestApplyCaps_BoundsByNamespace(t *testing.T) {
	p := &Payload{}
	p.Workloads.Pods.ByNamespace = map[string]int{}
	for i := 0; i < MaxNamespaces+25; i++ {
		p.Workloads.Pods.ByNamespace[fmt.Sprintf("ns-%04d", i)] = 3*i + 1
	}
	in := maps.Clone(p.Workloads.Pods.ByNamespace)
	ApplyCaps(p)
	if got := len(p.Workloads.Pods.ByNamespace); got != MaxNamespaces {
		t.Fatalf("byNamespace not capped: %d, want %d", got, MaxNamespaces)
	}
	for k, v := range p.Workloads.Pods.ByNamespace {
		if v != in[k] {
			t.Errorf("%s: count %d after the cap, want its own count %d", k, v, in[k])
		}
	}
	// Lexicographic keep-set: the lowest keys survive, the overflow is dropped.
	if _, ok := p.Workloads.Pods.ByNamespace["ns-0000"]; !ok {
		t.Errorf("lowest key evicted — cap is not deterministic")
	}
	if _, ok := p.Workloads.Pods.ByNamespace[fmt.Sprintf("ns-%04d", MaxNamespaces)]; ok {
		t.Errorf("overflow key survived the cap")
	}
	// A within-limit map passes through untouched.
	small := &Payload{}
	small.Workloads.Pods.ByNamespace = map[string]int{"default": 3}
	ApplyCaps(small)
	if small.Workloads.Pods.ByNamespace["default"] != 3 || len(small.Workloads.Pods.ByNamespace) != 1 {
		t.Errorf("within-limit map modified: %+v", small.Workloads.Pods.ByNamespace)
	}
}

// TestCapsMirrorAPIBounds pins every cap to the literal bound of the kubehz
// api (server/utils/validation.ts, HeartbeatSchema). The api refuses the
// WHOLE beat past each bound. The other cap tests compare with the agent's
// own constants, so they still pass when a constant drifts from the api.
//
// The api also publishes the schema: GET /api/openapi (server/api/openapi.get.ts,
// components.schemas.Heartbeat) carries maxLength, maxItems and maximum for
// every field that the agent sends, but not for byNamespace. Its bound is a
// zod refine, so the document has no maxProperties for it. This hand copy
// exists because of that gap.
func TestCapsMirrorAPIBounds(t *testing.T) {
	for _, c := range []struct {
		name    string
		got     int
		apiName string
		api     int
	}{
		{"MaxNodes", MaxNodes, "HB_MAX_NODES", 500},
		{"MaxComponents", MaxComponents, "HB_MAX_COMPONENTS", 50},
		{"MaxEvents", MaxEvents, "HB_MAX_EVENTS", 50},
		{"MaxActions", MaxActions, "HB_MAX_ACTIONS", 20},
		{"MaxMachineIssues", MaxMachineIssues, "HB_MAX_MACHINE_ISSUES", 20},
		{"MaxAddons", MaxAddons, "HB_MAX_INVENTORY_ADDONS", 100},
		{"MaxNamespaces", MaxNamespaces, "HB_MAX_NAMESPACES", 100},
		{"MaxNameLen", MaxNameLen, "HB_MAX_NAME_LEN", 253},
		{"MaxStatusLen", MaxStatusLen, "HB_MAX_STATUS_LEN", 63},
		{"MaxRolesLen", MaxRolesLen, "HB_MAX_ROLES_LEN", 253},
		{"MaxTypeLen", MaxTypeLen, "HB_MAX_TYPE_LEN", 64},
		{"MaxVersionLen", MaxVersionLen, "HB_MAX_VERSION_LEN", 64},
		{"MaxNoteLen", MaxNoteLen, "HB_MAX_NOTE_LEN", 256},
		{"MaxDetailLen", MaxDetailLen, "HB_MAX_NOTE_LEN", 256},
		{"MaxCount", MaxCount, "HB_MAX_COUNT", 10_000_000},
		{"MaxRevision", MaxRevision, "HB_MAX_COUNT", 10_000_000},
	} {
		if c.got != c.api {
			t.Errorf("%s = %d, want %d: it mirrors the api bound %s", c.name, c.got, c.api, c.apiName)
		}
	}
}

// The api refuses the WHOLE beat when byNamespace has more than 100 entries
// (HB_MAX_NAMESPACES). So one entry over the bound, and 150 entries, both
// trim to exactly 100. ApplyCaps keeps the first 100 names in byte order,
// and each kept name keeps its own count. Here the 100 "a-" names sort
// before every "b-" name. The counts differ from the index and are not in
// name order: the "b-" names hold the smallest and the largest counts, so a
// keep-set chosen by count, in either direction, keeps a "b-" name.
func TestApplyCaps_ByNamespaceTrimsToTheAPIBound(t *testing.T) {
	const apiBound = 100 // HB_MAX_NAMESPACES
	for _, over := range []int{1, 50} {
		in := map[string]int{}
		for i := 0; i < apiBound; i++ {
			in[fmt.Sprintf("a-%03d", i)] = 1000 + (i*37)%apiBound
		}
		for j := 0; j < over; j++ {
			if j%2 == 0 {
				in[fmt.Sprintf("b-%03d", j)] = j
			} else {
				in[fmt.Sprintf("b-%03d", j)] = 1_000_000 + j
			}
		}
		p := &Payload{}
		p.Workloads.Pods.ByNamespace = maps.Clone(in)
		ApplyCaps(p)
		got := p.Workloads.Pods.ByNamespace
		if len(got) != apiBound {
			t.Errorf("%d namespaces: %d entries after ApplyCaps, want %d", len(in), len(got), apiBound)
		}
		for i := 0; i < apiBound; i++ {
			k := fmt.Sprintf("a-%03d", i)
			if v, ok := got[k]; !ok {
				t.Errorf("%d namespaces: %s (in the first 100 by name) was dropped", len(in), k)
			} else if v != in[k] {
				t.Errorf("%d namespaces: %s has count %d, want its own count %d", len(in), k, v, in[k])
			}
		}
		for k := range got {
			if strings.HasPrefix(k, "b-") {
				t.Errorf("%d namespaces: %s (after the first 100 by name) was kept", len(in), k)
			}
		}
	}
}

// The event writer sets the count. The api accepts only 0..10,000,000
// (HB_MAX_COUNT) and refuses the WHOLE beat outside it.
func TestApplyCaps_ClampsEventCountToTheAPIBound(t *testing.T) {
	const apiMax = 10_000_000 // HB_MAX_COUNT
	counts := []struct{ in, want int32 }{
		{apiMax + 1, apiMax},
		{math.MaxInt32, apiMax},
		{-1, 0},
		{math.MinInt32, 0},
		{apiMax, apiMax},
		{0, 0},
		{7, 7},
	}
	p := &Payload{}
	for _, c := range counts {
		p.Events = append(p.Events, EventState{Reason: "BackOff", Kind: "Pod", Count: c.in})
	}
	ApplyCaps(p)
	for i, c := range counts {
		if got := p.Events[i].Count; got != c.want {
			t.Errorf("event count %d: %d after ApplyCaps, want %d", c.in, got, c.want)
		}
	}
}

// The api refuses the WHOLE beat when a node capacity cpu or memory string
// is longer than 64 (HB_MAX_TYPE_LEN). apimachinery caps only binary-SI
// quantities, so a decimal capacity in a node status can be longer. ApplyCaps
// omits such a value, because a cut one is a wrong number. The over-bound
// values sit on later nodes and end in a unit suffix, so a cut from either
// end, or a check of node 0 only, shows here.
func TestApplyCaps_OmitsAnOverBoundNodeCapacity(t *testing.T) {
	const apiMax = 64 // HB_MAX_TYPE_LEN
	digits := strings.Repeat("9", 70)
	atBound := strings.Repeat("1", apiMax-2) + "Gi"
	overByOne := strings.Repeat("1", apiMax-1) + "Gi"
	p := &Payload{Nodes: []NodeState{
		{Name: "n0", Capacity: &NodeCapacity{CPU: "4", Memory: "8148Mi"}},
		{Name: "n1", Capacity: &NodeCapacity{CPU: digits + "m", Memory: digits + "Gi"}},
		{Name: "n2", Capacity: &NodeCapacity{CPU: "1500m", Memory: digits + "Ki"}},
		{Name: "n3", Capacity: &NodeCapacity{CPU: digits, Memory: "16Gi"}},
		{Name: "n4", Capacity: &NodeCapacity{CPU: atBound, Memory: atBound}},
		{Name: "n5", Capacity: &NodeCapacity{CPU: overByOne, Memory: atBound}},
		{Name: "n6", Capacity: &NodeCapacity{CPU: atBound, Memory: overByOne}},
		{Name: "n7"},
	}}
	ApplyCaps(p)
	want := []*NodeCapacity{
		{CPU: "4", Memory: "8148Mi"},
		nil, // both values over the bound: no capacity at all
		{CPU: "1500m"},
		{Memory: "16Gi"},
		{CPU: atBound, Memory: atBound},
		{Memory: atBound},
		{CPU: atBound},
		nil,
	}
	for i, w := range want {
		got := p.Nodes[i].Capacity
		switch {
		case w == nil && got != nil:
			t.Errorf("%s: capacity %+v after ApplyCaps, want none", p.Nodes[i].Name, *got)
		case w != nil && got == nil:
			t.Errorf("%s: no capacity after ApplyCaps, want %+v", p.Nodes[i].Name, *w)
		case w != nil && *got != *w:
			t.Errorf("%s: capacity %+v after ApplyCaps, want %+v", p.Nodes[i].Name, *got, *w)
		}
	}
	// An omitted value is absent on the wire, not an empty string.
	b, err := json.Marshal(p.Nodes[2])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"memory"`) {
		t.Errorf("an omitted memory value is still on the wire: %s", b)
	}
}

// Actions must be capped to the server's HB_MAX_ACTIONS, every field clamped,
// and an empty-target entry dropped — one such entry would 400 the WHOLE beat
// (the server requires target 1..253).
func TestApplyCaps_BoundsActions(t *testing.T) {
	p := &Payload{}
	for i := 0; i < MaxActions+5; i++ {
		p.Actions = append(p.Actions, Action{
			Type:     ActionScale,
			Target:   strings.Repeat("p", MaxNameLen+7),
			Status:   ActionDone,
			Detail:   strings.Repeat("d", MaxDetailLen+50),
			Revision: MaxRevision + 10,
		})
	}
	// Poisoned entries: empty target (server min 1) and a negative revision.
	p.Actions[0].Target = ""
	p.Actions[1].Revision = -3

	ApplyCaps(p)

	if len(p.Actions) != MaxActions {
		t.Fatalf("actions not capped: %d, want %d", len(p.Actions), MaxActions)
	}
	for _, a := range p.Actions {
		if a.Target == "" {
			t.Errorf("empty-target action survived (would 400 the beat)")
		}
		if len(a.Target) > MaxNameLen || len(a.Detail) > MaxDetailLen {
			t.Errorf("action strings not clamped: %+v", a)
		}
		if a.Revision < 0 || a.Revision > MaxRevision {
			t.Errorf("revision out of server bounds: %d", a.Revision)
		}
	}

	// An all-invalid list must become nil so omitempty drops the key — the
	// server's "clear actions" signal, not a literal empty array.
	empty := &Payload{Actions: []Action{{Type: ActionScale, Target: "", Status: ActionDone}}}
	ApplyCaps(empty)
	if empty.Actions != nil {
		t.Errorf("emptied actions should be nil for omitempty, got %#v", empty.Actions)
	}
}

func TestApplyCaps_BoundsMachineIssues(t *testing.T) {
	p := &Payload{}
	for i := 0; i < MaxMachineIssues+5; i++ {
		p.MachineIssues = append(p.MachineIssues, MachineIssue{
			Pool:    strings.Repeat("p", MaxStatusLen+7),
			Machine: strings.Repeat("m", MaxNameLen+7),
			Reason:  strings.Repeat("r", MaxStatusLen+7),
			Message: strings.Repeat("x", MaxNoteLen+50),
			Since:   strings.Repeat("t", MaxVersionLen+7),
		})
	}
	// Poisoned entries: the server's zod requires pool and reason min(1).
	p.MachineIssues[0].Pool = ""
	p.MachineIssues[1].Reason = ""

	ApplyCaps(p)

	if len(p.MachineIssues) != MaxMachineIssues {
		t.Fatalf("machineIssues not capped: %d, want %d", len(p.MachineIssues), MaxMachineIssues)
	}
	for _, mi := range p.MachineIssues {
		if mi.Pool == "" || mi.Reason == "" {
			t.Errorf("empty pool/reason survived (would 400 the beat): %+v", mi)
		}
		if len(mi.Pool) > MaxStatusLen || len(mi.Machine) > MaxNameLen ||
			len(mi.Reason) > MaxStatusLen || len(mi.Message) > MaxNoteLen ||
			len(mi.Since) > MaxVersionLen {
			t.Errorf("machineIssue strings not clamped: %+v", mi)
		}
	}

	// An all-invalid list must become nil so omitempty drops the key — the
	// server's "clear machineIssues" signal, not a literal empty array.
	empty := &Payload{MachineIssues: []MachineIssue{{Pool: "", Reason: "x"}}}
	ApplyCaps(empty)
	if empty.MachineIssues != nil {
		t.Errorf("emptied machineIssues should be nil for omitempty, got %#v", empty.MachineIssues)
	}
}

// machineIssues serialize under the exact wire names the server's zod schema
// expects; an EMPTY machine must omit the key (zod machine is optional min(1)
// — an empty string would 400), and absent machineIssues omit the key entirely.
func TestPayload_MachineIssuesWireShape(t *testing.T) {
	p := &Payload{
		MachineIssues: []MachineIssue{{
			Pool:    "pool-a",
			Reason:  "ReconcilingError",
			Message: "unsupported location for server type",
			Since:   "2026-07-06T12:00:00Z",
		}},
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"machineIssues":[`, `"pool":"pool-a"`, `"reason":"ReconcilingError"`,
		`"message":"unsupported location for server type"`, `"since":"2026-07-06T12:00:00Z"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("machineIssues JSON missing %s\ngot: %s", want, s)
		}
	}
	if strings.Contains(s, `"machine"`) {
		t.Errorf("empty machine key must be omitted (zod min(1)): %s", s)
	}

	none := &Payload{}
	b, _ = json.Marshal(none)
	if strings.Contains(string(b), "machineIssues") {
		t.Errorf("absent machineIssues must omit the key: %s", b)
	}
}

// Actions serialize under the exact wire names + enum values the server's zod
// schema expects; absent actions must omit the key entirely (the clear signal).
func TestPayload_ActionsWireShape(t *testing.T) {
	p := &Payload{
		Actions: []Action{{
			Type: ActionScale, Target: "pool-a", Status: ActionInProgress,
			Detail: "replicas 2 to 3", Revision: 7,
		}},
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"actions":[{`, `"type":"scale"`, `"target":"pool-a"`,
		`"status":"in-progress"`, `"detail":"replicas 2 to 3"`, `"revision":7`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("actions JSON missing %s\ngot: %s", want, s)
		}
	}

	bare, _ := json.Marshal(&Payload{})
	if strings.Contains(string(bare), "actions") {
		t.Errorf("empty payload must OMIT actions (server clear signal): %s", bare)
	}
}

func TestApplyCaps_Idempotent(t *testing.T) {
	p := &Payload{ClusterID: "cluster.example.com", Nodes: []NodeState{{Name: "n1"}}}
	ApplyCaps(p)
	first, _ := json.Marshal(p)
	ApplyCaps(p)
	second, _ := json.Marshal(p)
	if string(first) != string(second) {
		t.Errorf("ApplyCaps not idempotent:\n%s\n%s", first, second)
	}
}

func TestClamp_RuneSafe(t *testing.T) {
	// A multi-byte string clamped mid-way must not split a codepoint.
	s := strings.Repeat("é", 100) // 2 bytes each
	got := clamp(s, 10)
	if len([]rune(got)) != 10 {
		t.Errorf("expected 10 runes, got %d", len([]rune(got)))
	}
	if !json.Valid(mustJSON(got)) {
		t.Errorf("clamped string is not valid when JSON-encoded")
	}
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

// The payload must serialize the schema-1 base fields under their exact wire
// names so the live (non-strict) HeartbeatSchema persists them.
func TestPayload_WireNames(t *testing.T) {
	p := &Payload{
		Schema:     SchemaVersion,
		ClusterID:  "cluster.example.com",
		Timestamp:  "2026-07-05T00:00:00Z",
		Agent:      AgentMeta{Version: "0.1.0", Mode: ModeOperator},
		Kubernetes: KubeInfo{Version: "v1.35.5"},
		Nodes: []NodeState{{
			Name: "cp-1", Status: "Ready", Ready: true, Roles: "control-plane",
			InstanceType: "cx32", KubeletVersion: "v1.35.5",
			Capacity: &NodeCapacity{CPU: "4", Memory: "8148Mi"},
		}},
		Workloads: Workloads{Pods: PodSummary{Total: 10, Running: 9, Pending: 1}},
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"schema":2`, `"clusterId":"cluster.example.com"`, `"kubernetes":{"version":"v1.35.5"}`,
		`"instanceType":"cx32"`, `"roles":"control-plane"`, `"ready":true`,
		`"kubeletVersion":"v1.35.5"`, `"mode":"operator"`, `"workloads"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("payload JSON missing %s\ngot: %s", want, s)
		}
	}
	// Privacy: with no namespaces/events set, those keys must be absent.
	if strings.Contains(s, "byNamespace") || strings.Contains(s, `"events"`) {
		t.Errorf("privacy-gated fields leaked into empty payload: %s", s)
	}
}

// TestApplyCaps_BoundsInventory: the inventory block is clamped to the
// ClusterInventory CRD's own bounds (a real CR can never exceed them — this
// defangs a hand-rolled/corrupt one), and a nil block stays nil.
func TestApplyCaps_BoundsInventory(t *testing.T) {
	p := &Payload{Inventory: &Inventory{
		Lok8sVersion:      strings.Repeat("v", MaxVersionLen+5),
		Kind:              strings.Repeat("k", MaxStatusLen+5),
		Provider:          strings.Repeat("p", MaxStatusLen+5),
		KubernetesVersion: strings.Repeat("K", MaxVersionLen+5),
		SpecHash:          strings.Repeat("h", MaxVersionLen+5),
		RenderedAt:        strings.Repeat("r", MaxVersionLen+5),
	}}
	for i := 0; i < MaxAddons+10; i++ {
		p.Inventory.Addons = append(p.Inventory.Addons, Addon{
			Name:         strings.Repeat("n", MaxNameLen+5),
			ChartVersion: strings.Repeat("c", MaxVersionLen+5),
			AppVersion:   strings.Repeat("a", MaxVersionLen+5),
			Category:     strings.Repeat("g", MaxStatusLen+5),
			Source:       strings.Repeat("s", MaxStatusLen+5),
		})
	}
	ApplyCaps(p)

	inv := p.Inventory
	if len(inv.Lok8sVersion) != MaxVersionLen || len(inv.Kind) != MaxStatusLen ||
		len(inv.Provider) != MaxStatusLen || len(inv.KubernetesVersion) != MaxVersionLen ||
		len(inv.SpecHash) != MaxVersionLen || len(inv.RenderedAt) != MaxVersionLen {
		t.Errorf("inventory scalar fields not clamped: %+v", inv)
	}
	if len(inv.Addons) != MaxAddons {
		t.Errorf("addons = %d, want %d", len(inv.Addons), MaxAddons)
	}
	a := inv.Addons[0]
	if len(a.Name) != MaxNameLen || len(a.ChartVersion) != MaxVersionLen ||
		len(a.AppVersion) != MaxVersionLen || len(a.Category) != MaxStatusLen ||
		len(a.Source) != MaxStatusLen {
		t.Errorf("addon fields not clamped: %+v", a)
	}

	empty := &Payload{}
	ApplyCaps(empty)
	if empty.Inventory != nil {
		t.Error("nil inventory must stay nil")
	}
}

// TestPayload_InventoryWireShape pins the inventory block to the CR's exact
// field names (spec: lok8sVersion/kind/provider/kubernetesVersion/specHash/
// renderedAt/addons[{name,chartVersion,appVersion,category,source}]) and to
// omitempty semantics: no CR → no key at all.
func TestPayload_InventoryWireShape(t *testing.T) {
	p := &Payload{Inventory: &Inventory{
		Lok8sVersion:      "1.4.2",
		Kind:              "kubeone",
		Provider:          "hetzner",
		KubernetesVersion: "v1.35.5",
		SpecHash:          strings.Repeat("a", 64),
		RenderedAt:        "2026-07-06T10:00:00Z",
		Addons: []Addon{{
			Name: "cilium", ChartVersion: "1.16.1", AppVersion: "1.16.1",
			Category: "networking", Source: "addon",
		}},
	}}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"inventory":{`, `"lok8sVersion":"1.4.2"`, `"kind":"kubeone"`,
		`"provider":"hetzner"`, `"kubernetesVersion":"v1.35.5"`,
		fmt.Sprintf(`"specHash":"%s"`, strings.Repeat("a", 64)),
		`"renderedAt":"2026-07-06T10:00:00Z"`,
		`"addons":[{"name":"cilium","chartVersion":"1.16.1","appVersion":"1.16.1","category":"networking","source":"addon"}]`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("payload JSON missing %s\ngot: %s", want, s)
		}
	}

	if b, err = json.Marshal(&Payload{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "inventory") {
		t.Errorf("absent CR must serialize with NO inventory key, got: %s", b)
	}
}
