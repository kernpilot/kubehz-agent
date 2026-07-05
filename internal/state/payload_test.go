package state

import (
	"encoding/json"
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

func TestApplyCaps_Idempotent(t *testing.T) {
	p := &Payload{ClusterID: "kubehz.in.net", Nodes: []NodeState{{Name: "n1"}}}
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
		ClusterID:  "kubehz.in.net",
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
		`"schema":2`, `"clusterId":"kubehz.in.net"`, `"kubernetes":{"version":"v1.35.5"}`,
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
