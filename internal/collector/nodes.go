// Package collector maps typed Kubernetes objects (from the informer listers)
// into the schema-2 state payload. Every extraction is done against TYPED
// client-go objects — never by string-scraping rendered JSON. This is the
// central reason the agent is written in Go: the bash heartbeat repeatedly got
// node facts wrong by parsing JSON with sed —
//
//   - roles: a greedy match over the labels blob and the empty-valued
//     node-role.kubernetes.io/* keys made EVERY node report "control-plane";
//   - kubelet version: `kubectl version` first-match grabbed the CLIENT tag
//     (the kubectl image version), not the server/node version;
//   - node names: a greedy scrape also matched status.runtimeHandlers[].name
//     ("runc"/"crun"), producing a phantom node.
//
// The functions below take the typed object and read the exact field, so those
// classes of bug cannot recur. They are pure and unit-tested.
package collector

import (
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// Well-known label keys (kubernetes.io) used for role/instance-type extraction.
const (
	labelRolePrefix   = "node-role.kubernetes.io/"
	labelRole         = "kubernetes.io/role"
	labelInstanceType = "node.kubernetes.io/instance-type"
	roleWorker        = "worker"
)

// NodeStateFromNode builds the schema-2 view of a single node from the typed
// object. All facts come from typed fields, never string parsing.
func NodeStateFromNode(n *corev1.Node) state.NodeState {
	ns := state.NodeState{
		Name:           n.Name,
		Roles:          strings.Join(RolesFromLabels(n.Labels), ","),
		InstanceType:   n.Labels[labelInstanceType],
		KubeletVersion: n.Status.NodeInfo.KubeletVersion, // SERVER/node version, per node
	}
	ready, status := NodeReadyStatus(n.Status.Conditions)
	ns.Ready = ready
	ns.Status = status
	if cap := nodeCapacity(n); cap != nil {
		ns.Capacity = cap
	}
	return ns
}

// RolesFromLabels derives the sorted set of node roles from the well-known
// label KEYS. The node-role.kubernetes.io/<role> labels carry an EMPTY value —
// presence of the KEY is the signal (the bash bug: a per-key jsonpath returns
// "" whether the label is present or absent, so its "&& echo control-plane"
// fired for every node). A typed map lookup has no such ambiguity. Falls back
// to the legacy kubernetes.io/role value label; defaults to ["worker"].
func RolesFromLabels(labels map[string]string) []string {
	set := map[string]struct{}{}
	for k := range labels {
		if role := strings.TrimPrefix(k, labelRolePrefix); role != k && role != "" {
			set[role] = struct{}{}
		}
	}
	if v, ok := labels[labelRole]; ok && v != "" {
		set[v] = struct{}{}
	}
	if len(set) == 0 {
		return []string{roleWorker}
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// NodeReadyStatus reads the typed Ready condition. Returns (ready, humanStatus)
// where humanStatus is "Ready" | "NotReady" | "Unknown". The bash agent emitted
// the raw condition value ("True"/"False"); the typed read gives an
// unambiguous boolean plus a display string.
func NodeReadyStatus(conds []corev1.NodeCondition) (bool, string) {
	for _, c := range conds {
		if c.Type != corev1.NodeReady {
			continue
		}
		switch c.Status {
		case corev1.ConditionTrue:
			return true, "Ready"
		case corev1.ConditionFalse:
			return false, "NotReady"
		default:
			return false, "Unknown"
		}
	}
	return false, "Unknown"
}

// nodeCapacity reports the node's advertised CPU/memory capacity as strings
// (the resource.Quantity canonical form, e.g. "4" / "8148Mi"). Missing values
// yield nil so the field is omitted (fail-soft: bare metal / kind / no CCM).
func nodeCapacity(n *corev1.Node) *state.NodeCapacity {
	cap := &state.NodeCapacity{}
	if q, ok := n.Status.Capacity[corev1.ResourceCPU]; ok {
		cap.CPU = q.String()
	}
	if q, ok := n.Status.Capacity[corev1.ResourceMemory]; ok {
		cap.Memory = q.String()
	}
	if cap.CPU == "" && cap.Memory == "" {
		return nil
	}
	return cap
}
