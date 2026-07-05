package collector

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// EventLimit caps how many recent Warning events a snapshot carries.
const EventLimit = 20

// Source supplies the current object sets. It is an interface so the payload
// builder is unit-testable with in-memory fixtures (no informer/cluster).
type Source interface {
	Nodes() ([]*corev1.Node, error)
	Pods() ([]*corev1.Pod, error)
	Events() ([]*corev1.Event, error)
}

// Meta carries the non-object facts a snapshot needs.
type Meta struct {
	ClusterID        string
	ServerVersion    string // discovery /version gitVersion (cluster-level)
	AgentVersion     string
	ReportNamespaces bool
	Now              func() time.Time // injectable clock for tests
}

// BuildPayload assembles the full schema-2 payload from the current object sets.
// It never fails hard on a partial read: a lister error for one resource yields
// an empty section rather than aborting the whole snapshot (fail toward
// report-only, spec §1.3.4). Caps are applied by the caller via state.ApplyCaps.
func BuildPayload(src Source, m Meta) *state.Payload {
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}

	p := &state.Payload{
		Schema:     state.SchemaVersion,
		ClusterID:  m.ClusterID,
		Timestamp:  now().UTC().Format(time.RFC3339),
		Agent:      state.AgentMeta{Version: m.AgentVersion, Mode: state.ModeOperator},
		Kubernetes: state.KubeInfo{Version: m.ServerVersion},
	}

	if nodes, err := src.Nodes(); err == nil {
		p.Nodes = make([]state.NodeState, 0, len(nodes))
		for _, n := range nodes {
			if n == nil {
				continue
			}
			p.Nodes = append(p.Nodes, NodeStateFromNode(n))
		}
	}

	if pods, err := src.Pods(); err == nil {
		p.Workloads = state.Workloads{Pods: SummarizePods(pods, m.ReportNamespaces)}
	}

	if events, err := src.Events(); err == nil {
		p.Events = SummarizeWarningEvents(events, EventLimit, m.ReportNamespaces)
	}

	return p
}

// ListerSource adapts the client-go informer listers to Source.
type ListerSource struct {
	NodeLister  corelisters.NodeLister
	PodLister   corelisters.PodLister
	EventLister corelisters.EventLister
}

func (s ListerSource) Nodes() ([]*corev1.Node, error) {
	return s.NodeLister.List(labels.Everything())
}

func (s ListerSource) Pods() ([]*corev1.Pod, error) {
	return s.PodLister.List(labels.Everything())
}

func (s ListerSource) Events() ([]*corev1.Event, error) {
	return s.EventLister.List(labels.Everything())
}
