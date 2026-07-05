package collector

import (
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// fakeSource is an in-memory Source for testing BuildPayload without a cluster.
type fakeSource struct {
	nodes    []*corev1.Node
	pods     []*corev1.Pod
	events   []*corev1.Event
	nodesErr error
}

func (f fakeSource) Nodes() ([]*corev1.Node, error)   { return f.nodes, f.nodesErr }
func (f fakeSource) Pods() ([]*corev1.Pod, error)     { return f.pods, nil }
func (f fakeSource) Events() ([]*corev1.Event, error) { return f.events, nil }

func TestBuildPayload_FullSnapshot(t *testing.T) {
	fixed := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)
	src := fakeSource{
		nodes: []*corev1.Node{{
			ObjectMeta: metav1.ObjectMeta{Name: "cp-1", Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}},
			Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.35.5"}},
		}},
		pods:   []*corev1.Pod{{Status: corev1.PodStatus{Phase: corev1.PodRunning}}},
		events: []*corev1.Event{warnEvent("e", "BackOff", "Pod", "d", "m", 1, fixed)},
	}
	p := BuildPayload(src, Meta{
		ClusterID:     "kubehz.in.net",
		ServerVersion: "v1.35.5",
		AgentVersion:  "0.1.0",
		Now:           func() time.Time { return fixed },
	})

	if p.Schema != state.SchemaVersion {
		t.Errorf("schema = %d", p.Schema)
	}
	if p.ClusterID != "kubehz.in.net" || p.Kubernetes.Version != "v1.35.5" {
		t.Errorf("meta wrong: %+v", p)
	}
	if p.Agent.Mode != state.ModeOperator {
		t.Errorf("mode = %q, want operator", p.Agent.Mode)
	}
	if p.Timestamp != fixed.Format(time.RFC3339) {
		t.Errorf("timestamp = %q", p.Timestamp)
	}
	if len(p.Nodes) != 1 || p.Nodes[0].Roles != "control-plane" {
		t.Errorf("nodes wrong: %+v", p.Nodes)
	}
	if p.Workloads.Pods.Running != 1 || p.Workloads.Pods.Total != 1 {
		t.Errorf("pods summary wrong: %+v", p.Workloads.Pods)
	}
	if len(p.Events) != 1 {
		t.Errorf("events wrong: %+v", p.Events)
	}
}

// A lister error must degrade that section to empty, not abort the snapshot
// (fail toward report-only).
func TestBuildPayload_PartialReadFailSoft(t *testing.T) {
	src := fakeSource{
		nodesErr: errors.New("informer not synced"),
		pods:     []*corev1.Pod{{Status: corev1.PodStatus{Phase: corev1.PodRunning}}},
	}
	p := BuildPayload(src, Meta{ClusterID: "d"})
	if len(p.Nodes) != 0 {
		t.Errorf("nodes should be empty on lister error, got %+v", p.Nodes)
	}
	if p.Workloads.Pods.Total != 1 {
		t.Errorf("pods should still be summarized: %+v", p.Workloads.Pods)
	}
}
