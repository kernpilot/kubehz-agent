package collector

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestRolesFromLabels is the regression guard for the bash agent's role bug:
// the empty-valued node-role.kubernetes.io/* keys must be read by KEY presence,
// and a worker (no role label) must NOT be reported as control-plane.
func TestRolesFromLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   []string
	}{
		{
			name:   "control-plane node (empty-valued role key)",
			labels: map[string]string{"node-role.kubernetes.io/control-plane": ""},
			want:   []string{"control-plane"},
		},
		{
			name:   "legacy master label",
			labels: map[string]string{"node-role.kubernetes.io/master": ""},
			want:   []string{"master"},
		},
		{
			name:   "worker: no role label at all -> defaults to worker (NOT control-plane)",
			labels: map[string]string{"node.kubernetes.io/instance-type": "cx32"},
			want:   []string{"worker"},
		},
		{
			name:   "nil labels -> worker",
			labels: nil,
			want:   []string{"worker"},
		},
		{
			name: "multi-role, sorted",
			labels: map[string]string{
				"node-role.kubernetes.io/control-plane": "",
				"node-role.kubernetes.io/etcd":          "",
			},
			want: []string{"control-plane", "etcd"},
		},
		{
			name:   "legacy kubernetes.io/role value label",
			labels: map[string]string{"kubernetes.io/role": "ingress"},
			want:   []string{"ingress"},
		},
		{
			name:   "the exact prefix key with empty suffix is ignored",
			labels: map[string]string{"node-role.kubernetes.io/": ""},
			want:   []string{"worker"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RolesFromLabels(tt.labels)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RolesFromLabels(%v) = %v, want %v", tt.labels, got, tt.want)
			}
		})
	}
}

func TestNodeReadyStatus(t *testing.T) {
	tests := []struct {
		name       string
		conds      []corev1.NodeCondition
		wantReady  bool
		wantStatus string
	}{
		{"ready", []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, true, "Ready"},
		{"notready", []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}, false, "NotReady"},
		{"unknown", []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}, false, "Unknown"},
		{"no ready condition", []corev1.NodeCondition{{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse}}, false, "Unknown"},
		{"empty", nil, false, "Unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready, status := NodeReadyStatus(tt.conds)
			if ready != tt.wantReady || status != tt.wantStatus {
				t.Errorf("got (%t, %q), want (%t, %q)", ready, status, tt.wantReady, tt.wantStatus)
			}
		})
	}
}

// TestNodeStateFromNode proves the whole per-node mapping, including the
// kubelet version coming from the SERVER-reported NodeInfo (not a client tag).
func TestNodeStateFromNode(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cp-1",
			Labels: map[string]string{
				"node-role.kubernetes.io/control-plane": "",
				"node.kubernetes.io/instance-type":      "cx32",
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.35.5"},
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8148Mi"),
			},
		},
	}
	got := NodeStateFromNode(node)
	if got.Name != "cp-1" {
		t.Errorf("name = %q", got.Name)
	}
	if got.Roles != "control-plane" {
		t.Errorf("roles = %q, want control-plane", got.Roles)
	}
	if !got.Ready || got.Status != "Ready" {
		t.Errorf("ready/status = %t/%q", got.Ready, got.Status)
	}
	if got.KubeletVersion != "v1.35.5" {
		t.Errorf("kubeletVersion = %q, want v1.35.5 (server NodeInfo)", got.KubeletVersion)
	}
	if got.InstanceType != "cx32" {
		t.Errorf("instanceType = %q", got.InstanceType)
	}
	if got.Capacity == nil || got.Capacity.CPU != "4" || got.Capacity.Memory != "8148Mi" {
		t.Errorf("capacity = %+v", got.Capacity)
	}
}

func TestNodeStateFromNode_WorkerNoCCM(t *testing.T) {
	// A worker with no role label, no instance-type, no capacity: must be a
	// worker, must not fabricate control-plane, must fail-soft on missing bits.
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "w-1"},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}},
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "v1.35.5"},
		},
	}
	got := NodeStateFromNode(node)
	if got.Roles != "worker" {
		t.Errorf("roles = %q, want worker", got.Roles)
	}
	if got.Ready || got.Status != "NotReady" {
		t.Errorf("expected NotReady, got %t/%q", got.Ready, got.Status)
	}
	if got.InstanceType != "" {
		t.Errorf("instanceType should be empty, got %q", got.InstanceType)
	}
	if got.Capacity != nil {
		t.Errorf("capacity should be nil, got %+v", got.Capacity)
	}
}
