package collector

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pod(ns string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "p"},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func TestSummarizePods_PhaseCounts(t *testing.T) {
	pods := []*corev1.Pod{
		pod("kube-system", corev1.PodRunning),
		pod("kube-system", corev1.PodRunning),
		pod("default", corev1.PodPending),
		pod("default", corev1.PodFailed),
		pod("batch", corev1.PodSucceeded),
		pod("batch", corev1.PodPhase("")), // unknown/empty
		nil,                               // must be skipped
	}
	got := SummarizePods(pods, false)
	if got.Total != 6 {
		t.Errorf("total = %d, want 6", got.Total)
	}
	if got.Running != 2 || got.Pending != 1 || got.Failed != 1 || got.Succeeded != 1 || got.Unknown != 1 {
		t.Errorf("phase counts wrong: %+v", got)
	}
	// Privacy default: no per-namespace breakdown.
	if got.ByNamespace != nil {
		t.Errorf("ByNamespace must be nil when reportNamespaces=false, got %v", got.ByNamespace)
	}
}

func TestSummarizePods_NamespaceOptIn(t *testing.T) {
	pods := []*corev1.Pod{
		pod("kube-system", corev1.PodRunning),
		pod("kube-system", corev1.PodRunning),
		pod("default", corev1.PodRunning),
	}
	got := SummarizePods(pods, true)
	if got.ByNamespace == nil {
		t.Fatal("ByNamespace must be populated when reportNamespaces=true")
	}
	if got.ByNamespace["kube-system"] != 2 || got.ByNamespace["default"] != 1 {
		t.Errorf("namespace counts wrong: %v", got.ByNamespace)
	}
}

func TestSummarizePods_Empty(t *testing.T) {
	got := SummarizePods(nil, true)
	if got.Total != 0 || got.ByNamespace != nil {
		t.Errorf("empty summary wrong: %+v", got)
	}
}
