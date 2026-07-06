package machines

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func machine(name string, labels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "Machine",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "kube-system",
		},
	}}
	if labels != nil {
		u.SetLabels(labels)
	}
	return u
}

func md(name string, matchLabels map[string]string) unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "MachineDeployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "kube-system",
		},
		"spec": map[string]any{},
	}
	if matchLabels != nil {
		ml := map[string]any{}
		for k, v := range matchLabels {
			ml[k] = v
		}
		obj["spec"].(map[string]any)["selector"] = map[string]any{"matchLabels": ml}
	}
	return unstructured.Unstructured{Object: obj}
}

// The primary resolution path is the MD's spec.selector matched against the
// machine's labels — exactly how the MD controller claims machines.
func TestPoolResolver_SelectorMatch(t *testing.T) {
	r := NewPoolResolver([]unstructured.Unstructured{
		md("pool-a", map[string]string{"workerset": "pool-a"}),
		md("pool-b", map[string]string{"workerset": "pool-b"}),
	})

	pool, mdObj := r.PoolFor(machine("pool-a-abc123-x9k2p", map[string]string{"workerset": "pool-a"}))
	if pool != "pool-a" || mdObj == nil {
		t.Fatalf("selector match: got pool %q md %v", pool, mdObj)
	}
	pool, _ = r.PoolFor(machine("whatever", map[string]string{"workerset": "pool-b"}))
	if pool != "pool-b" {
		t.Fatalf("selector match: got pool %q, want pool-b", pool)
	}
}

// An MD with an EMPTY selector must never claim machines — labels.Everything()
// would own the whole namespace, which is ownership fiction.
func TestPoolResolver_EmptySelectorClaimsNothing(t *testing.T) {
	r := NewPoolResolver([]unstructured.Unstructured{md("pool-a", nil)})
	if pool, _ := r.PoolFor(machine("stray", map[string]string{"x": "y"})); pool != "" {
		t.Fatalf("empty selector claimed machine into pool %q", pool)
	}
}

// Fallback: ownerRef name shape ("<md>-<hash>" MachineSet), accepted only when
// the derived MD actually exists.
func TestPoolResolver_OwnerRefFallback(t *testing.T) {
	r := NewPoolResolver([]unstructured.Unstructured{
		md("pool-a", map[string]string{"workerset": "pool-a"}),
	})

	m := machine("pool-a-6d8f9b7c4-xk2lp", nil) // no labels → selector misses
	m.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "cluster.k8s.io/v1alpha1", Kind: "MachineSet", Name: "pool-a-6d8f9b7c4",
	}})
	pool, mdObj := r.PoolFor(m)
	if pool != "pool-a" || mdObj == nil {
		t.Fatalf("ownerRef fallback: got pool %q", pool)
	}

	// A MachineSet name whose derived MD does NOT exist must resolve to "".
	m2 := machine("ghost-1a2b3c-zzz", nil)
	m2.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "cluster.k8s.io/v1alpha1", Kind: "MachineSet", Name: "ghost-1a2b3c",
	}})
	if pool, _ := r.PoolFor(m2); pool != "" {
		t.Fatalf("unverified ownerRef fallback resolved to %q, want unresolved", pool)
	}
}

// Field accessors read the exact machine-controller v1.65.0 CRD paths.
func TestFieldAccessors(t *testing.T) {
	m := machine("m1", nil)
	if got := ErrorReason(m); got != "" {
		t.Fatalf("ErrorReason on clean machine = %q", got)
	}
	m.Object["status"] = map[string]any{
		"errorReason":  "InvalidConfiguration",
		"errorMessage": "unsupported location for server type",
		"nodeRef":      map[string]any{"kind": "Node", "name": "worker-1"},
	}
	m.Object["spec"] = map[string]any{
		"versions": map[string]any{"kubelet": "v1.34.9"},
	}
	if got := ErrorReason(m); got != "InvalidConfiguration" {
		t.Errorf("ErrorReason = %q", got)
	}
	if got := ErrorMessage(m); got != "unsupported location for server type" {
		t.Errorf("ErrorMessage = %q", got)
	}
	if got := NodeRefName(m); got != "worker-1" {
		t.Errorf("NodeRefName = %q", got)
	}
	if got := MachineKubeletVersion(m); got != "v1.34.9" {
		t.Errorf("MachineKubeletVersion = %q", got)
	}

	d := md("pool-a", nil)
	if got := MDKubeletVersion(&d); got != "" {
		t.Errorf("MDKubeletVersion on bare MD = %q", got)
	}
	d.Object["spec"].(map[string]any)["template"] = map[string]any{
		"spec": map[string]any{"versions": map[string]any{"kubelet": "v1.35.6"}},
	}
	if got := MDKubeletVersion(&d); got != "v1.35.6" {
		t.Errorf("MDKubeletVersion = %q", got)
	}
}

func TestDeletingAndAge(t *testing.T) {
	m := machine("m1", nil)
	if Deleting(m) {
		t.Fatal("fresh machine reported deleting")
	}
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	if got := Age(m, now); got != 0 {
		t.Fatalf("Age without creationTimestamp = %s, want 0", got)
	}
	created := metav1.NewTime(now.Add(-15 * time.Minute))
	m.SetCreationTimestamp(created)
	if got := Age(m, now); got != 15*time.Minute {
		t.Fatalf("Age = %s, want 15m", got)
	}
	del := metav1.NewTime(now)
	m.SetDeletionTimestamp(&del)
	if !Deleting(m) {
		t.Fatal("machine with deletionTimestamp not reported deleting")
	}
}
