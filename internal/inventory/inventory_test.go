package inventory

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// specHash64 is a valid sha256-hex placeholder (the CRD pattern requires
// exactly 64 hex chars).
var specHash64 = strings.Repeat("ab", 32)

// inventoryCR is a fixture mirroring what `lo` writes (all spec fields set).
func inventoryCR() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "lok8s.dev/v1alpha1",
		"kind":       "ClusterInventory",
		"metadata":   map[string]any{"name": CRName},
		"spec": map[string]any{
			"lok8sVersion":      "1.4.2",
			"kind":              "kubeone",
			"provider":          "hetzner",
			"kubernetesVersion": "v1.35.5",
			"specHash":          specHash64,
			"renderedAt":        "2026-07-06T10:00:00Z",
			"addons": []any{
				map[string]any{
					"name":         "cilium",
					"chartVersion": "1.16.1",
					"appVersion":   "1.16.1",
					"category":     "networking",
					"source":       "addon",
				},
				map[string]any{"name": "rook-ceph", "chartVersion": "1.20.2", "category": "storage", "source": "addon"},
				map[string]any{"name": "site-glue", "source": "target"},
			},
		},
	}}
}

// TestSpecInventory_FieldMapping pins the CR→payload mapping to the exact CRD
// field names (operator/crds/clusterinventory.yaml): a rename on either side
// must break this test.
func TestSpecInventory_FieldMapping(t *testing.T) {
	inv := specInventory(inventoryCR())
	if inv == nil {
		t.Fatal("specInventory returned nil for a full CR")
	}
	if inv.Lok8sVersion != "1.4.2" {
		t.Errorf("lok8sVersion = %q, want 1.4.2", inv.Lok8sVersion)
	}
	if inv.Kind != "kubeone" {
		t.Errorf("kind = %q, want kubeone", inv.Kind)
	}
	if inv.Provider != "hetzner" {
		t.Errorf("provider = %q, want hetzner", inv.Provider)
	}
	if inv.KubernetesVersion != "v1.35.5" {
		t.Errorf("kubernetesVersion = %q, want v1.35.5", inv.KubernetesVersion)
	}
	if inv.SpecHash != specHash64 {
		t.Errorf("specHash = %q, want %q", inv.SpecHash, specHash64)
	}
	if inv.RenderedAt != "2026-07-06T10:00:00Z" {
		t.Errorf("renderedAt = %q", inv.RenderedAt)
	}
	if len(inv.Addons) != 3 {
		t.Fatalf("addons = %d entries, want 3", len(inv.Addons))
	}
	first := inv.Addons[0]
	if first.Name != "cilium" || first.ChartVersion != "1.16.1" || first.AppVersion != "1.16.1" ||
		first.Category != "networking" || first.Source != "addon" {
		t.Errorf("addon[0] mismapped: %+v", first)
	}
	// Optional fields stay empty (omitempty on the wire), not invented.
	last := inv.Addons[2]
	if last.Name != "site-glue" || last.ChartVersion != "" || last.Category != "" || last.Source != "target" {
		t.Errorf("addon[2] mismapped: %+v", last)
	}
}

// TestSpecInventory_MinimalAndMalformed: a spec with only the required fields
// maps cleanly; addon entries without a name (impossible past CRD validation,
// but cheap to defend) are skipped; a spec-less object yields nil.
func TestSpecInventory_MinimalAndMalformed(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "lok8s.dev/v1alpha1",
		"kind":       "ClusterInventory",
		"metadata":   map[string]any{"name": CRName},
		"spec": map[string]any{
			"lok8sVersion": "dev",
			"kind":         "lo",
			"specHash":     specHash64,
			"renderedAt":   "2026-07-06T10:00:00Z",
			"addons": []any{
				map[string]any{"chartVersion": "9.9.9"}, // no name → skipped
				"not-a-map",                             // wrong shape → skipped
				map[string]any{"name": "cilium"},
			},
		},
	}}
	inv := specInventory(u)
	if inv == nil {
		t.Fatal("specInventory returned nil for a minimal CR")
	}
	if inv.Provider != "" || inv.KubernetesVersion != "" {
		t.Errorf("optional spec fields should stay empty: %+v", inv)
	}
	if len(inv.Addons) != 1 || inv.Addons[0].Name != "cilium" {
		t.Errorf("malformed addons not skipped: %+v", inv.Addons)
	}

	if got := specInventory(&unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "lok8s.dev/v1alpha1", "kind": "ClusterInventory",
	}}); got != nil {
		t.Errorf("spec-less object should map to nil, got %+v", got)
	}
}
