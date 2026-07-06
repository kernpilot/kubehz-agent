// Package inventory reads the lok8s ClusterInventory CR — the deployment
// inventory the USER'S OWN `lo` CLI server-side-applies at the end of
// provision/bootstrap — and threads its spec into the heartbeat as the
// `inventory` block, so the platform can compute addon updates.
//
// Grounding (lok8s operator/crds/clusterinventory.yaml, read 2026-07-06):
//
//   - GVR lok8s.dev/v1alpha1 clusterinventories, CLUSTER-scoped, a singleton
//     named "cluster" (by convention, like OpenShift's ClusterVersion
//     "version"), with a status SUBRESOURCE.
//   - spec is LO-OWNED: lok8sVersion, kind, provider, kubernetesVersion,
//     specHash, renderedAt, addons[{name, chartVersion, appVersion, category,
//     source}]. The agent NEVER writes spec.
//   - status is AGENT-OWNED (via the status subresource): observedAddons,
//     availableUpdates[{name, current, latest}], lastReported. This package
//     writes ONLY availableUpdates + lastReported and never touches
//     observedAddons (a future observer's field).
//
// Polling, not an informer, on purpose: the CR changes only when someone runs
// a lo deploy, so a light periodic GET at the full-beat cadence is the whole
// freshness requirement — while an informer on a possibly-ABSENT CRD would
// keep a reflector error-looping on every registered-tier cluster that was
// never lok8s-deployed. Fail-soft like the machines surface: no CRD, no CR,
// or no RBAC → no inventory block, zero noise.
package inventory

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// GVR is the ClusterInventory resource (lok8s.dev/v1alpha1, cluster-scoped).
var GVR = schema.GroupVersionResource{
	Group:    "lok8s.dev",
	Version:  "v1alpha1",
	Resource: "clusterinventories",
}

// CRName is the conventional singleton name `lo` writes.
const CRName = "cluster"

// specInventory maps ClusterInventory.spec → the heartbeat inventory block,
// field for field. Returns nil when the object has no readable spec (a CR
// without spec cannot exist past CRD validation — required — so nil here
// means "not a ClusterInventory shape at all").
func specInventory(u *unstructured.Unstructured) *state.Inventory {
	spec, found, err := unstructured.NestedMap(u.Object, "spec")
	if !found || err != nil || len(spec) == 0 {
		return nil
	}
	inv := &state.Inventory{
		Lok8sVersion:      nestedString(spec, "lok8sVersion"),
		Kind:              nestedString(spec, "kind"),
		Provider:          nestedString(spec, "provider"),
		KubernetesVersion: nestedString(spec, "kubernetesVersion"),
		SpecHash:          nestedString(spec, "specHash"),
		RenderedAt:        nestedString(spec, "renderedAt"),
	}
	addons, found, err := unstructured.NestedSlice(u.Object, "spec", "addons")
	if !found || err != nil {
		return inv
	}
	for _, raw := range addons {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name := nestedString(entry, "name")
		if name == "" {
			continue // name is required by the CRD; skip malformed entries
		}
		inv.Addons = append(inv.Addons, state.Addon{
			Name:         name,
			ChartVersion: nestedString(entry, "chartVersion"),
			AppVersion:   nestedString(entry, "appVersion"),
			Category:     nestedString(entry, "category"),
			Source:       nestedString(entry, "source"),
		})
	}
	return inv
}

// statusAvailableUpdates reads the CR's CURRENT status.availableUpdates — the
// baseline the idempotence check compares against, so neither an agent
// restart nor a repeat server response ever re-writes an unchanged status.
func statusAvailableUpdates(u *unstructured.Unstructured) []state.AvailableUpdate {
	raw, found, err := unstructured.NestedSlice(u.Object, "status", "availableUpdates")
	if !found || err != nil {
		return nil
	}
	var updates []state.AvailableUpdate
	for _, item := range raw {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := nestedString(entry, "name")
		if name == "" {
			continue
		}
		updates = append(updates, state.AvailableUpdate{
			Name:    name,
			Current: nestedString(entry, "current"),
			Latest:  nestedString(entry, "latest"),
		})
	}
	return updates
}

// nestedString reads a top-level string key from an unstructured map ("" when
// absent or not a string).
func nestedString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
