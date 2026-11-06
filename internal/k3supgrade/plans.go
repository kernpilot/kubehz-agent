package k3supgrade

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Names and labels of the objects this executor writes. The admission
// policy (policy.go) checks the same values: change them together.
const (
	// Namespace is where the system-upgrade-controller runs and where it
	// reads Plans. The controller reads Plans of its own namespace only.
	Namespace = "system-upgrade"
	// ServiceAccount is the controller's service account (static, from the
	// maintenance overlay). Each Plan names it.
	ServiceAccount = "system-upgrade"

	// ServerPlanName and AgentPlanName are the two Plans of design 4.3. The
	// names are fixed: one operation runs at a time, and the agent Plan's
	// prepare step names the server Plan.
	ServerPlanName = "k3s-server"
	AgentPlanName  = "k3s-agent"

	// UpgradeImage is the k3s upgrade image of the documented templates
	// (docs.k3s.io/upgrades/automated). The controller adds the tag from
	// spec.version.
	UpgradeImage = "rancher/k3s-upgrade"

	// ControlPlaneLabel selects the server nodes. k3s sets it to "true".
	ControlPlaneLabel = "node-role.kubernetes.io/control-plane"

	// LabelManagedBy marks every object this executor writes.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ManagedBy is the value of LabelManagedBy.
	ManagedBy = "kubehz-agent"
	// LabelOperation names the upgrade operation on each Plan. The
	// controller copies Plan labels to its Jobs and their pods, so the same
	// selector finds all three.
	LabelOperation = "kubehz.cloud/operation-id"
)

// PlanGVR is the system-upgrade-controller Plan resource.
var PlanGVR = schema.GroupVersionResource{Group: "upgrade.cattle.io", Version: "v1", Resource: "plans"}

// ServerPlan is the server Plan of design 4.3 step 3: the documented
// template with an exact version instead of a channel. Concurrency 1 and
// cordon on; a server is never drained (a drain on a one-machine cluster
// has nowhere to move the pods, and a k3s restart keeps them running).
func ServerPlan(operationID, version string) *unstructured.Unstructured {
	return plan(ServerPlanName, operationID, map[string]any{
		"concurrency":        int64(1),
		"cordon":             true,
		"serviceAccountName": ServiceAccount,
		"version":            version,
		"nodeSelector": map[string]any{
			"matchExpressions": []any{
				map[string]any{
					"key":      ControlPlaneLabel,
					"operator": "In",
					"values":   []any{"true"},
				},
			},
		},
		"upgrade": map[string]any{"image": UpgradeImage},
	})
}

// AgentPlan is the agent Plan of design 4.3 step 3: the nodes that are not
// control-plane, after the server Plan. The prepare step is the same image
// with exactly the arguments "prepare <server plan>": it waits until the
// server Plan is complete, so servers upgrade before workers.
func AgentPlan(operationID, version string) *unstructured.Unstructured {
	return plan(AgentPlanName, operationID, map[string]any{
		"concurrency":        int64(1),
		"cordon":             true,
		"serviceAccountName": ServiceAccount,
		"version":            version,
		"nodeSelector": map[string]any{
			"matchExpressions": []any{
				map[string]any{
					"key":      ControlPlaneLabel,
					"operator": "DoesNotExist",
				},
			},
		},
		"prepare": map[string]any{
			"image": UpgradeImage,
			"args":  []any{"prepare", ServerPlanName},
		},
		"upgrade": map[string]any{"image": UpgradeImage},
	})
}

func plan(name, operationID string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": PlanGVR.GroupVersion().String(),
		"kind":       "Plan",
		"metadata": map[string]any{
			"name":      name,
			"namespace": Namespace,
			"labels": map[string]any{
				LabelManagedBy: ManagedBy,
				LabelOperation: operationID,
			},
		},
		"spec": spec,
	}}
}
