// Package machines is the agent's single source of truth for KubeOne's
// machine-controller API surface (kubermatic machine-controller, LEGACY
// cluster.k8s.io/v1alpha1 group — NOT cluster.x-k8s.io): the GVRs, the exact
// unstructured field paths, and the Machine→pool (owning MachineDeployment)
// resolution used by the machineIssues collector and the P5/P6 executors.
//
// Every field path here is grounded in the REAL CRD types of the version the
// pilot runs (kubermatic/machine-controller v1.65.0,
// sdk/apis/cluster/v1alpha1/{machine,machinedeployment}_types.go):
//
//   - Machine.status.errorReason / .errorMessage — TERMINAL failures only
//     ("manual intervention is required"): InvalidConfiguration,
//     UnsupportedChange, InsufficientResources, CreateError, UpdateError,
//     DeleteError, JoinClusterTimeoutError (sdk/apis/cluster/common/consts.go).
//   - TRANSIENT failures are NOT status fields: the controller records them as
//     Warning events on the Machine (reason "ReconcilingError",
//     pkg/controller/machine/controller.go) — the "webhook accepted it but
//     hcloud keeps rejecting it" retry loop ("unsupported location for server
//     type") lives THERE, which is why issue collection reads events too.
//   - Machine.status.nodeRef — set once the machine's node joined.
//   - Machine.spec.versions.kubelet / MachineDeployment
//     .spec.template.spec.versions.kubelet — the kubelet version contract
//     (MachineSpec.Versions MachineVersionInfo).
//   - MachineDeployment.spec.selector — a metav1.LabelSelector; the MD's
//     controller owns exactly the Machines matching it, which makes selector
//     matching the authoritative Machine→pool mapping.
package machines

import (
	"context"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// MachineDeploymentGVR is the worker-pool resource (scaling patches, kubelet
// rolls, pool resolution).
var MachineDeploymentGVR = schema.GroupVersionResource{
	Group:    "cluster.k8s.io",
	Version:  "v1alpha1",
	Resource: "machinedeployments",
}

// MachineGVR is the per-machine resource (issue collection, P5 remediation
// deletes, P6 roll-progress counting).
var MachineGVR = schema.GroupVersionResource{
	Group:    "cluster.k8s.io",
	Version:  "v1alpha1",
	Resource: "machines",
}

// ListMachines lists the Machines in namespace. The caller bounds ctx.
func ListMachines(ctx context.Context, dyn dynamic.Interface, namespace string) ([]unstructured.Unstructured, error) {
	list, err := dyn.Resource(MachineGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// ListMachineDeployments lists the MachineDeployments in namespace.
func ListMachineDeployments(ctx context.Context, dyn dynamic.Interface, namespace string) ([]unstructured.Unstructured, error) {
	list, err := dyn.Resource(MachineDeploymentGVR).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// NodeRefName returns Machine.status.nodeRef.name ("" = the node never joined
// or the ref is unreadable).
func NodeRefName(m *unstructured.Unstructured) string {
	v, _, _ := unstructured.NestedString(m.Object, "status", "nodeRef", "name")
	return v
}

// ErrorReason returns the TERMINAL Machine.status.errorReason ("" = none).
func ErrorReason(m *unstructured.Unstructured) string {
	v, _, _ := unstructured.NestedString(m.Object, "status", "errorReason")
	return v
}

// ErrorMessage returns the terminal Machine.status.errorMessage ("" = none).
func ErrorMessage(m *unstructured.Unstructured) string {
	v, _, _ := unstructured.NestedString(m.Object, "status", "errorMessage")
	return v
}

// MachineKubeletVersion returns Machine.spec.versions.kubelet ("" = unset).
func MachineKubeletVersion(m *unstructured.Unstructured) string {
	v, _, _ := unstructured.NestedString(m.Object, "spec", "versions", "kubelet")
	return v
}

// MDKubeletVersion returns the pool's desired kubelet version —
// MachineDeployment.spec.template.spec.versions.kubelet ("" = unset).
func MDKubeletVersion(md *unstructured.Unstructured) string {
	v, _, _ := unstructured.NestedString(md.Object, "spec", "template", "spec", "versions", "kubelet")
	return v
}

// Deleting reports whether the object carries a deletionTimestamp (machine-
// controller is already draining/deprovisioning it).
func Deleting(m *unstructured.Unstructured) bool {
	return m.GetDeletionTimestamp() != nil
}

// Age returns how long the object has existed at now (0 when the
// creationTimestamp is unset, e.g. in a hand-rolled fixture).
func Age(m *unstructured.Unstructured, now time.Time) time.Duration {
	created := m.GetCreationTimestamp()
	if created.IsZero() {
		return 0
	}
	return now.Sub(created.Time)
}

// PoolResolver maps a Machine to its owning MachineDeployment. Primary
// mechanism: the MD's spec.selector matched against the machine's labels —
// exactly how the MachineDeployment controller claims its machines, so it is
// authoritative. Fallback (selector unreadable / no match): the ownerRef chain
// by NAME SHAPE — a Machine is named "<machineset>-<rand>" and a MachineSet
// "<md>-<template-hash>", so stripping the trailing segment of the machine's
// owning-MachineSet name yields a candidate MD name, accepted only when an MD
// of that name actually exists (never a guess).
type PoolResolver struct {
	// mds maps MD name → MD object.
	mds map[string]*unstructured.Unstructured
	// selectors maps MD name → parsed spec.selector. An MD with an EMPTY or
	// unparseable selector is excluded: labels.Everything() would claim every
	// machine in the namespace, which is ownership fiction.
	selectors map[string]labels.Selector
	// order keeps resolution deterministic when (mis-)configured selectors
	// overlap: the lexicographically first matching MD wins.
	order []string
}

// NewPoolResolver builds a resolver over the given MachineDeployments.
func NewPoolResolver(mds []unstructured.Unstructured) *PoolResolver {
	r := &PoolResolver{
		mds:       make(map[string]*unstructured.Unstructured, len(mds)),
		selectors: make(map[string]labels.Selector, len(mds)),
	}
	for i := range mds {
		md := &mds[i]
		name := md.GetName()
		if name == "" {
			continue
		}
		r.mds[name] = md
		if sel := parseSelector(md); sel != nil && !sel.Empty() {
			r.selectors[name] = sel
			r.order = append(r.order, name)
		}
	}
	// map iteration order is random; sort for deterministic overlap ties.
	sort.Strings(r.order)
	return r
}

// MD returns the MachineDeployment object for a pool name (nil = unknown).
func (r *PoolResolver) MD(name string) *unstructured.Unstructured { return r.mds[name] }

// PoolFor resolves the owning MachineDeployment for a machine. Returns
// (poolName, mdObject) or ("", nil) when no owner can be established — callers
// must treat an unresolved machine as unowned and never act on it.
func (r *PoolResolver) PoolFor(m *unstructured.Unstructured) (string, *unstructured.Unstructured) {
	set := labels.Set(m.GetLabels())
	for _, name := range r.order {
		if r.selectors[name].Matches(set) {
			return name, r.mds[name]
		}
	}
	// Fallback: ownerRef name shape, verified against the known MDs.
	for _, ref := range m.GetOwnerReferences() {
		if ref.Kind != "MachineSet" {
			continue
		}
		if md := r.mds[stripLastSegment(ref.Name)]; md != nil {
			return md.GetName(), md
		}
	}
	return "", nil
}

// parseSelector extracts + parses MachineDeployment.spec.selector. Returns nil
// when absent or invalid.
func parseSelector(md *unstructured.Unstructured) labels.Selector {
	raw, found, err := unstructured.NestedMap(md.Object, "spec", "selector")
	if !found || err != nil {
		return nil
	}
	var ls metav1.LabelSelector
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &ls); err != nil {
		return nil
	}
	sel, err := metav1.LabelSelectorAsSelector(&ls)
	if err != nil {
		return nil
	}
	return sel
}

// stripLastSegment drops the final "-<segment>" of a generated name
// ("pool-a-6d8f9b7c4" → "pool-a"). Returns "" when there is no separator.
func stripLastSegment(name string) string {
	if i := strings.LastIndexByte(name, '-'); i >= 0 {
		return name[:i]
	}
	return ""
}
