//go:build integration

// Package integration runs the agent's acting loop against a REAL Kubernetes
// apiserver. Every other test in this repository drives a fake client, which
// answers whatever the fake was taught; this one proves the loop against the
// thing it actually talks to — real merge-patch semantics, real schema
// validation of the field it writes, real resourceVersion bookkeeping (so
// "idempotent" can be measured instead of asserted), and real 404s for the
// resources the agent refuses to create.
//
// SCALING is the loop under test on purpose: it is the reversible one. It
// patches one integer on one object, so a wrong answer costs a replica count,
// never a deleted server. Healing deletes machines and is not run here.
//
// Run it by hand against any throwaway cluster:
//
//	kind create cluster --name kubehz-agent-it \
//	  --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
//	go test -tags integration -count=1 -v ./test/integration/...
//	kind delete cluster --name kubehz-agent-it
//
// It uses the ambient kubeconfig (KUBECONFIG, else ~/.kube/config), creates
// its own namespace, and deletes it again. NEVER point it at a cluster you
// care about: it writes MachineDeployment objects and installs the
// machine-controller CRD if it is absent.
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/executor"
	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

var crdGVR = schema.GroupVersionResource{
	Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
}

const (
	mdCRDName   = "machinedeployments.cluster.k8s.io"
	maxReplicas = 5
	// waitTimeout bounds every wait loop: a real apiserver needs a moment to
	// serve a freshly established CRD.
	waitTimeout = 90 * time.Second
)

// TestScalingLoopAgainstRealAPIServer drives executor.Reconcile end to end on
// a live apiserver: one pool converges, every refusal holds, and a converged
// pool is re-reconciled without writing anything at all.
func TestScalingLoopAgainstRealAPIServer(t *testing.T) {
	ctx := context.Background()
	cfg := restConfig(t)
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}

	ensureMachineDeploymentCRD(ctx, t, dyn)
	ns := createNamespace(ctx, t, client)

	mds := dyn.Resource(executor.MachineDeploymentGVR).Namespace(ns)
	create(ctx, t, mds, machineDeployment("pool-a", 2, withServerType("cpx31")))
	create(ctx, t, mds, machineDeployment("pool-cp", 3, withCPLabel()))
	create(ctx, t, mds, machineDeployment("pool-auto", 3, withAutoscaler()))
	create(ctx, t, mds, machineDeployment("pool-big", 1))
	create(ctx, t, mds, machineDeployment("pool-legacy", 4))

	store := actions.New(nil)
	exec := executor.New(dyn, store, executor.Options{Namespace: ns, MaxReplicas: maxReplicas})

	doc := &desired.Doc{
		Revision:  11,
		Execution: desired.Execution{Scaling: true},
		WorkerPools: []desired.WorkerPool{
			{Name: "pool-a", MachineType: "cpx41", DesiredReplicas: 5},
			{Name: "pool-cp", DesiredReplicas: 1},
			{Name: "pool-auto", DesiredReplicas: 1},
			{Name: "pool-big", DesiredReplicas: maxReplicas + 1},
			{Name: "ghost", DesiredReplicas: 2},
		},
	}
	exec.Reconcile(ctx, doc)

	// ── The pool the platform may scale converged, on the apiserver.
	if got := replicas(ctx, t, mds, "pool-a"); got != 5 {
		t.Errorf("pool-a replicas = %d, want 5", got)
	}
	if a := action(t, store, state.ActionScale, "pool-a"); a.Status != state.ActionDone {
		t.Errorf("pool-a action = %+v, want done", a)
	}

	// ── machineType is reported, never applied: the template still declares
	//    what it was created with.
	if a := action(t, store, state.ActionScale, "pool-a"); !strings.Contains(a.Detail, "machineType cpx41 not applied") {
		t.Errorf("pool-a detail = %q, want the machineType divergence reported", a.Detail)
	}
	if got := machines.MDMachineType(get(ctx, t, mds, "pool-a")); got != "cpx31" {
		t.Errorf("pool-a machine type = %q, want the untouched cpx31", got)
	}

	// ── Every refusal holds against the real object.
	for _, tc := range []struct {
		pool, want string
		replicas   int64
	}{
		{pool: "pool-cp", want: "control-plane", replicas: 3},
		{pool: "pool-auto", want: "cluster-autoscaler", replicas: 3},
		{pool: "pool-big", want: "outside the agent's allowed range", replicas: 1},
		{pool: "ghost", want: "never creates pools"},
	} {
		a := action(t, store, state.ActionScale, tc.pool)
		if a.Status != state.ActionFailed || !strings.Contains(a.Detail, tc.want) {
			t.Errorf("%s action = %+v, want a failure naming %q", tc.pool, a, tc.want)
		}
		if tc.replicas > 0 {
			if got := replicas(ctx, t, mds, tc.pool); got != tc.replicas {
				t.Errorf("%s replicas = %d, want the untouched %d", tc.pool, got, tc.replicas)
			}
		}
	}

	// ── The agent creates no pool and deletes none. A refused pool must not
	//    exist afterwards, and an undeclared one must survive untouched.
	if _, err := mds.Get(ctx, "ghost", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("ghost MachineDeployment lookup = %v, want NotFound (the agent never creates pools)", err)
	}
	if got := replicas(ctx, t, mds, "pool-legacy"); got != 4 {
		t.Errorf("undeclared pool-legacy replicas = %d, want the untouched 4", got)
	}
	for _, a := range store.Snapshot() {
		if a.Target == "pool-legacy" {
			t.Errorf("undeclared pool reported: %+v", a)
		}
	}

	// ── IDEMPOTENCE, measured: a converged pool is re-reconciled with no
	//    write at all. Only a real apiserver can prove this — resourceVersion
	//    changes on every write, and on nothing else.
	before := get(ctx, t, mds, "pool-a").GetResourceVersion()
	exec.Reconcile(ctx, doc)
	if after := get(ctx, t, mds, "pool-a").GetResourceVersion(); after != before {
		t.Errorf("resourceVersion moved from %s to %s on a converged pool — the pass wrote when it had nothing to do",
			before, after)
	}
	if got := replicas(ctx, t, mds, "pool-a"); got != 5 {
		t.Errorf("pool-a replicas = %d after the second pass, want a stable 5", got)
	}

	// ── A new intent still lands (the loop is live, not wedged).
	doc.Revision = 12
	doc.WorkerPools[0].DesiredReplicas = 3
	exec.Reconcile(ctx, doc)
	if got := replicas(ctx, t, mds, "pool-a"); got != 3 {
		t.Errorf("pool-a replicas = %d after scaling down, want 3", got)
	}
}

// restConfig loads the ambient kubeconfig. No cluster is a FAILURE, not a
// skip: this test only builds under the integration tag, so reaching it means
// a cluster was promised.
func restConfig(t *testing.T) *rest.Config {
	t.Helper()
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatalf("no usable kubeconfig (%v).\ncreate a throwaway cluster first:\n"+
			"  kind create cluster --name kubehz-agent-it", err)
	}
	return cfg
}

// ensureMachineDeploymentCRD installs a stand-in for machine-controller's
// MachineDeployment CRD (kubermatic machine-controller v1.65.0,
// cluster.k8s.io/v1alpha1). spec.replicas is typed as an integer ON PURPOSE:
// the apiserver then validates the one field the agent writes, so a patch
// carrying the wrong JSON type fails here instead of in a customer cluster.
// The rest of the object keeps unknown fields, the way the real CRD does.
func ensureMachineDeploymentCRD(ctx context.Context, t *testing.T, dyn dynamic.Interface) {
	t.Helper()
	crd := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": mdCRDName},
		"spec": map[string]any{
			"group": "cluster.k8s.io",
			"names": map[string]any{
				"kind":     "MachineDeployment",
				"listKind": "MachineDeploymentList",
				"plural":   "machinedeployments",
				"singular": "machinedeployment",
			},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name":    "v1alpha1",
				"served":  true,
				"storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{
					"type":                                 "object",
					"x-kubernetes-preserve-unknown-fields": true,
					"properties": map[string]any{
						"spec": map[string]any{
							"type":                                 "object",
							"x-kubernetes-preserve-unknown-fields": true,
							"properties": map[string]any{
								"replicas": map[string]any{"type": "integer"},
							},
						},
					},
				}},
			}},
		},
	}}

	if _, err := dyn.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("install the MachineDeployment CRD: %v", err)
	}
	waitFor(t, "the MachineDeployment CRD to be established", func() bool {
		got, err := dyn.Resource(crdGVR).Get(ctx, mdCRDName, metav1.GetOptions{})
		if err != nil {
			return false
		}
		conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
		for _, c := range conds {
			m, ok := c.(map[string]any)
			if ok && m["type"] == "Established" && m["status"] == "True" {
				return true
			}
		}
		return false
	})
}

// createNamespace makes a throwaway namespace and removes it afterwards.
func createNamespace(ctx context.Context, t *testing.T, client kubernetes.Interface) string {
	t.Helper()
	name := fmt.Sprintf("kubehz-agent-it-%d", time.Now().UnixNano()%1_000_000)
	_, err := client.CoreV1().Namespaces().Create(ctx,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = client.CoreV1().Namespaces().Delete(context.Background(), name, metav1.DeleteOptions{})
	})
	return name
}

// machineDeployment builds a pool object shaped like machine-controller's.
func machineDeployment(name string, replicas int64, opts ...func(map[string]any)) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "cluster.k8s.io/v1alpha1",
		"kind":       "MachineDeployment",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"replicas": replicas,
			"selector": map[string]any{"matchLabels": map[string]any{"workerset": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"workerset": name}},
				"spec": map[string]any{
					"versions": map[string]any{"kubelet": "v1.35.8"},
				},
			},
		},
	}
	for _, opt := range opts {
		opt(obj)
	}
	return &unstructured.Unstructured{Object: obj}
}

func withServerType(serverType string) func(map[string]any) {
	return func(obj map[string]any) {
		u := &unstructured.Unstructured{Object: obj}
		_ = unstructured.SetNestedMap(u.Object, map[string]any{
			"cloudProvider":     "hetzner",
			"cloudProviderSpec": map[string]any{"serverType": serverType},
		}, "spec", "template", "spec", "providerSpec", "value")
	}
}

func withCPLabel() func(map[string]any) {
	return func(obj map[string]any) {
		u := &unstructured.Unstructured{Object: obj}
		_ = unstructured.SetNestedMap(u.Object,
			map[string]any{"node-role.kubernetes.io/control-plane": ""},
			"spec", "template", "metadata", "labels")
	}
}

func withAutoscaler() func(map[string]any) {
	return func(obj map[string]any) {
		u := &unstructured.Unstructured{Object: obj}
		u.SetAnnotations(map[string]string{
			"cluster.k8s.io/cluster-api-autoscaler-node-group-min-size": "1",
			"cluster.k8s.io/cluster-api-autoscaler-node-group-max-size": "9",
		})
	}
}

// create writes one fixture, retrying while the freshly established CRD is
// still propagating to every apiserver handler.
func create(ctx context.Context, t *testing.T, client dynamic.ResourceInterface, obj *unstructured.Unstructured) {
	t.Helper()
	var lastErr error
	waitFor(t, "the MachineDeployment resource to accept "+obj.GetName(), func() bool {
		_, err := client.Create(ctx, obj, metav1.CreateOptions{})
		if err == nil || apierrors.IsAlreadyExists(err) {
			return true
		}
		lastErr = err
		return false
	})
	if lastErr != nil {
		t.Logf("create %s succeeded after retries (last error: %v)", obj.GetName(), lastErr)
	}
}

func get(ctx context.Context, t *testing.T, client dynamic.ResourceInterface, name string) *unstructured.Unstructured {
	t.Helper()
	got, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	return got
}

func replicas(ctx context.Context, t *testing.T, client dynamic.ResourceInterface, name string) int64 {
	t.Helper()
	n, found, err := unstructured.NestedInt64(get(ctx, t, client, name).Object, "spec", "replicas")
	if err != nil || !found {
		t.Fatalf("spec.replicas on %s unreadable (found=%v): %v", name, found, err)
	}
	return n
}

func action(t *testing.T, store *actions.Store, actionType, target string) state.Action {
	t.Helper()
	for _, a := range store.Snapshot() {
		if a.Type == actionType && a.Target == target {
			return a
		}
	}
	t.Fatalf("no %s action for %q in %+v", actionType, target, store.Snapshot())
	return state.Action{}
}

func waitFor(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		if ready() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", waitTimeout, what)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
