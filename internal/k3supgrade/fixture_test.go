package k3supgrade

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/desired"
)

const (
	opID    = "op-1"
	fromV   = "v1.33.5+k3s1"
	targetV = "v1.34.2+k3s1"
)

type fakeReporter struct {
	ops     []string
	reports []Report
	err     error
}

func (f *fakeReporter) Report(_ context.Context, op string, r Report) error {
	if f.err != nil {
		return f.err
	}
	f.ops = append(f.ops, op)
	f.reports = append(f.reports, r)
	return nil
}

func (f *fakeReporter) last(t *testing.T) Report {
	t.Helper()
	if len(f.reports) == 0 {
		t.Fatal("no report sent")
	}
	return f.reports[len(f.reports)-1]
}

type fixture struct {
	t         *testing.T
	kube      *fake.Clientset
	dyn       *dynamicfake.FakeDynamicClient
	rep       *fakeReporter
	now       time.Time
	apiserver map[string]string
}

func newFixture(t *testing.T, objs ...runtime.Object) *fixture {
	t.Helper()
	var typed, plans []runtime.Object
	for _, o := range objs {
		if _, ok := o.(*unstructured.Unstructured); ok {
			plans = append(plans, o)
		} else {
			typed = append(typed, o)
		}
	}
	f := &fixture{
		t:    t,
		kube: fake.NewClientset(typed...),
		dyn: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{PlanGVR: "PlanList"}, plans...),
		rep:       &fakeReporter{},
		now:       time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		apiserver: map[string]string{},
	}
	// The fake tracker does not serve the scale subresource: map it onto
	// the stored Deployment, as the apiserver does.
	deployments := appsv1.SchemeGroupVersion.WithResource("deployments")
	f.kube.PrependReactor("get", "deployments", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "scale" {
			return false, nil, nil
		}
		obj, err := f.kube.Tracker().Get(deployments, a.GetNamespace(), a.(k8stesting.GetAction).GetName())
		if err != nil {
			return true, nil, err
		}
		d := obj.(*appsv1.Deployment)
		return true, &autoscalingv1.Scale{ObjectMeta: d.ObjectMeta, Spec: autoscalingv1.ScaleSpec{Replicas: replicasOf(d)}}, nil
	})
	f.kube.PrependReactor("update", "deployments", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "scale" {
			return false, nil, nil
		}
		sc := a.(k8stesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		obj, err := f.kube.Tracker().Get(deployments, a.GetNamespace(), sc.Name)
		if err != nil {
			return true, nil, err
		}
		d := obj.(*appsv1.Deployment).DeepCopy()
		d.Spec.Replicas = &sc.Spec.Replicas
		if sc.Spec.Replicas == 0 {
			d.Status.AvailableReplicas = 0
		}
		return true, sc, f.kube.Tracker().Update(deployments, d, a.GetNamespace())
	})
	return f
}

func (f *fixture) reconciler() *Reconciler {
	return New(Options{
		Kube:     f.kube,
		Dyn:      f.dyn,
		Reporter: f.rep,
		APIServerVersion: func(_ context.Context, n *corev1.Node) (string, error) {
			if v, ok := f.apiserver[n.Name]; ok {
				return v, nil
			}
			return "", errors.New("connection refused")
		},
		Now:    func() time.Time { return f.now },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func (f *fixture) op(state string) *desired.Operation {
	return &desired.Operation{
		ID:            opID,
		TargetVersion: targetV,
		State:         state,
		DumpTakenAt:   f.now.Add(-10 * time.Minute).Format(time.RFC3339),
	}
}

func doc(maintenance bool, op *desired.Operation) *desired.Doc {
	return &desired.Doc{StateStore: &desired.StateStore{Enabled: true, Maintenance: maintenance, Operation: op}}
}

func node(name string, server bool, version string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: version},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}},
		},
	}
	if server {
		n.Labels[ControlPlaneLabel] = "true"
	}
	return n
}

func ourDeployment(replicas, available int32) *appsv1.Deployment {
	d := ControllerDeployment(replicas)
	d.Status.AvailableReplicas = available
	return d
}

func job(name, nodeName string, done batchv1.JobConditionType) *batchv1.Job {
	j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: Namespace,
		Labels: map[string]string{
			LabelManagedBy:                   ManagedBy,
			LabelOperation:                   opID,
			jobLabelPlan:                     ServerPlanName,
			jobLabelNode:                     nodeName,
			planLabelPrefix + ServerPlanName: "hash-1",
		},
	}}
	if done != "" {
		j.Status.Conditions = []batchv1.JobCondition{{Type: done, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}
	}
	return j
}

func jobPod(name, jobName, nodeName string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: Namespace, Labels: map[string]string{
			LabelManagedBy: ManagedBy,
			LabelOperation: opID,
			jobNameLabel:   jobName,
			jobLabelNode:   nodeName,
		}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func controllerPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "system-upgrade-controller-abc", Namespace: Namespace,
		Labels: map[string]string{controllerPodLabel: ControllerName},
	}}
}

// plans returns the Plans in the fake.
func (f *fixture) plans() []unstructured.Unstructured {
	f.t.Helper()
	l, err := f.dyn.Resource(PlanGVR).Namespace(Namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		f.t.Fatal(err)
	}
	return l.Items
}

func (f *fixture) deployment() *appsv1.Deployment {
	f.t.Helper()
	d, err := f.kube.AppsV1().Deployments(Namespace).Get(context.Background(), ControllerName, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return d
}

func (f *fixture) markControllerAvailable() {
	f.t.Helper()
	d := f.deployment()
	if d == nil {
		f.t.Fatal("no controller Deployment")
	}
	d.Status.AvailableReplicas = replicasOf(d)
	if _, err := f.kube.AppsV1().Deployments(Namespace).UpdateStatus(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) deletePod(name string) {
	f.t.Helper()
	if err := f.kube.CoreV1().Pods(Namespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		f.t.Fatal(err)
	}
}

// writes lists the create, update, patch and delete actions on a resource.
func writes(actions []k8stesting.Action, resource string) []k8stesting.Action {
	var out []k8stesting.Action
	for _, a := range actions {
		if a.GetResource().Resource != resource {
			continue
		}
		switch a.GetVerb() {
		case "create", "update", "patch", "delete":
			out = append(out, a)
		}
	}
	return out
}

func verbs(actions []k8stesting.Action) string {
	var out []string
	for _, a := range actions {
		out = append(out, a.GetVerb()+" "+a.GetResource().Resource)
	}
	return strings.Join(out, ", ")
}

// deletePolicy returns the propagation policy of a delete action.
func deletePolicy(t *testing.T, a k8stesting.Action) metav1.DeletionPropagation {
	t.Helper()
	d, ok := a.(k8stesting.DeleteActionImpl)
	if !ok {
		t.Fatalf("action %s %s is not a delete", a.GetVerb(), a.GetResource().Resource)
	}
	if d.DeleteOptions.PropagationPolicy == nil {
		return ""
	}
	return *d.DeleteOptions.PropagationPolicy
}
