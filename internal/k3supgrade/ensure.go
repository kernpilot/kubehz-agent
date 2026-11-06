package k3supgrade

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/kernpilot/kubehz-agent/internal/desired"
)

// ensureController makes sure that the admission policy and its binding
// exist, that the overlay's controller and settings are there and turn
// retries off, and that the controller runs one replica, in that order: the
// policy is in place before the controller can act on a Plan. ready is true
// when the Plan resource is served and the controller is available.
func (r *Reconciler) ensureController(ctx context.Context, st *observed) (ready bool, err error) {
	if st.controller == nil {
		return false, refuse("the system-upgrade-controller of the maintenance overlay is missing in namespace %s; apply install-maintenance.yaml of the agent release, then start a new upgrade", Namespace)
	}
	if !ours(st.controller.Labels) {
		return false, refuse("a system-upgrade-controller that kubehz did not install runs in namespace %s; remove it, apply install-maintenance.yaml, then start a new upgrade", Namespace)
	}
	for i := range st.plans {
		if !ours(st.plans[i].GetLabels()) {
			return false, refuse("Plan %s/%s was not written by kubehz and the controller would run it; remove it, then start a new upgrade", Namespace, st.plans[i].GetName())
		}
	}
	if err := r.checkSettings(ctx); err != nil {
		return false, err
	}
	if err := r.ensurePolicy(ctx); err != nil {
		return false, err
	}
	if err := r.scaleController(ctx, st, 1); err != nil {
		return false, writeErr("the scale of Deployment "+Namespace+"/"+ControllerName, err)
	}
	available := replicasOf(st.controller) >= 1 && st.controller.Status.AvailableReplicas >= 1
	return available && !st.planCRDMissing, nil
}

// checkSettings refuses controller settings that allow job retries: a
// failed upgrade job must fail the operation (design 4.3 step 5).
func (r *Reconciler) checkSettings(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	cm, err := r.kube.CoreV1().ConfigMaps(Namespace).Get(cctx, ControllerEnvConfigMap, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return refuse("the controller settings ConfigMap %s/%s is missing; apply install-maintenance.yaml of the agent release, then start a new upgrade", Namespace, ControllerEnvConfigMap)
	case apierrors.IsForbidden(err):
		return writeErr("ConfigMap "+Namespace+"/"+ControllerEnvConfigMap, err)
	case err != nil:
		return fmt.Errorf("get ConfigMap %s/%s: %w", Namespace, ControllerEnvConfigMap, err)
	}
	want := controllerEnv()
	for _, k := range []string{"SYSTEM_UPGRADE_JOB_BACKOFF_LIMIT", "SYSTEM_UPGRADE_JOB_TTL_SECONDS_AFTER_FINISH"} {
		if cm.Data[k] != want[k] {
			return refuse("the controller settings %s/%s set %s=%q, want %q (no job retries); apply install-maintenance.yaml of the agent release, then start a new upgrade", Namespace, ControllerEnvConfigMap, k, cm.Data[k], want[k])
		}
	}
	return nil
}

// writeErr turns a write error into a refusal when a retry cannot help.
func writeErr(what string, err error) error {
	switch {
	case apierrors.IsForbidden(err):
		return refuse("the agent may not write %s (HTTP 403); apply the maintenance RBAC (install-maintenance.yaml of the agent release), then start a new upgrade", what)
	case apierrors.IsInvalid(err):
		return refuse("the apiserver refused %s: %s", what, errDetail(err))
	default:
		return fmt.Errorf("write %s: %w", what, err)
	}
}

func specHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

func withHash(annotations map[string]string, h string) map[string]string {
	out := make(map[string]string, len(annotations)+1)
	for k, v := range annotations {
		out[k] = v
	}
	out[specHashAnnotation] = h
	return out
}

func (r *Reconciler) ensurePolicy(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	api := r.kube.AdmissionregistrationV1()

	want := AdmissionPolicy()
	h := specHash(want.Spec)
	want.Annotations = withHash(nil, h)
	got, err := api.ValidatingAdmissionPolicies().Get(cctx, PolicyName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := api.ValidatingAdmissionPolicies().Create(cctx, want, metav1.CreateOptions{FieldManager: ManagedBy}); err != nil {
			return writeErr("ValidatingAdmissionPolicy "+PolicyName, err)
		}
		r.log.Info("k3s upgrade: admission policy created", "policy", PolicyName)
	case err != nil:
		return writeErr("ValidatingAdmissionPolicy "+PolicyName, err)
	case got.Annotations[specHashAnnotation] != h || !ours(got.Labels):
		got.Spec = want.Spec
		got.Labels = want.Labels
		got.Annotations = withHash(got.Annotations, h)
		if _, err := api.ValidatingAdmissionPolicies().Update(cctx, got, metav1.UpdateOptions{FieldManager: ManagedBy}); err != nil {
			return writeErr("ValidatingAdmissionPolicy "+PolicyName, err)
		}
	}

	wantB := AdmissionPolicyBinding()
	hb := specHash(wantB.Spec)
	wantB.Annotations = withHash(nil, hb)
	gotB, err := api.ValidatingAdmissionPolicyBindings().Get(cctx, PolicyName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := api.ValidatingAdmissionPolicyBindings().Create(cctx, wantB, metav1.CreateOptions{FieldManager: ManagedBy}); err != nil {
			return writeErr("ValidatingAdmissionPolicyBinding "+PolicyName, err)
		}
	case err != nil:
		return writeErr("ValidatingAdmissionPolicyBinding "+PolicyName, err)
	case gotB.Annotations[specHashAnnotation] != hb || !ours(gotB.Labels):
		gotB.Spec = wantB.Spec
		gotB.Labels = wantB.Labels
		gotB.Annotations = withHash(gotB.Annotations, hb)
		if _, err := api.ValidatingAdmissionPolicyBindings().Update(cctx, gotB, metav1.UpdateOptions{FieldManager: ManagedBy}); err != nil {
			return writeErr("ValidatingAdmissionPolicyBinding "+PolicyName, err)
		}
	}
	return nil
}

// scaleController sets the replica count of the overlay's controller
// through the scale subresource: the agent cannot change the controller's
// pod. A missing or foreign controller is left alone.
func (r *Reconciler) scaleController(ctx context.Context, st *observed, replicas int32) error {
	if st.controller == nil || !ours(st.controller.Labels) || replicasOf(st.controller) == replicas {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	_, err := r.kube.AppsV1().Deployments(Namespace).UpdateScale(cctx, ControllerName, &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: ControllerName, Namespace: Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}, metav1.UpdateOptions{FieldManager: ManagedBy})
	if err != nil {
		return fmt.Errorf("scale %s/%s to %d: %w", Namespace, ControllerName, replicas, err)
	}
	upd := st.controller.DeepCopy()
	upd.Spec.Replicas = &replicas
	if replicas == 0 {
		upd.Status.AvailableReplicas = 0
	}
	st.controller = upd
	r.log.Info("k3s upgrade: system-upgrade-controller scaled", "replicas", replicas)
	return nil
}

func replicasOf(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return 1 // the apiserver default
	}
	return *d.Spec.Replicas
}

// ensurePlans writes the server and the agent Plan of the operation. An
// existing Plan with another spec or labels is updated to the template.
func (r *Reconciler) ensurePlans(ctx context.Context, st *observed, op *desired.Operation) error {
	res := r.dyn.Resource(PlanGVR).Namespace(Namespace)
	for _, want := range []*unstructured.Unstructured{
		ServerPlan(op.ID, op.TargetVersion),
		AgentPlan(op.ID, op.TargetVersion),
	} {
		var got *unstructured.Unstructured
		for i := range st.plans {
			if st.plans[i].GetName() == want.GetName() {
				got = &st.plans[i]
			}
		}
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		var err error
		switch {
		case got == nil:
			_, err = res.Create(cctx, want, metav1.CreateOptions{FieldManager: ManagedBy})
			if err == nil {
				r.log.Info("k3s upgrade: Plan written", "plan", want.GetName(), "operation", op.ID, "version", op.TargetVersion)
			}
		case !samePlan(got, want):
			upd := got.DeepCopy()
			upd.SetLabels(want.GetLabels())
			upd.Object["spec"] = want.Object["spec"]
			_, err = res.Update(cctx, upd, metav1.UpdateOptions{FieldManager: ManagedBy})
			if err == nil {
				r.log.Info("k3s upgrade: Plan updated to the template", "plan", want.GetName(), "operation", op.ID)
			}
		}
		cancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("write Plan %s: the Plan resource is not served yet: %w", want.GetName(), err)
			}
			return writeErr("Plan "+Namespace+"/"+want.GetName(), err)
		}
	}
	return nil
}

// samePlan compares labels and the full spec of two Plans.
func samePlan(got, want *unstructured.Unstructured) bool {
	gl, wl := got.GetLabels(), want.GetLabels()
	for k, v := range wl {
		if gl[k] != v {
			return false
		}
	}
	gs, _ := json.Marshal(got.Object["spec"])
	ws, _ := json.Marshal(want.Object["spec"])
	return bytes.Equal(gs, ws)
}

// removePolicy removes the admission policy and its binding (maintenance
// off). The controller stays at zero replicas: it belongs to the overlay,
// and the customer removes it with the overlay.
func (r *Reconciler) removePolicy(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	bg := metav1.DeletePropagationBackground
	del := metav1.DeleteOptions{PropagationPolicy: &bg}
	api := r.kube.AdmissionregistrationV1()
	if b, err := api.ValidatingAdmissionPolicyBindings().Get(cctx, PolicyName, metav1.GetOptions{}); err == nil && ours(b.Labels) {
		if err := api.ValidatingAdmissionPolicyBindings().Delete(cctx, PolicyName, del); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ValidatingAdmissionPolicyBinding %s: %w", PolicyName, err)
		}
		r.log.Info("k3s upgrade: admission policy binding removed (maintenance is off)")
	} else if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
		return fmt.Errorf("get ValidatingAdmissionPolicyBinding %s: %w", PolicyName, err)
	}
	if p, err := api.ValidatingAdmissionPolicies().Get(cctx, PolicyName, metav1.GetOptions{}); err == nil && ours(p.Labels) {
		if err := api.ValidatingAdmissionPolicies().Delete(cctx, PolicyName, del); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ValidatingAdmissionPolicy %s: %w", PolicyName, err)
		}
	} else if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
		return fmt.Errorf("get ValidatingAdmissionPolicy %s: %w", PolicyName, err)
	}
	return nil
}
