package k3supgrade

import (
	admissionv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyName names the ValidatingAdmissionPolicy and its binding.
const PolicyName = "kubehz-k3s-upgrade-plans"

// PlanValidation is one CEL rule of the admission policy.
type PlanValidation struct {
	Expression string
	Message    string
}

// PlanValidations are the rules of design 4.3 step 4: in the controller's
// namespace the apiserver admits exactly the two Plans of plans.go and
// nothing else. A CREATE or UPDATE that breaks one rule is refused. The
// rules allowlist: a Plan field that is not named here as allowed must be
// absent (secrets, drain, tolerations, extra containers, env, volumes,
// security contexts, a channel, ...).
//
// The expressions run in the apiserver against the CRD's schema. The tests
// compile and run them with cel-go against the templates and against
// changed copies (policy_test.go).
var PlanValidations = []PlanValidation{
	{
		Expression: `object.metadata.name == 'k3s-server' || object.metadata.name == 'k3s-agent'`,
		Message:    "kubehz maintenance: only the Plans k3s-server and k3s-agent are allowed in namespace system-upgrade",
	},
	{
		Expression: `has(object.metadata.labels) && 'kubehz.cloud/operation-id' in object.metadata.labels && 'app.kubernetes.io/managed-by' in object.metadata.labels && object.metadata.labels['app.kubernetes.io/managed-by'] == 'kubehz-agent'`,
		Message:    "kubehz maintenance: a Plan must carry the kubehz operation labels",
	},
	{
		Expression: `has(object.spec.serviceAccountName) && object.spec.serviceAccountName == 'system-upgrade' && has(object.spec.concurrency) && object.spec.concurrency == 1 && has(object.spec.cordon) && object.spec.cordon == true`,
		Message:    "kubehz maintenance: a Plan must use the service account system-upgrade, concurrency 1 and cordon",
	},
	{
		Expression: `has(object.spec.version) && object.spec.version.matches('^v1\\.[0-9]{1,3}\\.[0-9]{1,3}\\+k3s[0-9]{1,3}$') && !has(object.spec.channel)`,
		Message:    "kubehz maintenance: a Plan must name an exact k3s version and no channel",
	},
	{
		Expression: `!has(object.spec.drain) && !has(object.spec.secrets) && !has(object.spec.tolerations) && !has(object.spec.window) && !has(object.spec.imagePullSecrets) && !has(object.spec.postCompleteDelay) && !has(object.spec.postCompleteLabels) && !has(object.spec.priorityClassName) && !has(object.spec.jobActiveDeadlineSecs) && (!has(object.spec.exclusive) || object.spec.exclusive == false)`,
		Message:    "kubehz maintenance: a Plan must not set drain, secrets, tolerations, window, image pull secrets, post-complete settings, a priority class, a job deadline or exclusive",
	},
	{
		Expression: `has(object.spec.upgrade) && object.spec.upgrade.image == 'rancher/k3s-upgrade' && !has(object.spec.upgrade.command) && !has(object.spec.upgrade.args) && !has(object.spec.upgrade.envs) && !has(object.spec.upgrade.envFrom) && !has(object.spec.upgrade.volumes) && !has(object.spec.upgrade.securityContext)`,
		Message:    "kubehz maintenance: the upgrade container must be rancher/k3s-upgrade with no command, args, env, volumes or security context",
	},
	{
		Expression: `object.metadata.name != 'k3s-server' || (!has(object.spec.prepare) && has(object.spec.nodeSelector) && !has(object.spec.nodeSelector.matchLabels) && has(object.spec.nodeSelector.matchExpressions) && size(object.spec.nodeSelector.matchExpressions) == 1 && object.spec.nodeSelector.matchExpressions[0].key == 'node-role.kubernetes.io/control-plane' && object.spec.nodeSelector.matchExpressions[0].operator == 'In' && has(object.spec.nodeSelector.matchExpressions[0].values) && object.spec.nodeSelector.matchExpressions[0].values == ['true'])`,
		Message:    "kubehz maintenance: the server Plan must select the control-plane nodes and have no prepare step",
	},
	{
		Expression: `object.metadata.name != 'k3s-agent' || (has(object.spec.prepare) && object.spec.prepare.image == 'rancher/k3s-upgrade' && has(object.spec.prepare.args) && object.spec.prepare.args == ['prepare', 'k3s-server'] && !has(object.spec.prepare.command) && !has(object.spec.prepare.envs) && !has(object.spec.prepare.envFrom) && !has(object.spec.prepare.volumes) && !has(object.spec.prepare.securityContext) && has(object.spec.nodeSelector) && !has(object.spec.nodeSelector.matchLabels) && has(object.spec.nodeSelector.matchExpressions) && size(object.spec.nodeSelector.matchExpressions) == 1 && object.spec.nodeSelector.matchExpressions[0].key == 'node-role.kubernetes.io/control-plane' && object.spec.nodeSelector.matchExpressions[0].operator == 'DoesNotExist' && (!has(object.spec.nodeSelector.matchExpressions[0].values) || size(object.spec.nodeSelector.matchExpressions[0].values) == 0))`,
		Message:    "kubehz maintenance: the agent Plan must select the nodes that are not control-plane and prepare with exactly 'prepare k3s-server'",
	},
}

// AdmissionPolicy is the ValidatingAdmissionPolicy of design 4.3 step 4.
// The binding limits it to the controller's namespace: the controller
// reads Plans of that namespace only.
func AdmissionPolicy() *admissionv1.ValidatingAdmissionPolicy {
	fail := admissionv1.Fail
	equivalent := admissionv1.Equivalent
	validations := make([]admissionv1.Validation, 0, len(PlanValidations))
	for _, v := range PlanValidations {
		validations = append(validations, admissionv1.Validation{Expression: v.Expression, Message: v.Message})
	}
	return &admissionv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: PolicyName, Labels: managedLabels()},
		Spec: admissionv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &fail,
			MatchConstraints: &admissionv1.MatchResources{
				MatchPolicy: &equivalent,
				ResourceRules: []admissionv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{admissionv1.Create, admissionv1.Update},
						Rule: admissionv1.Rule{
							APIGroups:   []string{PlanGVR.Group},
							APIVersions: []string{PlanGVR.Version},
							Resources:   []string{PlanGVR.Resource},
						},
					},
				}},
			},
			Validations: validations,
		},
	}
}

// AdmissionPolicyBinding binds the policy with the Deny action to the
// controller's namespace.
func AdmissionPolicyBinding() *admissionv1.ValidatingAdmissionPolicyBinding {
	return &admissionv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: PolicyName, Labels: managedLabels()},
		Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        PolicyName,
			ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny},
			MatchResources: &admissionv1.MatchResources{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					"kubernetes.io/metadata.name": Namespace,
				}},
			},
		},
	}
}
