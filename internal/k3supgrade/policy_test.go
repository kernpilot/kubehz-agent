package k3supgrade

import (
	"testing"

	"github.com/google/cel-go/cel"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// evalPolicy runs every rule of the admission policy against obj, as the
// apiserver does for a CREATE or UPDATE. It returns the messages of the
// rules that deny.
func evalPolicy(t *testing.T, obj *unstructured.Unstructured) []string {
	t.Helper()
	env, err := cel.NewEnv(cel.Variable("object", cel.DynType))
	if err != nil {
		t.Fatalf("CEL env: %v", err)
	}
	var denied []string
	for _, v := range PlanValidations {
		ast, iss := env.Compile(v.Expression)
		if iss.Err() != nil {
			t.Fatalf("compile %q: %v", v.Expression, iss.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			t.Fatalf("program %q: %v", v.Expression, err)
		}
		out, _, err := prg.Eval(map[string]any{"object": obj.Object})
		if err != nil {
			// The apiserver refuses on an evaluation error (failurePolicy
			// Fail), so an error counts as a denial.
			denied = append(denied, v.Message+" (error: "+err.Error()+")")
			continue
		}
		if ok, _ := out.Value().(bool); !ok {
			denied = append(denied, v.Message)
		}
	}
	return denied
}

func TestPolicyAdmitsTheTemplates(t *testing.T) {
	for _, p := range []*unstructured.Unstructured{
		ServerPlan("op-1", "v1.34.2+k3s1"),
		AgentPlan("op-1", "v1.34.2+k3s1"),
	} {
		if denied := evalPolicy(t, p); len(denied) > 0 {
			t.Errorf("template %s denied: %v", p.GetName(), denied)
		}
	}
}

func TestPolicyDeniesChangedPlans(t *testing.T) {
	set := func(p *unstructured.Unstructured, v any, path ...string) *unstructured.Unstructured {
		if err := unstructured.SetNestedField(p.Object, v, path...); err != nil {
			t.Fatal(err)
		}
		return p
	}
	del := func(p *unstructured.Unstructured, path ...string) *unstructured.Unstructured {
		unstructured.RemoveNestedField(p.Object, path...)
		return p
	}
	exprOp := func(p *unstructured.Unstructured, op string) *unstructured.Unstructured {
		exprs, _, _ := unstructured.NestedSlice(p.Object, "spec", "nodeSelector", "matchExpressions")
		exprs[0].(map[string]any)["operator"] = op
		return set(p, exprs, "spec", "nodeSelector", "matchExpressions")
	}
	srv := func() *unstructured.Unstructured { return ServerPlan("op-1", "v1.34.2+k3s1") }
	agt := func() *unstructured.Unstructured { return AgentPlan("op-1", "v1.34.2+k3s1") }

	cases := map[string]*unstructured.Unstructured{
		"other name":             set(srv(), "evil", "metadata", "name"),
		"no operation label":     del(srv(), "metadata", "labels", LabelOperation),
		"other manager":          set(srv(), "someone", "metadata", "labels", LabelManagedBy),
		"concurrency 2":          set(srv(), int64(2), "spec", "concurrency"),
		"no cordon":              set(srv(), false, "spec", "cordon"),
		"other service account":  set(srv(), "default", "spec", "serviceAccountName"),
		"channel":                set(srv(), "https://update.k3s.io/v1-release/channels/latest", "spec", "channel"),
		"version not exact":      set(srv(), "latest", "spec", "version"),
		"version with suffix":    set(srv(), "v1.34.2+k3s1; rm -rf /", "spec", "version"),
		"drain":                  set(srv(), map[string]any{"force": true}, "spec", "drain"),
		"secrets":                set(srv(), []any{map[string]any{"name": "x", "path": "/host"}}, "spec", "secrets"),
		"tolerations":            set(srv(), []any{map[string]any{"operator": "Exists"}}, "spec", "tolerations"),
		"exclusive":              set(srv(), true, "spec", "exclusive"),
		"other upgrade image":    set(srv(), "evil/k3s-upgrade", "spec", "upgrade", "image"),
		"upgrade command":        set(srv(), []any{"sh", "-c", "id"}, "spec", "upgrade", "command"),
		"upgrade env":            set(srv(), []any{map[string]any{"name": "A", "value": "B"}}, "spec", "upgrade", "envs"),
		"upgrade volumes":        set(srv(), []any{map[string]any{"name": "v", "source": "/"}}, "spec", "upgrade", "volumes"),
		"server with prepare":    set(srv(), map[string]any{"image": UpgradeImage, "args": []any{"prepare", "x"}}, "spec", "prepare"),
		"server selects all":     exprOp(srv(), "Exists"),
		"server matchLabels":     set(srv(), map[string]any{"a": "b"}, "spec", "nodeSelector", "matchLabels"),
		"agent prepare args":     set(agt(), []any{"prepare", "k3s-server", "--extra"}, "spec", "prepare", "args"),
		"agent prepare image":    set(agt(), "evil/image", "spec", "prepare", "image"),
		"agent prepare command":  set(agt(), []any{"sh"}, "spec", "prepare", "command"),
		"agent selects servers":  exprOp(agt(), "Exists"),
		"agent without prepare":  del(agt(), "spec", "prepare"),
		"agent without selector": del(agt(), "spec", "nodeSelector"),
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if denied := evalPolicy(t, p); len(denied) == 0 {
				t.Errorf("the policy admits a Plan with %s", name)
			}
		})
	}
}

func TestPolicyBindingIsScopedToTheControllerNamespace(t *testing.T) {
	b := AdmissionPolicyBinding()
	if b.Spec.PolicyName != PolicyName {
		t.Fatalf("binding names policy %q", b.Spec.PolicyName)
	}
	if len(b.Spec.ValidationActions) != 1 || b.Spec.ValidationActions[0] != "Deny" {
		t.Fatalf("validation actions = %v, want [Deny]", b.Spec.ValidationActions)
	}
	sel := b.Spec.MatchResources.NamespaceSelector.MatchLabels
	if len(sel) != 1 || sel["kubernetes.io/metadata.name"] != Namespace {
		t.Fatalf("namespace selector = %v, want only %s", sel, Namespace)
	}
	p := AdmissionPolicy()
	if p.Spec.FailurePolicy == nil || *p.Spec.FailurePolicy != "Fail" {
		t.Fatal("failure policy must be Fail")
	}
	if len(p.Spec.Validations) != len(PlanValidations) {
		t.Fatalf("policy has %d validations, want %d", len(p.Spec.Validations), len(PlanValidations))
	}
}
