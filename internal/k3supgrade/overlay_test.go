package k3supgrade

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

var update = flag.Bool("update", false, "rewrite the overlay files that the tests compare")

// overlayFile is the controller file of the maintenance overlay. The Go
// templates are its source: run
//
//	go test ./internal/k3supgrade -run TestOverlayMatchesTemplates -update
//
// after a change to controller.go.
var overlayFile = filepath.Join("..", "..", "deploy", "maintenance", "system-upgrade-controller.yaml")

const overlayHeader = `# GENERATED from internal/k3supgrade/controller.go by
#   go test ./internal/k3supgrade -run TestOverlayMatchesTemplates -update
# Do not edit by hand: a test compares this file with the templates.
#
# The system-upgrade-controller settings and Deployment, with ZERO replicas.
# The agent scales the Deployment to one replica only while an upgrade
# operation runs (it has the right to change the scale, not the pod).
`

func renderOverlay(t *testing.T) []byte {
	t.Helper()
	cm := ControllerConfigMap()
	cm.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
	dep := ControllerDeployment(0)
	dep.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	var b bytes.Buffer
	b.WriteString(overlayHeader)
	for _, obj := range []any{cm, dep} {
		raw, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		delete(m, "status")
		delete(m["metadata"].(map[string]any), "creationTimestamp")
		if spec, ok := m["spec"].(map[string]any); ok {
			delete(spec["template"].(map[string]any)["metadata"].(map[string]any), "creationTimestamp")
		}
		out, err := yaml.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString("---\n")
		b.Write(out)
	}
	return b.Bytes()
}

func TestOverlayMatchesTemplates(t *testing.T) {
	want := renderOverlay(t)
	if *update {
		if err := os.WriteFile(overlayFile, want, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(overlayFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s differs from the templates in controller.go; run the test with -update", overlayFile)
	}
	if !strings.Contains(string(got), "replicas: 0") || !strings.Contains(string(got), ControllerImage) {
		t.Fatal("the overlay controller must start with zero replicas and the pinned image")
	}
}
