package inventory

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// ── fixtures ────────────────────────────────────────────────────────────────

// releaseDoc is a helm release document with the fields helm really writes
// (storage/driver + release.Release json tags), PLUS a fat `manifest` so every
// test exercises the "the payload carries rendered content we must never keep"
// case.
func releaseDoc(name, ns, chart, chartVer, appVer, status, lastDeployed string, rev int) map[string]any {
	return map[string]any{
		"name":      name,
		"namespace": ns,
		"version":   rev,
		"info": map[string]any{
			"status":         status,
			"last_deployed":  lastDeployed,
			"first_deployed": "2026-01-01T00:00:00Z",
			"description":    "Upgrade complete",
		},
		"chart": map[string]any{
			"metadata": map[string]any{
				"name":       chart,
				"version":    chartVer,
				"appVersion": appVer,
			},
		},
		"config":   map[string]any{"adminPassword": "hunter2"},
		"manifest": "apiVersion: v1\nkind: Secret\nstringData:\n  password: hunter2\n",
	}
}

// encodeRelease applies helm's own encoding: JSON → gzip → base64.
func encodeRelease(t *testing.T, doc map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatalf("gzip release: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return []byte(base64.StdEncoding.EncodeToString(buf.Bytes()))
}

// helmSecret builds the Secret helm 3 stores for one release revision.
func helmSecret(t *testing.T, ns, name, chart, chartVer, appVer, status, lastDeployed string, rev int) *corev1.Secret {
	t.Helper()
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("sh.helm.release.v1.%s.v%d", name, rev),
			Namespace: ns,
			Labels: map[string]string{
				"owner": "helm", "name": name, "status": status, "version": fmt.Sprint(rev),
			},
		},
		Type: HelmReleaseSecretType,
		Data: map[string][]byte{
			releaseDataKey: encodeRelease(t, releaseDoc(name, ns, chart, chartVer, appVer, status, lastDeployed, rev)),
		},
	}
}

// helmLister runs the objects through a REAL informer with the production
// transform installed, then hands back its lister — the same path the agent
// uses, so a projection bug cannot hide behind a hand-built cache.
func helmLister(t *testing.T, objs ...runtime.Object) func() ([]*corev1.Secret, error) {
	t.Helper()
	client := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(client, 0)
	inf := factory.Core().V1().Secrets()
	if err := inf.Informer().SetTransform(ProjectHelmRelease); err != nil {
		t.Fatalf("set transform: %v", err)
	}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	if !cache.WaitForCacheSync(stop, inf.Informer().HasSynced) {
		t.Fatal("secret informer cache did not sync")
	}
	return func() ([]*corev1.Secret, error) { return inf.Lister().List(labels.Everything()) }
}

func addonNames(addons []state.Addon) []string {
	out := make([]string, 0, len(addons))
	for _, a := range addons {
		out = append(out, a.Name)
	}
	return out
}

// ── decoding ────────────────────────────────────────────────────────────────

// TestDecodeRelease_HelmEncoding: a normally-stored release parses to the
// right name and versions.
func TestDecodeRelease_HelmEncoding(t *testing.T) {
	raw := encodeRelease(t, releaseDoc("cilium", "kube-system", "cilium", "1.19.2", "1.19.2", "deployed", "2026-07-06T10:00:00Z", 3))
	rel, err := decodeRelease(raw)
	if err != nil {
		t.Fatalf("decodeRelease: %v", err)
	}
	if rel.Name != "cilium" || rel.Chart != "cilium" {
		t.Errorf("name/chart = %q/%q, want cilium/cilium", rel.Name, rel.Chart)
	}
	if rel.ChartVersion != "1.19.2" || rel.AppVersion != "1.19.2" {
		t.Errorf("versions = %q/%q, want 1.19.2/1.19.2", rel.ChartVersion, rel.AppVersion)
	}
	if rel.Revision != 3 || rel.Status != "deployed" {
		t.Errorf("revision/status = %d/%q, want 3/deployed", rel.Revision, rel.Status)
	}
	if rel.LastDeployed != "2026-07-06T10:00:00Z" {
		t.Errorf("lastDeployed = %q", rel.LastDeployed)
	}
}

// TestDecodeRelease_UncompressedIsStillReadable: releases written before helm
// compressed them are base64'd JSON with no gzip header — helm still decodes
// those, so the agent must too.
func TestDecodeRelease_UncompressedIsStillReadable(t *testing.T) {
	raw, err := json.Marshal(releaseDoc("legacy", "default", "legacy", "0.1.0", "1.0", "deployed", "", 1))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rel, err := decodeRelease([]byte(base64.StdEncoding.EncodeToString(raw)))
	if err != nil {
		t.Fatalf("decodeRelease: %v", err)
	}
	if rel.Name != "legacy" || rel.ChartVersion != "0.1.0" {
		t.Errorf("got %+v", rel)
	}
}

// TestDecodeRelease_MalformedIsSkippedNotFatal: every corruption mode returns
// an error (the caller drops that ONE release) and none of them panics.
func TestDecodeRelease_MalformedIsSkippedNotFatal(t *testing.T) {
	gzipHeaderOnly := base64.StdEncoding.EncodeToString([]byte{0x1f, 0x8b, 0x08, 0x00, 0x99})
	noName, err := json.Marshal(map[string]any{"namespace": "default", "version": 2})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	cases := map[string]string{
		"empty":             "",
		"not base64":        "!!! not base64 !!!",
		"base64 of junk":    base64.StdEncoding.EncodeToString([]byte("this is not json")),
		"truncated gzip":    gzipHeaderOnly,
		"json without name": base64.StdEncoding.EncodeToString(noName),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if rel, err := decodeRelease([]byte(payload)); err == nil {
				t.Fatalf("want an error, got release %+v", rel)
			}
		})
	}
}

// ── the transform (privacy) ─────────────────────────────────────────────────

// TestProjectHelmRelease_NeverCachesPayloads is the privacy contract: whatever
// goes in, what comes out carries NO Secret data — the helm payload (rendered
// manifests, merged values, credentials) is decoded to seven metadata strings
// and dropped, and a Secret that is not a helm release keeps nothing at all.
func TestProjectHelmRelease_NeverCachesPayloads(t *testing.T) {
	t.Run("helm release keeps metadata only", func(t *testing.T) {
		in := helmSecret(t, "kube-system", "cilium", "cilium", "1.19.2", "1.19.2", "deployed", "2026-07-06T10:00:00Z", 3)
		out, err := ProjectHelmRelease(in)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		s := out.(*corev1.Secret)
		if s.Data != nil {
			t.Errorf("Data survived the transform: %v", s.Data)
		}
		if s.StringData[projChartVersion] != "1.19.2" || s.StringData[projName] != "cilium" {
			t.Errorf("projection = %v", s.StringData)
		}
		// BOUND the projection, do not just spot-check two keys. Without this
		// an added `"values": …` line would keep the whole release payload in
		// the cache and this test would still pass — which is the one thing
		// it exists to prevent.
		allowed := map[string]bool{
			projName: true, projChart: true, projChartVersion: true, projAppVersion: true,
			projStatus: true, projRevision: true, projLastDeployed: true,
		}
		for k := range s.StringData {
			if !allowed[k] {
				t.Errorf("projection leaked an unexpected key %q (value %q) — the transform must keep ONLY release metadata", k, s.StringData[k])
			}
		}
		if len(s.StringData) > len(allowed) {
			t.Errorf("projection has %d keys, allow-list has %d: %v", len(s.StringData), len(allowed), s.StringData)
		}
		rel, ok := releaseFromSecret(s)
		if !ok || rel.Revision != 3 || rel.Namespace != "kube-system" {
			t.Errorf("readback = %+v (ok=%t)", rel, ok)
		}
	})

	t.Run("foreign secret is emptied and never projected", func(t *testing.T) {
		in := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "app"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"password": []byte("hunter2")},
		}
		out, err := ProjectHelmRelease(in)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		s := out.(*corev1.Secret)
		if s.Data != nil || s.StringData != nil {
			t.Errorf("foreign secret retained data: Data=%v StringData=%v", s.Data, s.StringData)
		}
		if _, ok := releaseFromSecret(s); ok {
			t.Error("a foreign secret must not read back as a release")
		}
	})

	t.Run("undecodable release is emptied, not dropped as an error", func(t *testing.T) {
		in := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1.broken.v1", Namespace: "app"},
			Type:       HelmReleaseSecretType,
			Data:       map[string][]byte{releaseDataKey: []byte("not-base64-at-all!!")},
		}
		out, err := ProjectHelmRelease(in)
		if err != nil {
			t.Fatalf("transform must never error: %v", err)
		}
		s := out.(*corev1.Secret)
		if s.Data != nil || len(s.StringData) != 0 {
			t.Errorf("broken release retained data: Data=%v StringData=%v", s.Data, s.StringData)
		}
	})

	t.Run("tombstone wrapper is unwrapped", func(t *testing.T) {
		in := cache.DeletedFinalStateUnknown{
			Key: "kube-system/sh.helm.release.v1.cilium.v3",
			Obj: helmSecret(t, "kube-system", "cilium", "cilium", "1.19.2", "1.19.2", "deployed", "", 3),
		}
		out, err := ProjectHelmRelease(in)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		d, ok := out.(cache.DeletedFinalStateUnknown)
		if !ok {
			t.Fatalf("wrapper lost: %T", out)
		}
		if s := d.Obj.(*corev1.Secret); s.Data != nil {
			t.Errorf("Data survived inside the tombstone: %v", s.Data)
		}
	})
}

// ── the producer ────────────────────────────────────────────────────────────

// TestObserve_FromHelmReleaseSecrets is the end-to-end path through a fake
// clientset + real informer: several releases, several revisions, one
// uninstalled tombstone, one corrupt record and one unrelated Secret.
func TestObserve_FromHelmReleaseSecrets(t *testing.T) {
	lister := helmLister(t,
		helmSecret(t, "kube-system", "cilium", "cilium", "1.19.1", "1.19.1", "superseded", "2026-07-01T09:00:00Z", 1),
		helmSecret(t, "kube-system", "cilium", "cilium", "1.19.2", "1.19.2", "deployed", "2026-07-06T10:00:00Z", 2),
		helmSecret(t, "cert-manager", "cert-manager", "cert-manager", "v1.20.1", "v1.20.1", "deployed", "2026-07-05T08:00:00Z", 1),
		helmSecret(t, "old", "removed-app", "removed-app", "0.4.0", "0.4.0", "uninstalled", "2026-06-01T00:00:00Z", 2),
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "sh.helm.release.v1.broken.v1", Namespace: "app"},
			Type:       HelmReleaseSecretType,
			Data:       map[string][]byte{releaseDataKey: []byte("corrupt")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "app"},
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{"password": []byte("hunter2")},
		},
	)

	inv := Observe(Sources{Secrets: lister})
	if inv == nil {
		t.Fatal("Observe returned nil for a cluster with helm releases")
	}
	got := addonNames(inv.Addons)
	want := []string{"cert-manager", "cilium"} // sorted; tombstone + corrupt + foreign dropped
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("addons = %v, want %v", got, want)
	}
	for _, a := range inv.Addons {
		if a.Source != SourceHelm {
			t.Errorf("%s source = %q, want %q", a.Name, a.Source, SourceHelm)
		}
	}
	// The NEWEST revision wins: 1.19.2, not the superseded 1.19.1.
	if inv.Addons[1].ChartVersion != "1.19.2" || inv.Addons[1].AppVersion != "1.19.2" {
		t.Errorf("cilium = %+v, want chart/app 1.19.2", inv.Addons[1])
	}
	if inv.Addons[0].ChartVersion != "v1.20.1" {
		t.Errorf("cert-manager chartVersion = %q, want v1.20.1", inv.Addons[0].ChartVersion)
	}
	// renderedAt is derived from the data (newest last_deployed), never the
	// clock — an unchanged cluster must produce an identical block.
	if inv.RenderedAt != "2026-07-06T10:00:00Z" {
		t.Errorf("renderedAt = %q, want the newest release's last_deployed", inv.RenderedAt)
	}
}

// TestObserve_EmptyClusterIsEmptyNotAPanic: no releases, no pods, no listers
// at all — every combination yields "no inventory block", never a panic.
func TestObserve_EmptyClusterIsEmptyNotAPanic(t *testing.T) {
	if inv := Observe(Sources{}); inv != nil {
		t.Errorf("no sources: got %+v, want nil", inv)
	}
	if inv := Observe(Sources{Secrets: helmLister(t)}); inv != nil {
		t.Errorf("empty cluster: got %+v, want nil", inv)
	}
	if addons, at := foldReleases(nil); len(addons) != 0 || at != "" {
		t.Errorf("foldReleases(nil) = %v/%q, want empty", addons, at)
	}
	if addons, _ := foldReleases([]release{{Name: ""}}); len(addons) != 0 {
		t.Errorf("a nameless release must not become an addon: %v", addons)
	}
}

// TestObserve_UnreadableSecretsFallBackToPodLabels: no secrets RBAC (the
// lister errors) degrades to the helm labels on pods rather than to nothing.
func TestObserve_UnreadableSecretsFallBackToPodLabels(t *testing.T) {
	pods := []*corev1.Pod{
		helmPod("kube-system", "cilium-abc", map[string]string{
			labelManagedBy: "Helm", labelInstance: "cilium",
			labelChart: "cilium-1.19.2", labelAppVer: "1.19.2",
		}),
		helmPod("kube-system", "cilium-def", map[string]string{ // same release, second pod
			labelManagedBy: "Helm", labelInstance: "cilium",
			labelChart: "cilium-1.19.2", labelAppVer: "1.19.2",
		}),
		helmPod("cert-manager", "cm-1", map[string]string{
			labelManagedBy: "helm", labelAppName: "cert-manager",
			labelChart: "cert-manager-v1.20.1", labelAppVer: "v1.20.1",
		}),
		helmPod("default", "hand-rolled", map[string]string{"app": "mine"}), // not helm-managed
	}
	inv := Observe(Sources{
		Secrets: func() ([]*corev1.Secret, error) { return nil, errors.New("secrets is forbidden") },
		Pods:    func() ([]*corev1.Pod, error) { return pods, nil },
	})
	if inv == nil {
		t.Fatal("pod fallback produced no inventory")
	}
	if got, want := addonNames(inv.Addons), []string{"cert-manager", "cilium"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("addons = %v, want %v", got, want)
	}
	if inv.Addons[1].ChartVersion != "1.19.2" || inv.Addons[0].ChartVersion != "v1.20.1" {
		t.Errorf("chart label split wrong: %+v", inv.Addons)
	}
	if inv.RenderedAt != "" {
		t.Errorf("pod labels carry no deploy time; renderedAt = %q, want empty", inv.RenderedAt)
	}
}

func helmPod(ns, name string, lbls map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls}}
}

// TestSplitChartLabel pins the `helm.sh/chart` split: both the chart name and
// the version may contain dashes.
func TestSplitChartLabel(t *testing.T) {
	cases := []struct{ in, chart, version string }{
		{"cilium-1.19.2", "cilium", "1.19.2"},
		{"cert-manager-v1.20.1", "cert-manager", "v1.20.1"},
		{"cilium-1.2.3-rc.1", "cilium", "1.2.3-rc.1"},
		{"nochart", "", ""},
		{"", "", ""},
		{"weird-name-only", "", ""},
	}
	for _, c := range cases {
		chart, version := splitChartLabel(c.in)
		if chart != c.chart || version != c.version {
			t.Errorf("splitChartLabel(%q) = %q/%q, want %q/%q", c.in, chart, version, c.chart, c.version)
		}
	}
}

// TestFoldReleases_DedupesAndCaps: one name per cluster (the wire schema has no
// namespace), and never more entries than kubehz-api's hard cap.
func TestFoldReleases_DedupesAndCaps(t *testing.T) {
	rs := []release{
		{Name: "redis", Namespace: "team-b", ChartVersion: "2.0.0"},
		{Name: "redis", Namespace: "team-a", ChartVersion: "1.0.0"},
	}
	addons, _ := foldReleases(rs)
	if len(addons) != 1 || addons[0].ChartVersion != "1.0.0" {
		t.Fatalf("collision fold = %+v, want the team-a entry only", addons)
	}

	var many []release
	for i := range state.MaxAddons + 25 {
		many = append(many, release{Name: fmt.Sprintf("app-%03d", i), Namespace: "default"})
	}
	addons, _ = foldReleases(many)
	if len(addons) != state.MaxAddons {
		t.Fatalf("cap = %d addons, want %d", len(addons), state.MaxAddons)
	}
	if addons[0].Name != "app-000" {
		t.Errorf("cap must clip deterministically from a sorted list, got %q first", addons[0].Name)
	}
}

// TestObserve_WireShape is the CONTRACT test against kubehz-api: the block is
// marshalled and compared byte for byte with the keys HeartbeatSchema's
// `inventory` object accepts (kubehz-api server/utils/validation.ts — the
// object is non-strict, so a mis-named key would be silently STRIPPED and the
// dashboard would stay dark with no error anywhere).
func TestObserve_WireShape(t *testing.T) {
	lister := helmLister(t,
		helmSecret(t, "kube-system", "cilium", "cilium", "1.19.2", "1.19.2", "deployed", "2026-07-06T10:00:00Z", 2),
	)
	inv := Observe(Sources{Secrets: lister})
	if inv == nil {
		t.Fatal("no inventory")
	}
	state.ApplyCaps(&state.Payload{Inventory: inv}) // the caps the sender applies
	raw, err := json.Marshal(inv)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"lok8sVersion":"","kind":"","specHash":"","renderedAt":"2026-07-06T10:00:00Z",` +
		`"addons":[{"name":"cilium","chartVersion":"1.19.2","appVersion":"1.19.2","source":"helm"}]}`
	if string(raw) != want {
		t.Errorf("wire shape drifted from the kubehz-api contract\n got: %s\nwant: %s", raw, want)
	}
}
