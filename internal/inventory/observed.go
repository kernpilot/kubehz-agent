package inventory

import (
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// The OBSERVED inventory producer: what is actually installed in the cluster,
// built from informer caches at push time and folded into the SAME debounced
// heartbeat as everything else (there is no second sender and no poll loop —
// the helm-release Secret watch and the pod watch wake the existing coalescer).
//
// It exists because the declared inventory (the lok8s ClusterInventory CR) is
// absent on every cluster that was not deployed by a recent `lo`, which left
// the dashboard's addon overview dark for those users. The CR still WINS when
// it exists: it carries lok8sVersion/kind/provider/specHash and the curated
// addon categories, none of which can be observed. This producer only fills
// the gap.
//
// Two sources, in order of fidelity:
//
//  1. helm release Secrets (`type=helm.sh/release.v1`) — authoritative chart
//     and app versions for EVERY helm release, needs cluster-wide secrets
//     list/watch (the opt-in deploy/inventory overlay).
//  2. helm's recommended labels on PODS — `app.kubernetes.io/managed-by=Helm`
//     plus `helm.sh/chart` / `app.kubernetes.io/version`. No new permission at
//     all (the pod informer already runs), but coverage depends on the chart
//     propagating those labels to its pod template, so it is the FALLBACK.
//
// Both are fail-soft to the point of silence: a lister error, an unreadable
// secret, a namespace the agent cannot see, or a chart that labels nothing all
// degrade to a partial (or absent) inventory. Nothing here can fail a beat.

// Helm's recommended labels (helm.sh/docs/chart_best_practices/labels).
const (
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "app.kubernetes.io/instance"
	labelAppName   = "app.kubernetes.io/name"
	labelAppVer    = "app.kubernetes.io/version"
	labelChart     = "helm.sh/chart"

	managedByHelm = "helm"
)

// Sources are the informer-backed listers the producer reads. Both are
// optional: a nil (or erroring) source is skipped, which is how the agent
// degrades when the helm-release Secret watch has no RBAC. Matching the
// executor's Nodes/Pods options, they are plain funcs so tests need no
// informer plumbing beyond the fake they already build.
type Sources struct {
	// Secrets lists the PROJECTED helm-release Secrets (see ProjectHelmRelease
	// — the cached objects carry metadata only, never release payloads).
	Secrets func() ([]*corev1.Secret, error)
	// Pods lists the pod cache the live view already watches.
	Pods func() ([]*corev1.Pod, error)
}

// Observe builds the heartbeat `inventory` block from the cluster's own helm
// state, or nil when nothing is observable — a cluster with no helm releases
// sends no inventory key at all, exactly as before, so the dashboard card
// self-hides instead of claiming an empty deployment.
//
// The block deliberately fills only the fields that can be OBSERVED: addons
// (name + chart/app version + source=helm) and renderedAt (the newest release's
// last-deployed time — the honest "as of" anchor for the card). lok8sVersion,
// kind, provider, specHash stay empty; they are declarations, not observations,
// and kubehz-api treats every one of them as optional.
func Observe(src Sources) *state.Inventory {
	releases := releasesFromSecrets(src.Secrets)
	if len(releases) == 0 {
		releases = releasesFromPods(src.Pods)
	}
	addons, renderedAt := foldReleases(releases)
	if len(addons) == 0 {
		return nil
	}
	return &state.Inventory{
		RenderedAt: renderedAt,
		Addons:     addons,
	}
}

// releasesFromSecrets reads every projected helm-release Secret. A lister
// error yields no releases (the caller falls back to pod labels); individual
// unprojected/foreign secrets are skipped one by one, so one bad record never
// costs the rest of the inventory.
func releasesFromSecrets(list func() ([]*corev1.Secret, error)) []release {
	if list == nil {
		return nil
	}
	secrets, err := list()
	if err != nil {
		return nil
	}
	out := make([]release, 0, len(secrets))
	for _, s := range secrets {
		if rel, ok := releaseFromSecret(s); ok {
			out = append(out, rel)
		}
	}
	return out
}

// releasesFromPods derives releases from the labels helm charts put on their
// pods. Only pods explicitly labelled `app.kubernetes.io/managed-by=Helm` are
// considered, so nothing that is not a helm release can appear. Versions come
// from `helm.sh/chart` ("<chart>-<version>") and `app.kubernetes.io/version`
// (the chart's appVersion); either may be missing, and a name-only entry is
// still worth reporting.
//
// Revision/status/lastDeployed are unknowable from a label, so every entry is
// revision 0 with an empty status — foldReleases treats an empty status as
// "not a tombstone" and keeps it.
func releasesFromPods(list func() ([]*corev1.Pod, error)) []release {
	if list == nil {
		return nil
	}
	pods, err := list()
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{}, len(pods))
	var out []release
	for _, p := range pods {
		if p == nil || !strings.EqualFold(p.Labels[labelManagedBy], managedByHelm) {
			continue
		}
		name := p.Labels[labelInstance]
		if name == "" {
			name = p.Labels[labelAppName]
		}
		if name == "" {
			continue
		}
		key := p.Namespace + "/" + name
		if _, dup := seen[key]; dup {
			continue // many pods per release
		}
		seen[key] = struct{}{}
		chart, version := splitChartLabel(p.Labels[labelChart])
		out = append(out, release{
			Name:         name,
			Namespace:    p.Namespace,
			Chart:        chart,
			ChartVersion: version,
			AppVersion:   p.Labels[labelAppVer],
		})
	}
	return out
}

// splitChartLabel splits helm's `helm.sh/chart` value ("<chart>-<version>")
// into its two halves. The chart NAME may itself contain dashes
// (cert-manager-v1.20.1) and the VERSION may too (cilium-1.2.3-rc.1), so the
// split point is the first dash whose remainder starts a version — a digit, or
// a `v` followed by a digit. An unsplittable value yields ("", "") rather than
// a guess.
func splitChartLabel(v string) (chart, version string) {
	for i := 0; i < len(v); i++ {
		if v[i] != '-' {
			continue
		}
		rest := v[i+1:]
		if startsVersion(rest) {
			return v[:i], rest
		}
	}
	return "", ""
}

func startsVersion(s string) bool {
	if s == "" {
		return false
	}
	if s[0] >= '0' && s[0] <= '9' {
		return true
	}
	return (s[0] == 'v' || s[0] == 'V') && len(s) > 1 && s[1] >= '0' && s[1] <= '9'
}

// foldReleases turns raw release records into the payload's addon list:
//
//   - per (namespace, release) the HIGHEST revision wins — helm keeps one
//     Secret per revision, and the newest one is the current state;
//   - a release whose newest record is `uninstalled` is dropped: with
//     `--keep-history` its records survive the uninstall, and reporting it as
//     installed would be a lie;
//   - the same release NAME in several namespaces collapses to the
//     lexicographically first namespace — the wire schema has no namespace
//     field, and duplicate names would key two rows identically in the
//     dashboard and dedupe first-wins in the api's update diff anyway;
//   - the result is sorted by name and capped at state.MaxAddons, so an
//     over-large cluster clips deterministically instead of flapping.
//
// renderedAt is the newest parseable last-deployed timestamp across the kept
// releases, normalized to RFC3339 UTC ("" when nothing parses — the dashboard
// hides the "as of" line rather than showing a fabricated date). It is derived
// from the DATA, never from the clock, so an unchanged cluster produces a
// byte-identical block and never wakes an extra push.
func foldReleases(releases []release) ([]state.Addon, string) {
	newest := make(map[string]release, len(releases))
	for _, r := range releases {
		if r.Name == "" {
			continue
		}
		key := r.Namespace + "/" + r.Name
		if cur, ok := newest[key]; ok && cur.Revision >= r.Revision {
			continue
		}
		newest[key] = r
	}

	kept := make([]release, 0, len(newest))
	for _, r := range newest {
		if strings.EqualFold(r.Status, statusUninstalled) {
			continue
		}
		kept = append(kept, r)
	}
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].Name != kept[j].Name {
			return kept[i].Name < kept[j].Name
		}
		return kept[i].Namespace < kept[j].Namespace
	})

	addons := make([]state.Addon, 0, len(kept))
	var latest time.Time
	seenName := make(map[string]struct{}, len(kept))
	for _, r := range kept {
		if _, dup := seenName[r.Name]; dup {
			continue
		}
		seenName[r.Name] = struct{}{}
		if len(addons) == state.MaxAddons {
			break
		}
		addons = append(addons, state.Addon{
			Name:         r.Name,
			ChartVersion: r.ChartVersion,
			AppVersion:   r.AppVersion,
			Source:       SourceHelm,
		})
		if t, err := time.Parse(time.RFC3339, r.LastDeployed); err == nil && t.After(latest) {
			latest = t
		}
	}
	if len(addons) == 0 {
		return nil, ""
	}
	var renderedAt string
	if !latest.IsZero() {
		renderedAt = latest.UTC().Format(time.RFC3339)
	}
	return addons, renderedAt
}
