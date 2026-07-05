package collector

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// SummarizePods reduces the full pod set to phase counts — never names or
// specs (spec §2: "workload visibility without workload contents"). The
// per-namespace breakdown is included ONLY when reportNamespaces is true
// (namespace names are the privacy-sensitive bit); by default the summary is
// phase-only, which reveals nothing about what runs where.
func SummarizePods(pods []*corev1.Pod, reportNamespaces bool) state.PodSummary {
	s := state.PodSummary{}
	var byNS map[string]int
	if reportNamespaces {
		byNS = map[string]int{}
	}
	for _, p := range pods {
		if p == nil {
			continue
		}
		s.Total++
		switch p.Status.Phase {
		case corev1.PodRunning:
			s.Running++
		case corev1.PodPending:
			s.Pending++
		case corev1.PodFailed:
			s.Failed++
		case corev1.PodSucceeded:
			s.Succeeded++
		default:
			s.Unknown++
		}
		if byNS != nil {
			byNS[p.Namespace]++
		}
	}
	if len(byNS) > 0 {
		s.ByNamespace = byNS
	}
	return s
}
