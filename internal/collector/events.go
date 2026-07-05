package collector

import (
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// SummarizeWarningEvents returns the most recent Warning events, newest first,
// capped to limit. Reason + involved-object Kind + count + last-seen are always
// reported (they describe cluster health without leaking specifics). The
// namespace and the free-text message are privacy-sensitive (they can carry
// object names / arbitrary text) and are included ONLY when reportNamespaces is
// true. Non-Warning events are dropped.
func SummarizeWarningEvents(events []*corev1.Event, limit int, reportNamespaces bool) []state.EventState {
	type withTime struct {
		ev *corev1.Event
		at time.Time
	}
	filtered := make([]withTime, 0, len(events))
	for _, e := range events {
		if e == nil || e.Type != corev1.EventTypeWarning {
			continue
		}
		filtered = append(filtered, withTime{ev: e, at: eventLastSeen(e)})
	}
	// Newest first; ties broken by name for a deterministic order.
	sort.SliceStable(filtered, func(i, j int) bool {
		if !filtered[i].at.Equal(filtered[j].at) {
			return filtered[i].at.After(filtered[j].at)
		}
		return filtered[i].ev.Name > filtered[j].ev.Name
	})
	if limit >= 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}

	out := make([]state.EventState, 0, len(filtered))
	for _, f := range filtered {
		e := f.ev
		es := state.EventState{
			Reason:   e.Reason,
			Kind:     e.InvolvedObject.Kind,
			Count:    e.Count,
			LastSeen: f.at.UTC().Format(time.RFC3339),
		}
		if reportNamespaces {
			es.Namespace = e.Namespace
			es.Note = e.Message
		}
		out = append(out, es)
	}
	return out
}

// eventLastSeen picks the best available timestamp: LastTimestamp (classic
// events), else EventTime (events.k8s.io-style), else the object creation time.
func eventLastSeen(e *corev1.Event) time.Time {
	if !e.LastTimestamp.IsZero() {
		return e.LastTimestamp.Time
	}
	if !e.EventTime.IsZero() {
		return e.EventTime.Time
	}
	return e.CreationTimestamp.Time
}
