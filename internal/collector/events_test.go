package collector

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func warnEvent(name, reason, kind, ns, msg string, count int32, last time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: ns},
		Type:           corev1.EventTypeWarning,
		Reason:         reason,
		Message:        msg,
		Count:          count,
		LastTimestamp:  metav1.NewTime(last),
		InvolvedObject: corev1.ObjectReference{Kind: kind, Namespace: ns},
	}
}

func TestSummarizeWarningEvents_FiltersAndOrders(t *testing.T) {
	base := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	events := []*corev1.Event{
		warnEvent("e1", "BackOff", "Pod", "default", "back-off restarting", 3, base),
		warnEvent("e2", "FailedScheduling", "Pod", "batch", "no nodes", 1, base.Add(2*time.Minute)),
		{ // Normal event MUST be dropped
			ObjectMeta: metav1.ObjectMeta{Name: "n1"},
			Type:       corev1.EventTypeNormal,
			Reason:     "Scheduled",
		},
		nil, // skipped
	}
	got := SummarizeWarningEvents(events, 20, false)
	if len(got) != 2 {
		t.Fatalf("want 2 warnings, got %d: %+v", len(got), got)
	}
	// Newest first: e2 (12:02) before e1 (12:00).
	if got[0].Reason != "FailedScheduling" || got[1].Reason != "BackOff" {
		t.Errorf("wrong order: %+v", got)
	}
	if got[0].Kind != "Pod" || got[0].Count != 1 {
		t.Errorf("field mapping wrong: %+v", got[0])
	}
	// Privacy default: no namespace, no message.
	if got[0].Namespace != "" || got[0].Note != "" {
		t.Errorf("privacy leak with reportNamespaces=false: %+v", got[0])
	}
	if got[0].LastSeen != base.Add(2*time.Minute).Format(time.RFC3339) {
		t.Errorf("lastSeen = %q", got[0].LastSeen)
	}
}

func TestSummarizeWarningEvents_NamespaceOptIn(t *testing.T) {
	base := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	events := []*corev1.Event{warnEvent("e1", "BackOff", "Pod", "prod", "secret detail", 3, base)}
	got := SummarizeWarningEvents(events, 20, true)
	if len(got) != 1 {
		t.Fatalf("want 1, got %d", len(got))
	}
	if got[0].Namespace != "prod" || got[0].Note != "secret detail" {
		t.Errorf("namespace/note not included on opt-in: %+v", got[0])
	}
}

func TestSummarizeWarningEvents_Limit(t *testing.T) {
	base := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
	var events []*corev1.Event
	for i := 0; i < 50; i++ {
		events = append(events, warnEvent("e", "R", "Pod", "d", "m", 1, base.Add(time.Duration(i)*time.Second)))
	}
	got := SummarizeWarningEvents(events, 20, false)
	if len(got) != 20 {
		t.Errorf("limit not applied: got %d", len(got))
	}
	// The most-recent one (i=49) must be first.
	if got[0].LastSeen != base.Add(49*time.Second).Format(time.RFC3339) {
		t.Errorf("newest not first after limit: %q", got[0].LastSeen)
	}
}
