package desired

import (
	"encoding/json"
	"testing"
)

// The stateStore block of build contract 4.4 decodes as published.
func TestDoc_DecodesStateStore(t *testing.T) {
	var d Doc
	err := json.Unmarshal([]byte(`{"revision": 4, "stateStore": {
	  "enabled": true, "maintenance": true, "renewPaused": false,
	  "operation": {"id": "op-7", "targetVersion": "v1.37.2+k3s1", "state": "published", "dumpTakenAt": "2026-11-06T10:00:00Z"}
	}}`), &d)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	ss := d.StateStore
	if ss == nil || !ss.Enabled || !ss.Maintenance || ss.Operation == nil || ss.Operation.ID != "op-7" || !ss.Operation.Active() {
		t.Fatalf("stateStore = %+v", ss)
	}
}

func TestOperation_Active(t *testing.T) {
	var nilOp *Operation
	if nilOp.Active() {
		t.Fatal("nil operation is active")
	}
	for state, want := range map[string]bool{
		OperationPending: false, OperationDumping: false, OperationPublished: true,
		OperationRunning: true, OperationFailed: false, OperationDone: false, OperationCancelled: false,
	} {
		if got := (&Operation{State: state}).Active(); got != want {
			t.Errorf("state %s: Active() = %t, want %t", state, got, want)
		}
	}
}

// The operation id goes into a label and a path, the version into a
// privileged upgrade job: a bad value rejects the whole document.
func TestDoc_ValidateOperation(t *testing.T) {
	doc := func(id, v string) *Doc {
		return &Doc{StateStore: &StateStore{Enabled: true, Operation: &Operation{ID: id, TargetVersion: v, State: OperationPublished}}}
	}
	if err := doc("op-1", "v1.34.2+k3s1").Validate(); err != nil {
		t.Fatalf("valid operation rejected: %v", err)
	}
	for name, d := range map[string]*Doc{
		"path in id":        doc("../x", "v1.34.2+k3s1"),
		"empty id":          doc("", "v1.34.2+k3s1"),
		"long id":           doc(string(make([]byte, 64)), "v1.34.2+k3s1"),
		"channel":           doc("op-1", "stable"),
		"no k3s build":      doc("op-1", "v1.34.2"),
		"shell in version":  doc("op-1", "v1.34.2+k3s1;id"),
		"major 2":           doc("op-1", "v2.0.0+k3s1"),
		"leading space":     doc("op-1", " v1.34.2+k3s1"),
		"rc version":        doc("op-1", "v1.34.2-rc1+k3s1"),
		"missing version":   doc("op-1", ""),
		"newline in id":     doc("op-1\n", "v1.34.2+k3s1"),
		"label-invalid end": doc("op-", "v1.34.2+k3s1"),
	} {
		if err := d.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
