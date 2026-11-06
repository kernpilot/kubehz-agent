package k3supgrade

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

func TestCheckStep(t *testing.T) {
	srv := func(v string) nodeState { return nodeState{name: "cp", server: true, kubelet: v} }
	wrk := func(v string) nodeState { return nodeState{name: "w", kubelet: v} }
	cases := []struct {
		name   string
		target string
		nodes  []nodeState
		ok     bool
	}{
		{"patch step", "v1.33.6+k3s1", []nodeState{srv("v1.33.5+k3s1")}, true},
		{"one minor", "v1.34.2+k3s1", []nodeState{srv("v1.33.5+k3s1"), wrk("v1.33.5+k3s1")}, true},
		{"rerun at target", "v1.34.2+k3s1", []nodeState{srv("v1.34.2+k3s1")}, true},
		{"halfway", "v1.34.2+k3s1", []nodeState{srv("v1.34.2+k3s1"), wrk("v1.33.5+k3s1")}, true},
		{"two minors", "v1.35.0+k3s1", []nodeState{srv("v1.33.5+k3s1")}, false},
		{"downgrade", "v1.33.4+k3s1", []nodeState{srv("v1.33.5+k3s1")}, false},
		{"build downgrade", "v1.33.5+k3s1", []nodeState{srv("v1.33.5+k3s2")}, false},
		{"worker newer", "v1.34.2+k3s1", []nodeState{srv("v1.34.1+k3s1"), wrk("v1.34.3+k3s1")}, false},
		{"no server", "v1.34.2+k3s1", []nodeState{wrk("v1.33.5+k3s1")}, false},
		{"not k3s", "v1.34.2", []nodeState{srv("v1.33.5+k3s1")}, false},
		{"node not k3s", "v1.34.2+k3s1", []nodeState{srv("v1.33.5")}, false},
		{"lowest server counts", "v1.35.1+k3s1", []nodeState{srv("v1.34.2+k3s1"), {name: "cp2", server: true, kubelet: "v1.33.5+k3s1"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkStep(c.target, c.nodes)
			if (err == nil) != c.ok {
				t.Fatalf("checkStep(%s) = %v, want ok=%t", c.target, err, c.ok)
			}
			if err != nil && !errors.Is(err, errStep) {
				t.Fatalf("error %v does not wrap errStep", err)
			}
		})
	}
}

func TestAllAtTarget(t *testing.T) {
	ok := nodeState{name: "cp", server: true, kubelet: targetV, apiserver: targetV, ready: true}
	if !allAtTarget([]nodeState{ok}, targetV) {
		t.Fatal("ready server at target is not at target")
	}
	if allAtTarget(nil, targetV) {
		t.Fatal("no nodes counts as done")
	}
	notReady := ok
	notReady.ready = false
	oldAPI := ok
	oldAPI.apiserver = ""
	for name, n := range map[string]nodeState{"not ready": notReady, "apiserver unknown": oldAPI} {
		if allAtTarget([]nodeState{n}, targetV) {
			t.Errorf("%s counts as at target", name)
		}
	}
}

func TestHTTPReporter(t *testing.T) {
	var gotAuth []string
	var body Report
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/clusters/c1/state/upgrades/op-1/report" {
			t.Errorf("request %s %s", r.Method, r.URL.Path)
		}
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") != "Bearer new" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	h := NewHTTPReporter(srv.URL, "c1", "old", "test", srv.Client())
	err := h.Report(context.Background(), "op-1", Report{Phase: PhaseRunning})
	var ae *publisher.AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("no reloader: err = %v, want AuthError", err)
	}
	h.SetTokenReloader(func(context.Context) (string, error) { return "new", nil })
	if err := h.Report(context.Background(), "op-1", Report{Phase: PhaseDone, Servers: []NodeReport{{Node: "cp-1", KubeletVersion: targetV, APIServerVersion: targetV, Ready: true}}}); err != nil {
		t.Fatalf("after reload: %v", err)
	}
	if body.Phase != PhaseDone || body.Servers[0].APIServerVersion != targetV {
		t.Fatalf("body %+v", body)
	}
	if gotAuth[len(gotAuth)-1] != "Bearer new" {
		t.Fatalf("auth headers %v", gotAuth)
	}
}
