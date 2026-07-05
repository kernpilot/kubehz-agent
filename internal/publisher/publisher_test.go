package publisher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

const testToken = "khz_agt_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func samplePayload() *state.Payload {
	return &state.Payload{
		Schema:     state.SchemaVersion,
		ClusterID:  "kubehz.in.net",
		Timestamp:  "2026-07-05T00:00:00Z",
		Agent:      state.AgentMeta{Version: "0.1.0", Mode: state.ModeOperator},
		Kubernetes: state.KubeInfo{Version: "v1.35.5"},
		Nodes:      []state.NodeState{{Name: "cp-1", Status: "Ready", Ready: true, Roles: "control-plane"}},
	}
}

// TestPublish_SendsBearerAndCorrectRequest is the core identity test: the agent
// reuses the P0 agent-token by sending `Authorization: Bearer <A>` to the
// existing heartbeat endpoint.
func TestPublish_SendsBearerAndCorrectRequest(t *testing.T) {
	var gotAuth, gotCT, gotMethod, gotPath, gotUA string
	var gotBody state.Payload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		gotUA = r.Header.Get("User-Agent")
		gotMethod = r.Method
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	if err := p.Publish(context.Background(), samplePayload()); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}

	if gotAuth != "Bearer "+testToken {
		t.Errorf("Authorization = %q, want Bearer <token>", gotAuth)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/clusters/kubehz.in.net/heartbeat" {
		t.Errorf("path = %q", gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type = %q", gotCT)
	}
	if gotUA != "kubehz-agent/0.1.0" {
		t.Errorf("user-agent = %q", gotUA)
	}
	if gotBody.ClusterID != "kubehz.in.net" || gotBody.Schema != state.SchemaVersion {
		t.Errorf("decoded body wrong: %+v", gotBody)
	}
}

func TestPublish_AuthErrorOn401And403(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		}))
		p := New(srv.URL, "d", testToken, "0.1.0", srv.Client())
		err := p.Publish(context.Background(), samplePayload())
		var authErr *AuthError
		if err == nil || !asAuthError(err, &authErr) {
			t.Errorf("status %d: expected *AuthError, got %v", code, err)
		} else if authErr.Status != code {
			t.Errorf("AuthError.Status = %d, want %d", authErr.Status, code)
		}
		srv.Close()
	}
}

func TestPublish_RetryableErrorOn500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := New(srv.URL, "d", testToken, "0.1.0", srv.Client())
	err := p.Publish(context.Background(), samplePayload())
	if err == nil {
		t.Fatal("expected error on 500")
	}
	var authErr *AuthError
	if asAuthError(err, &authErr) {
		t.Errorf("500 must NOT be an AuthError: %v", err)
	}
}

func TestNew_TrimsTrailingSlash(t *testing.T) {
	p := New("https://api.kubehz.cloud/", "kubehz.in.net", testToken, "0.1.0", nil)
	want := "https://api.kubehz.cloud/api/clusters/kubehz.in.net/heartbeat"
	if p.URL() != want {
		t.Errorf("URL() = %q, want %q", p.URL(), want)
	}
}

// asAuthError is errors.As without importing errors in every call site.
func asAuthError(err error, target **AuthError) bool {
	ae, ok := err.(*AuthError)
	if ok {
		*target = ae
	}
	return ok
}
