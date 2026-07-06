package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestNew_PathEscapesClusterID: config rejects malformed cluster IDs, but the
// publisher must keep the URL well-formed regardless — a separator-bearing ID
// must not be able to change the request path.
func TestNew_PathEscapesClusterID(t *testing.T) {
	p := New("https://api.kubehz.cloud", "a/b?c#d e", testToken, "0.1.0", nil)
	want := "https://api.kubehz.cloud/api/clusters/a%2Fb%3Fc%23d%20e/heartbeat"
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

// TestPublish_ParsesAvailableUpdates: a 200 body carrying availableUpdates
// reaches the registered consumer with the exact parsed entries; extra keys
// in the response are ignored.
func TestPublish_ParsesAvailableUpdates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"status":"ok","availableUpdates":[`+
			`{"name":"cilium","current":"1.16.1","latest":"1.17.4"},`+
			`{"name":"rook-ceph","current":"1.20.2","latest":"1.20.4"}]}`)
	}))
	defer srv.Close()

	var got []state.AvailableUpdate
	p := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	p.OnAvailableUpdates(func(_ context.Context, updates []state.AvailableUpdate) { got = updates })

	if err := p.Publish(context.Background(), samplePayload()); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	want := []state.AvailableUpdate{
		{Name: "cilium", Current: "1.16.1", Latest: "1.17.4"},
		{Name: "rook-ceph", Current: "1.20.2", Latest: "1.20.4"},
	}
	if len(got) != len(want) {
		t.Fatalf("consumer got %d updates, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("update[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestPublish_AbsentKeyIsNoVerdict: availableUpdates is TRISTATE (kubehz-api
// f628c97): the server OMITS the key when the beat carried no inventory or
// the addons index was unreachable — precisely so the agent does not treat an
// outage as "no updates" and wipe its last known CR status. An absent/null
// key must therefore never reach the consumer; legacy/garbage/oversized
// bodies land in the same no-verdict bucket, and the beat still succeeds.
func TestPublish_AbsentKeyIsNoVerdict(t *testing.T) {
	for name, body := range map[string]string{
		"legacy-shape": `{"status":"ok"}`,
		"not-json":     `pong`,
		"empty-body":   ``,
		"null-key":     `{"availableUpdates":null}`,
		"oversized":    `{"availableUpdates":[{"name":"` + strings.Repeat("x", maxResponseBytes) + `"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprint(w, body)
			}))
			defer srv.Close()

			called := false
			p := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
			p.OnAvailableUpdates(func(context.Context, []state.AvailableUpdate) { called = true })

			if err := p.Publish(context.Background(), samplePayload()); err != nil {
				t.Fatalf("Publish returned error: %v", err)
			}
			if called {
				t.Error("consumer fired without a server verdict")
			}
		})
	}
}

// TestPublish_EmptyListIsAVerdict: a PRESENT-but-empty [] is the server's
// "index consulted, nothing is newer" verdict and MUST reach the consumer (as
// a non-nil empty slice) so stale CR status entries get cleared — swallowing
// it would leave a user who upgraded their addons staring at "update
// available" in the CR forever.
func TestPublish_EmptyListIsAVerdict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"status":"ok","availableUpdates":[]}`)
	}))
	defer srv.Close()

	called := false
	var got []state.AvailableUpdate
	p := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	p.OnAvailableUpdates(func(_ context.Context, updates []state.AvailableUpdate) {
		called = true
		got = updates
	})

	if err := p.Publish(context.Background(), samplePayload()); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if !called {
		t.Fatal("consumer did not fire for a present-but-empty availableUpdates")
	}
	if got == nil || len(got) != 0 {
		t.Errorf("consumer got %#v, want a non-nil empty slice", got)
	}
}

// TestPublish_NoConsumerNoRead: without a registered consumer the response
// body path stays exactly as before (drained + discarded) — no behavior
// change for the pre-inventory wiring.
func TestPublish_NoConsumerNoRead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{"availableUpdates":[{"name":"cilium"}]}`)
	}))
	defer srv.Close()

	p := New(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	if err := p.Publish(context.Background(), samplePayload()); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
}
