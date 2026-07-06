package desired

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

const testToken = "khz_agt_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const docBody = `{
  "revision": 3,
  "kubernetesVersion": null,
  "workerPools": [{"name": "pool-a", "machineType": "cpx31", "desiredReplicas": 3}],
  "execution": {"scaling": true, "upgrades": false}
}`

// The client must send bearer A + If-None-Match, treat 304 as "unchanged", and
// cache the strong ETag across calls — the steady state is a body-less 304.
func TestClient_ETagConditionalGet(t *testing.T) {
	const etag = `"3-10"`
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+testToken {
			t.Errorf("bad Authorization header: %q", got)
		}
		if r.URL.Path != "/api/clusters/kubehz.in.net/desired" {
			t.Errorf("bad path: %q", r.URL.Path)
		}
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = w.Write([]byte(docBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())

	doc, notModified, err := c.Fetch(context.Background())
	if err != nil || notModified {
		t.Fatalf("first fetch: doc=%v notModified=%v err=%v", doc, notModified, err)
	}
	if doc.Revision != 3 || !doc.Execution.Scaling || doc.Execution.Upgrades {
		t.Errorf("doc mis-decoded: %+v", doc)
	}
	if doc.KubernetesVersion != nil {
		t.Errorf("null kubernetesVersion must decode as nil, got %q", *doc.KubernetesVersion)
	}
	if len(doc.WorkerPools) != 1 || doc.WorkerPools[0].Name != "pool-a" || doc.WorkerPools[0].DesiredReplicas != 3 {
		t.Errorf("workerPools mis-decoded: %+v", doc.WorkerPools)
	}

	doc, notModified, err = c.Fetch(context.Background())
	if err != nil || !notModified || doc != nil {
		t.Fatalf("second fetch should be 304: doc=%v notModified=%v err=%v", doc, notModified, err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2", got)
	}
}

// 401/403 map to *publisher.AuthError — the marker the poller keys its
// full-backoff auth discipline on (same type the heartbeat sender uses).
func TestClient_AuthErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		c := NewClient(srv.URL, "d", testToken, "0.1.0", srv.Client())
		_, _, err := c.Fetch(context.Background())
		var authErr *publisher.AuthError
		if !errors.As(err, &authErr) || authErr.Status != status {
			t.Errorf("HTTP %d: want AuthError, got %v", status, err)
		}
		srv.Close()
	}
}

// Any other failure (5xx, other 4xx, garbage body, negative revision) is a
// generic error: no document, so the caller acts on nothing (report-only).
func TestClient_RejectsBadResponses(t *testing.T) {
	cases := []struct {
		name string
		h    http.HandlerFunc
	}{
		{"http 500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }},
		{"http 404", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) }},
		{"garbage body", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{nope")) }},
		{"negative revision", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"revision":-1,"kubernetesVersion":null,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`))
		}},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(tc.h)
		c := NewClient(srv.URL, "d", testToken, "0.1.0", srv.Client())
		doc, notModified, err := c.Fetch(context.Background())
		if err == nil || doc != nil || notModified {
			t.Errorf("%s: want error, got doc=%v notModified=%v err=%v", tc.name, doc, notModified, err)
		}
		var authErr *publisher.AuthError
		if errors.As(err, &authErr) {
			t.Errorf("%s: must not be an AuthError", tc.name)
		}
		srv.Close()
	}
}

// The URL is path-escaped even if a caller bypasses config validation.
func TestClient_EscapesClusterID(t *testing.T) {
	c := NewClient("https://api.kubehz.cloud", "a/../b", testToken, "0.1.0", nil)
	if strings.Contains(c.URL(), "a/../b") {
		t.Errorf("cluster ID not escaped: %s", c.URL())
	}
}
