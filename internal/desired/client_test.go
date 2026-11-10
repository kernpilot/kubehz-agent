package desired

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

// The ETag is an OPAQUE token: whatever the server serves — the P5 3-bit
// format ("7-101"), the old 2-bit one, or any future shape — is cached and
// echoed VERBATIM in If-None-Match, never parsed. A format change therefore
// costs exactly one extra 200 (the cached old-format tag doesn't match),
// after which 304 polling resumes with the new tag.
func TestClient_ETagIsOpaque(t *testing.T) {
	// Three deliberately different formats, including the P5 3-bit one and a
	// shape that would crash any "revision-bits" parser.
	for _, etag := range []string{`"7-101"`, `"3-10"`, `"utterly/opaque token=42"`} {
		var mu sync.Mutex
		var mismatchedINM []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if inm := r.Header.Get("If-None-Match"); inm == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			} else if inm != "" {
				mu.Lock()
				mismatchedINM = append(mismatchedINM, inm)
				mu.Unlock()
			}
			w.Header().Set("ETag", etag)
			_, _ = w.Write([]byte(docBody))
		}))

		c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
		if _, _, err := c.Fetch(context.Background()); err != nil {
			t.Fatalf("etag %s: first fetch: %v", etag, err)
		}
		_, notModified, err := c.Fetch(context.Background())
		if err != nil || !notModified {
			t.Errorf("etag %s: second fetch must 304 via verbatim echo (notModified=%v err=%v)",
				etag, notModified, err)
		}
		mu.Lock()
		if len(mismatchedINM) != 0 {
			t.Errorf("etag %s: client mangled the token before echoing: %q", etag, mismatchedINM)
		}
		mu.Unlock()
		srv.Close()
	}
}

// A server-deployed ETag format change (2-bit → 3-bit) must cost one full 200
// and then resume 304s — the client re-caches the new opaque tag.
func TestClient_ETagFormatChangeRecaches(t *testing.T) {
	var current atomic.Value
	current.Store(`"3-10"`) // old 2-bit format cached first
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etag := current.Load().(string)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = w.Write([]byte(docBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	if _, _, err := c.Fetch(context.Background()); err != nil {
		t.Fatalf("first fetch: %v", err)
	}

	current.Store(`"3-100"`) // server upgraded to the 3-bit format, same revision
	doc, notModified, err := c.Fetch(context.Background())
	if err != nil || notModified || doc == nil {
		t.Fatalf("format change must yield one full 200: doc=%v notModified=%v err=%v", doc, notModified, err)
	}
	_, notModified, err = c.Fetch(context.Background())
	if err != nil || !notModified {
		t.Fatalf("after re-cache the steady state must be 304 again: notModified=%v err=%v", notModified, err)
	}
}

// The P5 healing block and execution.healing decode wire-identically to
// kubehz-api's desired doc.
func TestClient_DecodesHealingBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
		  "revision": 9,
		  "kubernetesVersion": "v1.35.6",
		  "workerPools": [],
		  "execution": {"scaling": false, "upgrades": false, "healing": true},
		  "healing": {"enabled": true, "maxUnhealthy": 2, "nodeStartupTimeoutSeconds": 600,
		              "unhealthyAfterSeconds": 300, "cooldownSeconds": 900}
		}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	doc, _, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !doc.Execution.Healing {
		t.Errorf("execution.healing mis-decoded: %+v", doc.Execution)
	}
	want := Healing{Enabled: true, MaxUnhealthy: 2, NodeStartupTimeoutSeconds: 600,
		UnhealthyAfterSeconds: 300, CooldownSeconds: 900}
	if doc.Healing != want {
		t.Errorf("healing = %+v, want %+v", doc.Healing, want)
	}
}

// A P3/P4 server that serves NO healing block decodes to the zero policy
// (enabled=false, all zeros) — report-only for healing, no error.
func TestClient_MissingHealingBlockIsDisabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(docBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	doc, _, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if doc.Healing != (Healing{}) || doc.Execution.Healing {
		t.Errorf("missing healing block must decode to disabled zero value, got %+v / %+v",
			doc.Healing, doc.Execution)
	}
}

// Healing guardrail numbers are validated at the boundary: negative or absurd
// values reject the WHOLE document (fail toward report-only) — a broken
// maxUnhealthy could otherwise disable the storm brake.
func TestDoc_ValidateHealingBounds(t *testing.T) {
	valid := func() *Doc {
		return &Doc{Revision: 1, Healing: Healing{
			MaxUnhealthy: 1, NodeStartupTimeoutSeconds: 600,
			UnhealthyAfterSeconds: 300, CooldownSeconds: 900,
		}}
	}
	if err := valid().Validate(); err != nil {
		t.Fatalf("valid doc rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Doc)
	}{
		{"negative maxUnhealthy", func(d *Doc) { d.Healing.MaxUnhealthy = -1 }},
		{"absurd maxUnhealthy", func(d *Doc) { d.Healing.MaxUnhealthy = 1_000_001 }},
		{"negative startup timeout", func(d *Doc) { d.Healing.NodeStartupTimeoutSeconds = -1 }},
		{"absurd startup timeout", func(d *Doc) { d.Healing.NodeStartupTimeoutSeconds = 2_000_000 }},
		{"negative unhealthyAfter", func(d *Doc) { d.Healing.UnhealthyAfterSeconds = -300 }},
		{"absurd unhealthyAfter", func(d *Doc) { d.Healing.UnhealthyAfterSeconds = 2_000_000 }},
		{"negative cooldown", func(d *Doc) { d.Healing.CooldownSeconds = -1 }},
		{"absurd cooldown", func(d *Doc) { d.Healing.CooldownSeconds = 2_000_000 }},
	}
	for _, tc := range cases {
		d := valid()
		tc.mutate(d)
		if err := d.Validate(); err == nil {
			t.Errorf("%s: want validation error, got nil", tc.name)
		}
	}

	// Zero values (P3/P4 server without the block) stay valid.
	if err := (&Doc{Revision: 1}).Validate(); err != nil {
		t.Errorf("zero healing block must validate: %v", err)
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

// Any other failure (5xx, other 4xx, garbage body, negative revision) is an
// error that is not an AuthError: no document, so the caller acts on nothing
// (report-only). TestClient_ClassifiesRejections covers which are rejections.
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

// Revalidate must ask the ORIGIN: no If-None-Match, plus no-cache so no
// intermediary may answer from a stored copy. It exists because a 304 proves
// nothing about freshness — anything in the path can serve one forever.
func TestClient_RevalidateBypassesCaches(t *testing.T) {
	const etag = `"3-10"`
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("ETag", etag)
		_, _ = w.Write([]byte(docBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	if _, _, err := c.Fetch(context.Background()); err != nil { // caches the ETag
		t.Fatalf("priming fetch: %v", err)
	}
	doc, notModified, err := c.Revalidate(context.Background())
	if err != nil || notModified || doc == nil {
		t.Fatalf("revalidate: doc=%v notModified=%v err=%v", doc, notModified, err)
	}
	if v := got.Get("If-None-Match"); v != "" {
		t.Errorf("revalidation sent If-None-Match %q — a conditional request can be answered 304 by any cache", v)
	}
	if v := got.Get("Cache-Control"); v != "no-cache" {
		t.Errorf("revalidation Cache-Control = %q, want no-cache", v)
	}
}

// A 304 to an unconditional request is a broken cache answering, never an
// affirmation — the client reports it as an error so the poller counts the
// poll as unaffirmed.
func TestClient_Revalidate304IsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	doc, notModified, err := c.Revalidate(context.Background())
	if err == nil {
		t.Fatalf("revalidate accepted a 304: doc=%v notModified=%v", doc, notModified)
	}
	if !strings.Contains(err.Error(), "304") {
		t.Errorf("error = %q, want it to name the 304", err.Error())
	}
	if doc != nil || notModified {
		t.Errorf("a rejected revalidation must yield no document: doc=%v notModified=%v", doc, notModified)
	}
}

// fetchBody serves body once and returns what Fetch makes of it.
func fetchBody(t *testing.T, body string) (*Doc, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	doc, _, err := NewClient(srv.URL, "d", testToken, "0.1.0", srv.Client()).Fetch(context.Background())
	return doc, err
}

// MISSING IS NOT ZERO. A pool without desiredReplicas would decode as 0, and
// the scaling executor would patch the pool to zero machines. An incomplete
// document is a rejection, never intent; only an explicit 0 means zero.
func TestClient_RejectsIncompleteIntent(t *testing.T) {
	const execOn = `"execution":{"scaling":true,"upgrades":false,"healing":true}`
	for name, body := range map[string]string{
		"pool without desiredReplicas": `{"revision":3,"kubernetesVersion":null,"workerPools":[{"name":"pool-a"}],` + execOn + `}`,
		"pool with null desiredReplicas": `{"revision":3,"kubernetesVersion":null,` +
			`"workerPools":[{"name":"pool-a","desiredReplicas":null}],` + execOn + `}`,
		"pool without a name": `{"revision":3,"kubernetesVersion":null,"workerPools":[{"desiredReplicas":3}],` + execOn + `}`,
		"armed healing without cooldownSeconds": `{"revision":3,"kubernetesVersion":null,"workerPools":[],` + execOn + `,` +
			`"healing":{"enabled":true,"maxUnhealthy":2,"nodeStartupTimeoutSeconds":600,"unhealthyAfterSeconds":300}}`,
		"armed healing with null maxUnhealthy": `{"revision":3,"kubernetesVersion":null,"workerPools":[],` + execOn + `,` +
			`"healing":{"enabled":true,"maxUnhealthy":null,"nodeStartupTimeoutSeconds":600,"unhealthyAfterSeconds":300,"cooldownSeconds":900}}`,
	} {
		t.Run(name, func(t *testing.T) {
			doc, err := fetchBody(t, body)
			if doc != nil {
				t.Fatalf("incomplete intent decoded into a document: %+v", doc)
			}
			var rejected *RejectedError
			if !errors.As(err, &rejected) || rejected.Status != http.StatusOK {
				t.Errorf("err = %v, want a *RejectedError carrying the 200", err)
			}
		})
	}

	for name, body := range map[string]string{
		"explicit zero replicas": `{"revision":3,"kubernetesVersion":null,` +
			`"workerPools":[{"name":"pool-a","desiredReplicas":0}],` + execOn + `}`,
		"healing off without numbers": `{"revision":3,"kubernetesVersion":null,"workerPools":[],` + execOn + `,` +
			`"healing":{"enabled":false}}`,
	} {
		t.Run(name, func(t *testing.T) {
			doc, err := fetchBody(t, body)
			if err != nil || doc == nil {
				t.Fatalf("complete intent rejected: %v", err)
			}
		})
	}
}

// The poller revokes the cached document on a REJECTION and keeps it on a
// transient failure, so the classification is part of the contract.
func TestClient_ClassifiesRejections(t *testing.T) {
	for name, tc := range map[string]struct {
		h         http.HandlerFunc
		rejection bool
	}{
		"401": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) }, true},
		"403": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(403) }, true},
		"404": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) }, true},
		"410": {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(410) }, true},
		"negative revision": {func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"revision":-1,"kubernetesVersion":null,"workerPools":[],"execution":{"scaling":false,"upgrades":false}}`))
		}, true},
		"408":          {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(408) }, false},
		"429":          {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429) }, false},
		"500":          {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }, false},
		"503":          {func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, false},
		"garbage body": {func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{nope")) }, false},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(tc.h)
			defer srv.Close()
			_, _, err := NewClient(srv.URL, "d", testToken, "0.1.0", srv.Client()).Fetch(context.Background())
			if err == nil {
				t.Fatal("no error")
			}
			if got := IsRejection(err); got != tc.rejection {
				t.Errorf("IsRejection(%v) = %v, want %v", err, got, tc.rejection)
			}
		})
	}
}

// After ForgetETag the next Fetch carries no validator, so a 304 cannot bring
// back a representation the platform revoked. Every request, the conditional
// one too, asks for the origin's own answer (no-cache).
func TestClient_ForgetETagAndNoCache(t *testing.T) {
	var mu sync.Mutex
	var seen []http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("ETag", `"3-10"`)
		_, _ = w.Write([]byte(docBody))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "kubehz.in.net", testToken, "0.1.0", srv.Client())
	for i := 0; i < 2; i++ {
		if _, _, err := c.Fetch(context.Background()); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
	}
	c.ForgetETag()
	if _, _, err := c.Fetch(context.Background()); err != nil {
		t.Fatalf("fetch after ForgetETag: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := seen[1].Get("If-None-Match"); got != `"3-10"` {
		t.Fatalf("second fetch If-None-Match = %q, want the cached ETag", got)
	}
	if got := seen[2].Get("If-None-Match"); got != "" {
		t.Errorf("fetch after ForgetETag sent If-None-Match %q", got)
	}
	for i, h := range seen {
		if h.Get("Cache-Control") != "no-cache" || h.Get("Pragma") != "no-cache" {
			t.Errorf("request %d: Cache-Control %q, Pragma %q, want no-cache on every request",
				i, h.Get("Cache-Control"), h.Get("Pragma"))
		}
	}
}
