// Package publisher owns the OUTBOUND-ONLY delivery of live-view payloads to
// kubehz-api. There is no inbound listener, no port, and no server anywhere in
// this package — the agent only ever DIALS OUT. It reuses the P0 agent-token
// identity verbatim: the payload is POSTed to the existing heartbeat endpoint
// with `Authorization: Bearer <A>`, exactly like the bash agent, so the
// server's authenticated-heartbeat ratchet applies unchanged.
package publisher

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// heartbeatPath is the live P0 endpoint (kubehz-api
// server/api/clusters/[id]/heartbeat.ts). The schema-2 payload rides the same
// route: the non-strict HeartbeatSchema accepts the additive fields today and
// the API extension (managed-platform-spec §2) reads them once it lands.
const heartbeatPath = "/api/clusters/%s/heartbeat"

// AuthError signals a 401/403 from the server — the token is unknown/revoked or
// bound to another cluster. It is surfaced (not silently retried forever) so
// operators see an identity problem, though the sender still keeps trying: the
// only recovery is a redeploy/rotation, which is out of the agent's scope.
type AuthError struct{ Status int }

func (e *AuthError) Error() string {
	return fmt.Sprintf("agent token rejected by server (HTTP %d) — token unknown, revoked, or bound to another cluster", e.Status)
}

// maxResponseBytes bounds how much of a heartbeat response body is ever read:
// the only thing the agent consumes is availableUpdates (≤256 entries of three
// short strings — well under 128 KiB), so anything larger is a misbehaving
// server and gets truncated into a failed parse, not a memory balloon.
const maxResponseBytes = 128 << 10

// Publisher performs a single authenticated POST of a payload. Timing (debounce,
// min-gap, backoff) lives in the Coalescer/Sender; the Publisher is one shot.
type Publisher struct {
	client    *http.Client
	url       string
	token     string
	userAgent string

	// onUpdates consumes the availableUpdates the server computes from the
	// reported inventory and returns in the 200 body (nil = the body is
	// discarded, the pre-inventory behaviour). Set once via
	// OnAvailableUpdates before the Sender starts.
	onUpdates func(context.Context, []state.AvailableUpdate)
}

// heartbeatResponse is the slice of the response body the agent consumes.
// Everything else in the body is deliberately ignored (the server owns its
// response shape; the agent depends only on this one additive key).
type heartbeatResponse struct {
	AvailableUpdates []state.AvailableUpdate `json:"availableUpdates"`
}

// DefaultHTTPClient is the hardened client every outbound kubehz-api call
// uses when the caller does not inject one: it clones the default transport
// (keeps proxy support, HTTP/2, dial and idle-conn tuning) and pins the TLS
// floor explicitly. Go's client default already is 1.2, but for a
// credential-bearing request in a security-audited binary the floor should be
// declared, not implied. Shared by the heartbeat Publisher and the
// desired-state pull client so both halves of the outbound loop stay on one
// audited configuration.
func DefaultHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &http.Client{Timeout: 15 * time.Second, Transport: tr}
}

// New builds a Publisher targeting apiURL for clusterID with bearer token A.
// httpClient may be nil (DefaultHTTPClient is used). clusterID is
// path-escaped: config validates its shape, but the URL must stay well-formed
// even if a caller skips that validation.
func New(apiURL, clusterID, token, agentVersion string, httpClient *http.Client) *Publisher {
	if httpClient == nil {
		httpClient = DefaultHTTPClient()
	}
	return &Publisher{
		client:    httpClient,
		url:       strings.TrimRight(apiURL, "/") + fmt.Sprintf(heartbeatPath, url.PathEscape(clusterID)),
		token:     token,
		userAgent: "kubehz-agent/" + agentVersion,
	}
}

// URL is the full endpoint (exposed for logging without leaking the token).
func (p *Publisher) URL() string { return p.url }

// OnAvailableUpdates registers the consumer for the availableUpdates the
// server returns in a 2xx heartbeat response. NOT safe to call once the
// Sender is running — wire it during startup, before the first Enqueue.
func (p *Publisher) OnAvailableUpdates(fn func(ctx context.Context, updates []state.AvailableUpdate)) {
	p.onUpdates = fn
}

// Publish marshals and POSTs the payload once. It returns:
//   - nil on 2xx;
//   - *AuthError on 401/403 (identity problem);
//   - a generic error on any other status or transport failure (retryable).
func (p *Publisher) Publish(ctx context.Context, payload *state.Payload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", p.userAgent)
	// The ONLY credential the agent ever sends: bearer A, outbound.
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("post heartbeat: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		p.consumeUpdates(ctx, resp.Body)
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return &AuthError{Status: resp.StatusCode}
	default:
		return fmt.Errorf("heartbeat rejected: HTTP %d", resp.StatusCode)
	}
}

// consumeUpdates parses availableUpdates out of a 2xx body and hands them to
// the registered consumer. FAIL-SOFT in every direction: no consumer, an
// unreadable/over-long/non-JSON body (today's API returns a different shape —
// that must never fail the beat), or an empty list all mean "do nothing".
// The beat itself already succeeded; this is a bonus read.
func (p *Publisher) consumeUpdates(ctx context.Context, body io.Reader) {
	if p.onUpdates == nil {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBytes))
	if err != nil {
		return
	}
	var hr heartbeatResponse
	if json.Unmarshal(raw, &hr) != nil || len(hr.AvailableUpdates) == 0 {
		return
	}
	p.onUpdates(ctx, hr.AvailableUpdates)
}
