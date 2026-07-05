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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// Publisher performs a single authenticated POST of a payload. Timing (debounce,
// min-gap, backoff) lives in the Coalescer/Sender; the Publisher is one shot.
type Publisher struct {
	client    *http.Client
	url       string
	token     string
	userAgent string
}

// New builds a Publisher targeting apiURL for clusterID with bearer token A.
// httpClient may be nil (a sane default with a request timeout is used).
func New(apiURL, clusterID, token, agentVersion string, httpClient *http.Client) *Publisher {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Publisher{
		client:    httpClient,
		url:       strings.TrimRight(apiURL, "/") + fmt.Sprintf(heartbeatPath, clusterID),
		token:     token,
		userAgent: "kubehz-agent/" + agentVersion,
	}
}

// URL is the full endpoint (exposed for logging without leaking the token).
func (p *Publisher) URL() string { return p.url }

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
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return &AuthError{Status: resp.StatusCode}
	default:
		return fmt.Errorf("heartbeat rejected: HTTP %d", resp.StatusCode)
	}
}
