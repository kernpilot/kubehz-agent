package desired

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/kernpilot/kubehz-agent/internal/publisher"
)

// desiredPath is the agent-facing pull endpoint (kubehz-api
// server/api/clusters/[id]/desired.ts). Same identifier semantics as the
// heartbeat: the canonical cl-<uuid8> id or the cluster domain.
const desiredPath = "/api/clusters/%s/desired"

// maxBodyBytes bounds the decoded response. The server caps pools per cluster,
// but the agent validates its input at the boundary regardless — a mis-served
// megabyte document must not become agent memory.
const maxBodyBytes = 1 << 20

// Client performs the conditional GET of the desired-state document. It
// remembers the last ETag and sends If-None-Match, so the steady state is a
// cheap 304 with no body. Auth is the SAME bearer A the heartbeat uses —
// the agent holds exactly one credential.
type Client struct {
	client    *http.Client
	url       string
	token     string
	userAgent string

	mu   sync.Mutex
	etag string // last seen ETag; empty = no cached representation
}

// NewClient builds a Client. httpClient may be nil
// (publisher.DefaultHTTPClient: 15s timeout, TLS 1.2 floor).
func NewClient(apiURL, clusterID, token, agentVersion string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = publisher.DefaultHTTPClient()
	}
	return &Client{
		client:    httpClient,
		url:       strings.TrimRight(apiURL, "/") + fmt.Sprintf(desiredPath, url.PathEscape(clusterID)),
		token:     token,
		userAgent: "kubehz-agent/" + agentVersion,
	}
}

// URL is the full endpoint (exposed for logging without leaking the token).
func (c *Client) URL() string { return c.url }

// Fetch performs one conditional GET. It returns:
//   - (doc, false, nil) on 200 — a (re)changed document, ETag cached;
//   - (nil, true, nil) on 304 — unchanged since the last 200;
//   - (nil, false, *publisher.AuthError) on 401/403 — token revoked/unknown or
//     bound to another cluster (the caller must honor the FULL backoff, same
//     as the heartbeat sender's auth-error discipline);
//   - (nil, false, err) on any other status or transport/decode failure.
//
// A 4xx (including auth) never yields a document, so the caller's report-only
// posture — act on nothing — follows for free.
func (c *Client) Fetch(ctx context.Context) (*Doc, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	// The ONLY credential the agent ever sends: bearer A, outbound.
	req.Header.Set("Authorization", "Bearer "+c.token)
	if etag := c.currentETag(); etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("get desired state: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return nil, true, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var doc Doc
		dec := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes))
		if err := dec.Decode(&doc); err != nil {
			return nil, false, fmt.Errorf("decode desired doc: %w", err)
		}
		if err := doc.Validate(); err != nil {
			return nil, false, err
		}
		// Cache the (strong) ETag for the next conditional GET. A missing
		// header clears the cache — better an unconditional re-fetch than a
		// stale If-None-Match 304-ing a real change into oblivion.
		c.setETag(resp.Header.Get("ETag"))
		return &doc, false, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, false, &publisher.AuthError{Status: resp.StatusCode}
	default:
		return nil, false, fmt.Errorf("desired state rejected: HTTP %d", resp.StatusCode)
	}
}

func (c *Client) currentETag() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.etag
}

func (c *Client) setETag(etag string) {
	c.mu.Lock()
	c.etag = etag
	c.mu.Unlock()
}
