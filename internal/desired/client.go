package desired

import (
	"context"
	"encoding/json"
	"errors"
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
	userAgent string

	mu     sync.Mutex
	token  string
	reload publisher.TokenReloader // nil = the token cannot change (env)
	etag   string                  // last seen ETag; empty = no cached representation
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

// SetTokenReloader registers how to re-read bearer A after a rejection —
// the same reloader the heartbeat Publisher uses (one credential, one
// source). Wire it before the Poller starts; nil disables reloading.
func (c *Client) SetTokenReloader(fn publisher.TokenReloader) {
	c.mu.Lock()
	c.reload = fn
	c.mu.Unlock()
}

// ReloadToken re-reads bearer A; true when it changed. See
// publisher.Publisher.ReloadToken for the contract.
func (c *Client) ReloadToken(ctx context.Context) (bool, error) {
	c.mu.Lock()
	fn := c.reload
	c.mu.Unlock()
	if fn == nil {
		return false, nil
	}
	tok, err := fn(ctx)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if tok == c.token {
		return false, nil
	}
	c.token = tok
	return true, nil
}

func (c *Client) bearer() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.token
}

// RejectedError is the platform's DEFINITIVE refusal: it answered, and the
// answer is no document the agent may act on. That is a 4xx other than 408
// and 429 (401/403 stay *publisher.AuthError, the marker the full-backoff
// discipline keys on), or a 2xx body that fails validation. The poller
// treats both kinds the same way: it drops the cached document and its ETag,
// and acts again only on a NEW document. A 2xx body is invalid when it is
// complete JSON that breaks the contract: a missing or null field, a value
// out of range, a field of the wrong JSON type, or a body above
// maxBodyBytes. Transport failures, 5xx, 408, 429 and a body that is not
// complete JSON (a transfer cut short) are not rejections; they are
// transient.
type RejectedError struct {
	// Status is the HTTP status of the answer (a 2xx for an invalid document).
	Status int
	Err    error
}

func (e *RejectedError) Error() string { return e.Err.Error() }
func (e *RejectedError) Unwrap() error { return e.Err }

// IsRejection reports whether err is a definitive refusal by the platform:
// an auth failure or a RejectedError.
func IsRejection(err error) bool {
	var authErr *publisher.AuthError
	var rejected *RejectedError
	return errors.As(err, &authErr) || errors.As(err, &rejected)
}

// Fetch performs one conditional GET. It returns:
//   - (doc, false, nil) on 200 — a (re)changed document, ETag cached;
//   - (nil, true, nil) on 304 — unchanged since the last 200;
//   - (nil, false, *publisher.AuthError) on 401/403 — token revoked/unknown or
//     bound to another cluster (the caller must honor the FULL backoff, same
//     as the heartbeat sender's auth-error discipline);
//   - (nil, false, *RejectedError) on another definitive refusal (see there);
//   - (nil, false, err) on any other status or transport/decode failure.
//
// A 4xx (including auth) never yields a document, so the caller's report-only
// posture — act on nothing — follows for free.
func (c *Client) Fetch(ctx context.Context) (*Doc, bool, error) {
	return c.fetch(ctx, false)
}

// Revalidate performs one UNCONDITIONAL GET: no If-None-Match, so no cache
// can answer it with a 304. It exists because a 304 proves nothing about
// freshness — any cache between the agent and the platform can serve one from
// a frozen representation, and the agent must not act forever on intent
// nobody re-affirmed (see the Poller's freshness bounds).
//
// A 304 has no meaning without a conditional header, so one is reported as an
// error here: the answer is a broken cache, not an affirmation.
func (c *Client) Revalidate(ctx context.Context) (*Doc, bool, error) {
	doc, notModified, err := c.fetch(ctx, true)
	if err == nil && notModified {
		return nil, false, fmt.Errorf("desired state answered 304 to an unconditional request (a cache is serving a frozen copy)")
	}
	return doc, notModified, err
}

// ForgetETag drops the cached ETag, so the next Fetch is unconditional. The
// poller calls it on a rejection: a 304 to a validator from BEFORE the
// rejection must not bring the revoked document back.
func (c *Client) ForgetETag() { c.setETag("") }

// fetch is the shared request path. revalidate drops the conditional header.
func (c *Client) fetch(ctx context.Context, revalidate bool) (*Doc, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	// The ONLY credential the agent ever sends: bearer A, outbound.
	req.Header.Set("Authorization", "Bearer "+c.bearer())
	// EVERY request asks for the origin's own answer, the conditional ones
	// too. A cache that obeys no-cache must check with the platform before it
	// serves a stored copy, so neither a stored 200 nor a stored 304 can stand
	// in for the platform. (A cache that ignores the header is the reason for
	// the Poller's freshness bounds.)
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	if !revalidate {
		if etag := c.currentETag(); etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
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
		// One byte more than the bound, so an oversized body is seen as
		// oversized and not as a cut-short one.
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		if err != nil {
			return nil, false, fmt.Errorf("read desired doc: %w", err)
		}
		doc, err := decodeDoc(body)
		if err != nil {
			var rejected *RejectedError
			if errors.As(err, &rejected) {
				rejected.Status = resp.StatusCode
			}
			return nil, false, err
		}
		// Cache the (strong) ETag for the next conditional GET. A missing
		// header clears the cache — better an unconditional re-fetch than a
		// stale If-None-Match 304-ing a real change into oblivion.
		c.setETag(resp.Header.Get("ETag"))
		return doc, false, nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, false, &publisher.AuthError{Status: resp.StatusCode}
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests:
		return nil, false, fmt.Errorf("desired state not served: HTTP %d (retrying)", resp.StatusCode)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return nil, false, &RejectedError{Status: resp.StatusCode,
			Err: fmt.Errorf("desired state rejected: HTTP %d", resp.StatusCode)}
	default:
		return nil, false, fmt.Errorf("desired state rejected: HTTP %d", resp.StatusCode)
	}
}

// decodeDoc decodes and validates one 200 body. A body that is not complete
// JSON is a plain error: it can be a transfer cut short, and the next poll
// can bring the whole document. Everything else that fails is the platform's
// complete answer and a *RejectedError: a body above maxBodyBytes, and JSON
// that breaks the contract, a field of the wrong JSON type included (for
// example "desiredReplicas":"3"). Such a document can carry flags that turn
// execution off, so it must revoke the cached one, never leave it in force.
func decodeDoc(body []byte) (*Doc, error) {
	if len(body) > maxBodyBytes {
		return nil, &RejectedError{Err: fmt.Errorf("desired doc exceeds %d bytes", maxBodyBytes)}
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("decode desired doc: the body is not complete JSON")
	}
	var doc Doc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, &RejectedError{Err: fmt.Errorf("decode desired doc: %w", err)}
	}
	var present docPresence
	if err := json.Unmarshal(body, &present); err != nil {
		return nil, &RejectedError{Err: fmt.Errorf("decode desired doc: %w", err)}
	}
	if err := present.check(); err != nil {
		return nil, &RejectedError{Err: err}
	}
	if err := doc.Validate(); err != nil {
		return nil, &RejectedError{Err: err}
	}
	return &doc, nil
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
