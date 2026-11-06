package k3supgrade

import (
	"bytes"
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

// Report phases the agent sends. The platform keeps the operation state;
// the agent reports what it saw and did.
const (
	PhaseRunning   = "running"
	PhaseDone      = "done"
	PhaseFailed    = "failed"
	PhaseCancelled = "cancelled"
)

// Report is the body of POST /api/clusters/:id/state/upgrades/:operationId/report
// (build contract 4.4).
type Report struct {
	Phase   string       `json:"phase"`
	Servers []NodeReport `json:"servers"`
	Agents  []NodeReport `json:"agents"`
	Error   string       `json:"error,omitempty"`
}

// NodeReport is one node of a Report. APIServerVersion is set for servers
// only: the version that the apiserver on that node serves.
type NodeReport struct {
	Node             string `json:"node"`
	KubeletVersion   string `json:"kubeletVersion"`
	APIServerVersion string `json:"apiserverVersion,omitempty"`
	Ready            bool   `json:"ready"`
}

// Reporter sends an operation report to the platform.
type Reporter interface {
	Report(ctx context.Context, operationID string, r Report) error
}

// reportPath is the agent route of build contract 4.4.
const reportPath = "/api/clusters/%s/state/upgrades/%s/report"

// maxReportResponse bounds how much of a response body is read.
const maxReportResponse = 64 << 10

// HTTPReporter posts reports with bearer A, the same credential the
// heartbeat and the desired-state poll use.
type HTTPReporter struct {
	client    *http.Client
	base      string
	clusterID string
	userAgent string

	mu     sync.Mutex
	token  string
	reload publisher.TokenReloader
}

// NewHTTPReporter builds an HTTPReporter. httpClient may be nil
// (publisher.DefaultHTTPClient).
func NewHTTPReporter(apiURL, clusterID, token, agentVersion string, httpClient *http.Client) *HTTPReporter {
	if httpClient == nil {
		httpClient = publisher.DefaultHTTPClient()
	}
	return &HTTPReporter{
		client:    httpClient,
		base:      strings.TrimRight(apiURL, "/"),
		clusterID: clusterID,
		token:     token,
		userAgent: "kubehz-agent/" + agentVersion,
	}
}

// SetTokenReloader registers how to re-read bearer A after a 401/403.
func (h *HTTPReporter) SetTokenReloader(fn publisher.TokenReloader) {
	h.mu.Lock()
	h.reload = fn
	h.mu.Unlock()
}

// Report implements Reporter. A 401/403 re-reads the token once and sends
// again when it changed.
func (h *HTTPReporter) Report(ctx context.Context, operationID string, r Report) error {
	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	for attempt := 0; ; attempt++ {
		status, err := h.post(ctx, operationID, body)
		if err != nil {
			return err
		}
		switch {
		case status >= 200 && status < 300:
			return nil
		case (status == http.StatusUnauthorized || status == http.StatusForbidden) && attempt == 0 && h.reloadToken(ctx):
			continue
		case status == http.StatusUnauthorized || status == http.StatusForbidden:
			return &publisher.AuthError{Status: status}
		default:
			return fmt.Errorf("upgrade report for operation %s rejected: HTTP %d", operationID, status)
		}
	}
}

func (h *HTTPReporter) post(ctx context.Context, operationID string, body []byte) (int, error) {
	u := h.base + fmt.Sprintf(reportPath, url.PathEscape(h.clusterID), url.PathEscape(operationID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build report request: %w", err)
	}
	h.mu.Lock()
	token := h.token
	h.mu.Unlock()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", h.userAgent)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post upgrade report: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReportResponse))
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func (h *HTTPReporter) reloadToken(ctx context.Context) bool {
	h.mu.Lock()
	fn := h.reload
	h.mu.Unlock()
	if fn == nil {
		return false
	}
	tok, err := fn(ctx)
	if err != nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if tok == h.token {
		return false
	}
	h.token = tok
	return true
}
