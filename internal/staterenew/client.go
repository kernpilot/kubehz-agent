package staterenew

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Machine routes of build contract 4.2.
const (
	pathRedeem = "/api/state/redeem"
	pathCheck  = "/api/state/renew/check"
	pathRenew  = "/api/state/renew"
	pathAck    = "/api/state/renew/ack"
)

const maxResponse = 1 << 20

// CheckRequest is the body of renew/check. The contract does not name a
// body; K3sVersion is the local k3s version that design 4.4 step 4 needs
// for a rollback, and LastError is the last failure of this renewer.
type CheckRequest struct {
	K3sVersion string `json:"k3sVersion,omitempty"`
	LastError  string `json:"lastError,omitempty"`
}

// CheckResponse is the answer of renew/check.
type CheckResponse struct {
	Due    bool   `json:"due"`
	Reason string `json:"reason"`
	Paused bool   `json:"paused"`
}

// RenewRequest is the body of renew. K3sVersion is not in the contract yet
// (design 4.4 step 4).
type RenewRequest struct {
	CSR        string `json:"csr"`
	K3sVersion string `json:"k3sVersion,omitempty"`
}

// RenewResponse is the answer of renew: a PENDING certificate.
type RenewResponse struct {
	Certificate  string `json:"certificate"`
	CAGeneration int64  `json:"caGeneration"`
}

// AckResponse is the answer of renew/ack.
type AckResponse struct {
	Serial string `json:"serial"`
}

// RedeemRequest is the body of redeem.
type RedeemRequest struct {
	Ticket       string `json:"ticket"`
	CSR          string `json:"csr"`
	MachineLabel string `json:"machineLabel"`
	Role         string `json:"role"`
}

// RedeemResponse is the answer of redeem. AgentToken is not in the build
// contract yet: the install needs it to register the in-cluster agent.
type RedeemResponse struct {
	MachineID     string        `json:"machineId"`
	Certificate   string        `json:"certificate"`
	ServingCA     string        `json:"servingCA"`
	Endpoint      string        `json:"endpoint"`
	K3sToken      string        `json:"k3sToken"`
	InstallConfig InstallConfig `json:"installConfig"`
	AgentToken    string        `json:"agentToken,omitempty"`
}

// InstallConfig is the stored install configuration (design 4.2 step 3).
// The build contract names the field but not its shape; this is the shape
// the install reads. Every field is optional except ClusterID.
type InstallConfig struct {
	ClusterID            string        `json:"clusterId"`
	K3sVersion           string        `json:"k3sVersion,omitempty"`
	ClusterCIDR          string        `json:"clusterCidr,omitempty"`
	ServiceCIDR          string        `json:"serviceCidr,omitempty"`
	TLSSANs              []string      `json:"tlsSans,omitempty"`
	AuthenticationConfig string        `json:"authenticationConfig,omitempty"`
	RoleBindings         []RoleBinding `json:"roleBindings,omitempty"`
}

// RoleBinding is one static SSO binding of design 3.4.
type RoleBinding struct {
	Name        string   `json:"name"`
	ClusterRole string   `json:"clusterRole"`
	Groups      []string `json:"groups"`
}

// StatusError is a non-2xx answer of the api.
type StatusError struct {
	Path   string
	Status int
	Detail string
}

func (e *StatusError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s answered HTTP %d: %s", e.Path, e.Status, e.Detail)
	}
	return fmt.Sprintf("%s answered HTTP %d", e.Path, e.Status)
}

// Permanent reports whether a retry of the same request cannot succeed.
func (e *StatusError) Permanent() bool {
	return e.Status >= 400 && e.Status < 500 && e.Status != http.StatusRequestTimeout && e.Status != http.StatusTooManyRequests
}

func isPermanent(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Permanent()
}

// Client calls the machine routes.
type Client struct {
	http      *http.Client
	base      *url.URL
	clusterID string
	userAgent string
	now       func() time.Time
}

// NewClient builds a Client for apiURL (https; http only for a loopback
// host). httpClient may be nil.
func NewClient(apiURL, clusterID, userAgent string, httpClient *http.Client) (*Client, error) {
	u, err := checkAPIURL(apiURL)
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		httpClient = &http.Client{Timeout: 30 * time.Second, Transport: tr}
	}
	return &Client{http: httpClient, base: u, clusterID: clusterID, userAgent: userAgent, now: time.Now}, nil
}

func checkAPIURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(raw), "/"))
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("api URL %q is not an absolute URL", raw)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if h := u.Hostname(); h != "localhost" && h != "127.0.0.1" && h != "::1" {
			return nil, fmt.Errorf("api URL %q uses plain http; use https", raw)
		}
	default:
		return nil, fmt.Errorf("api URL %q must use https", raw)
	}
	return u, nil
}

// Check calls renew/check, signed with the current key.
func (c *Client) Check(ctx context.Context, s Signer, req CheckRequest) (CheckResponse, error) {
	var out CheckResponse
	return out, c.do(ctx, &s, pathCheck, req, &out)
}

// Renew calls renew, signed with the current key.
func (c *Client) Renew(ctx context.Context, s Signer, req RenewRequest) (RenewResponse, error) {
	var out RenewResponse
	return out, c.do(ctx, &s, pathRenew, req, &out)
}

// Ack calls renew/ack, signed with the NEW key.
func (c *Client) Ack(ctx context.Context, s Signer) (AckResponse, error) {
	var out AckResponse
	return out, c.do(ctx, &s, pathAck, struct{}{}, &out)
}

// Redeem calls redeem. The ticket is in the body; nothing logs the body.
func (c *Client) Redeem(ctx context.Context, req RedeemRequest) (RedeemResponse, error) {
	var out RedeemResponse
	return out, c.do(ctx, nil, pathRedeem, req, &out)
}

func (c *Client) do(ctx context.Context, s *Signer, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", path, err)
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if s != nil {
		jws, err := s.Sign(http.MethodPost, u.Path, body, c.clusterID, c.now())
		if err != nil {
			return fmt.Errorf("sign %s request: %w", path, err)
		}
		req.Header.Set(SignatureHeader, jws)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return fmt.Errorf("read %s answer: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{Path: path, Status: resp.StatusCode, Detail: problemDetail(raw)}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s answer: %w", path, err)
	}
	return nil
}

// problemDetail takes the title/detail of a problem response, one line,
// bounded. It never echoes a request body.
func problemDetail(raw []byte) string {
	var p struct {
		Title   string `json:"title"`
		Detail  string `json:"detail"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return ""
	}
	d := p.Detail
	if d == "" {
		d = p.Message
	}
	if d == "" {
		d = p.Title
	}
	d = strings.ReplaceAll(d, "\n", " ")
	if r := []rune(d); len(r) > 200 {
		d = string(r[:200]) + "…"
	}
	return d
}
