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
	"regexp"
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

// CheckRequest is the body of renew/check and renew/ack: the local k3s
// version (design 4.4 step 4). The api refuses any other field, so the last
// failure of this renewer stays on the host (file last-error).
type CheckRequest struct {
	K3sVersion string `json:"k3sVersion,omitempty"`
}

// CheckResponse is the answer of renew/check.
type CheckResponse struct {
	Due    bool   `json:"due"`
	Reason string `json:"reason"`
	Paused bool   `json:"paused"`
}

// RenewRequest is the body of renew.
type RenewRequest struct {
	CSR        string `json:"csr"`
	K3sVersion string `json:"k3sVersion,omitempty"`
}

// RenewResponse is the answer of renew: a PENDING certificate.
type RenewResponse struct {
	Certificate  string    `json:"certificate"`
	CAGeneration int64     `json:"caGeneration"`
	Serial       string    `json:"serial"`
	NotAfter     time.Time `json:"notAfter"`
}

// AckResponse is the answer of renew/ack.
type AckResponse struct {
	Serial string `json:"serial"`
}

// RedeemRequest is the body of redeem. CSR is empty for role agent: the
// api refuses a CSR from an agent machine (contract 10.5).
type RedeemRequest struct {
	Ticket       string `json:"ticket"`
	CSR          string `json:"csr,omitempty"`
	MachineLabel string `json:"machineLabel"`
	Role         string `json:"role"`
}

// RedeemResponse is the answer of redeem. The api answers null for the
// certificate fields, ServingCA and Endpoint of role agent; a null decodes
// to the zero value here. Serial can also be null for role server.
// AgentToken is not in the api contract yet: the install needs it to
// register the in-cluster agent, and waits for the Secret without it.
type RedeemResponse struct {
	MachineID     string        `json:"machineId"`
	Kind          string        `json:"kind"`
	Certificate   string        `json:"certificate"`
	Serial        string        `json:"serial"`
	NotAfter      time.Time     `json:"notAfter"`
	CAGeneration  int64         `json:"caGeneration"`
	ServingCA     string        `json:"servingCA"`
	Endpoint      string        `json:"endpoint"`
	K3sToken      string        `json:"k3sToken"`
	InstallConfig InstallConfig `json:"installConfig"`
	AgentToken    string        `json:"agentToken,omitempty"`
}

// InstallConfig is the stored install configuration (design 4.2 step 3).
// The api sends k3sVersion, ciliumVersion, clusterCidr, serviceCidr,
// serverFlags and serverUrl; the install reads the fields below. ServerURL
// is the k3s server that a role agent machine joins (null for role server).
// ClusterID, TLSSANs, AuthenticationConfig and RoleBindings are not in the
// api contract yet; the install uses them when they come. The cluster id of
// a server comes from its client certificate.
type InstallConfig struct {
	ClusterID            string        `json:"clusterId,omitempty"`
	K3sVersion           string        `json:"k3sVersion,omitempty"`
	ClusterCIDR          string        `json:"clusterCidr,omitempty"`
	ServiceCIDR          string        `json:"serviceCidr,omitempty"`
	ServerURL            string        `json:"serverUrl,omitempty"`
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

// StatusError is a non-2xx answer of the api. Code is the api's stable
// error code (for example STATE_TICKET_USED), when the answer has one.
type StatusError struct {
	Path   string
	Status int
	Code   string
	Detail string
}

func (e *StatusError) Error() string {
	s := fmt.Sprintf("%s answered HTTP %d", e.Path, e.Status)
	if e.Code != "" {
		s += " " + e.Code
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
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
// host; no path, so that the request path is the signed path). httpClient
// may be nil.
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
	// The api checks the signed path against the request path (contract
	// 10.3). A path of the api URL would be in the request path only.
	if u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("api URL %q must be https://<host>[:<port>], with no path, query or user", raw)
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

// Ack calls renew/ack, signed with the NEW key. It acknowledges a renewed
// certificate and the first certificate of an install.
func (c *Client) Ack(ctx context.Context, s Signer, req CheckRequest) (AckResponse, error) {
	var out AckResponse
	return out, c.do(ctx, &s, pathAck, req, &out)
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
	u.Path = path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build %s request: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if s != nil {
		// The signed path is the route path: with /api, without a query
		// (contract 10.3). It is the whole request path (checkAPIURL).
		jws, err := s.Sign(http.MethodPost, path, body, c.clusterID, c.now())
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
		code, detail := problemDetail(raw)
		return &StatusError{Path: path, Status: resp.StatusCode, Code: code, Detail: detail}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s answer: %w", path, err)
	}
	return nil
}

// errorCode returns the api's error code of err, or "".
func errorCode(err error) string {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code
	}
	return ""
}

// problemDetail takes the code and the message of an error answer, one
// line, bounded. The api sends {statusCode, data: {code, message, help}};
// a problem response {title, detail} also works. It never echoes a
// request body.
func problemDetail(raw []byte) (string, string) {
	var p struct {
		Title   string `json:"title"`
		Detail  string `json:"detail"`
		Message string `json:"message"`
		Code    string `json:"code"`
		Data    struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return "", ""
	}
	clean := func(s string, n int) string {
		s = strings.Map(func(r rune) rune {
			if r < 0x20 || r == 0x7f {
				return ' '
			}
			return r
		}, s)
		if r := []rune(s); len(r) > n {
			s = string(r[:n]) + "…"
		}
		return s
	}
	code := p.Data.Code
	if code == "" {
		code = p.Code
	}
	if !errorCodeRE.MatchString(code) {
		code = ""
	}
	d := p.Data.Message
	for _, alt := range []string{p.Detail, p.Message, p.Title} {
		if d == "" {
			d = alt
		}
	}
	return code, clean(d, 200)
}

var errorCodeRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)
