package staterenew

import (
	"bufio"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"

	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/desired"
)

// Settings is renew.json: what the renewer needs to call the api.
type Settings struct {
	APIURL    string `json:"apiUrl"`
	ClusterID string `json:"clusterId"`
	MachineID string `json:"machineId"`
}

// ReadSettings reads renew.json from dir.
func ReadSettings(dir string) (Settings, error) {
	var s Settings
	b, err := os.ReadFile(filepath.Join(dir, ConfigFile))
	if err != nil {
		return s, fmt.Errorf("read %s: %w", filepath.Join(dir, ConfigFile), err)
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("decode %s: %w", filepath.Join(dir, ConfigFile), err)
	}
	if _, err := checkAPIURL(s.APIURL); err != nil {
		return s, err
	}
	if !config.ValidClusterID(s.ClusterID) {
		return s, fmt.Errorf("%s has the cluster id %q, which is not valid", filepath.Join(dir, ConfigFile), s.ClusterID)
	}
	return s, nil
}

// Machine roles of a redemption (contract 8.8). The ticket fixes the role;
// the api refuses a redemption with another role.
const (
	// RoleServer is a k3s server: it gets a datastore client certificate.
	RoleServer = "server"
	// RoleAgent is a k3s agent node: it joins a k3s server and gets NO
	// datastore identity (contract 10.5).
	RoleAgent = "agent"
)

// Limits of the redeem answer.
const (
	maxTicket      = 512
	maxAuthnConfig = 64 << 10
	maxSANs        = 20
)

var (
	ticketRE    = regexp.MustCompile(`^[\x21-\x7e]{16,512}$`)
	machineIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	k3sTokenRE  = regexp.MustCompile(`^[\x21-\x7e]{16,512}$`)
	labelRE     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	// serverURLRE is the api's rule for the k3s server URL of an agent
	// ticket: https, a host name or a bracketed IPv6 address, an optional
	// port, no path.
	serverURLRE = regexp.MustCompile(`^https://(?:\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?)(?::[0-9]{1,5})?$`)
	// ciliumVersionRE is an exact Cilium release (the install script pins
	// the chart of each version by sha256).
	ciliumVersionRE = regexp.MustCompile(`^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$`)
)

// ReadTicket reads the ticket from the first line of r. The install script
// asks for it at a hidden prompt and writes it to stdin, so the ticket is in
// no argument list and no environment.
func ReadTicket(r io.Reader) (string, error) {
	line, err := bufio.NewReader(io.LimitReader(r, maxTicket+2)).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read the ticket from stdin: %w", err)
	}
	t := strings.TrimSpace(line)
	if t == "" {
		return "", errors.New("no ticket on stdin; copy the ticket from the dashboard and try again")
	}
	if !ticketRE.MatchString(t) {
		// SECURITY: never echo the ticket.
		return "", errors.New("the ticket has an invalid format; copy the ticket from the dashboard again")
	}
	return t, nil
}

// Redeemer is the first step of the host install. A server makes its first
// key on this machine, redeems the ticket with a CSR and writes the state
// folder. A server install (not a recovery) also makes the kubehz agent
// token of the cluster and sends only its hash. An agent node redeems the
// ticket without a CSR and gets only the k3s agent token and its server URL.
type Redeemer struct {
	Store Store
	API   interface {
		Redeem(context.Context, RedeemRequest) (RedeemResponse, error)
	}
	APIURL string
	Label  string
	Role   string
	// Recovery is set for a recovery ticket (the script's --recovery). A
	// recovery sends no agent token hash: the cluster has its agent token.
	Recovery bool
	Now      func() time.Time
}

// Result is what the install script needs to know after a redemption. It
// holds no secret. ClusterID is empty for role agent: the api tells an
// agent node no cluster id. AgentToken is true when this install made the
// cluster's kubehz agent token (the file agent-token).
type Result struct {
	Role       string
	MachineID  string
	ClusterID  string
	AgentToken bool
}

// ErrInstalled means the machine already has a state folder in use.
var ErrInstalled = errors.New("managed state is already set up on this machine")

// Run redeems ticket.
func (d *Redeemer) Run(ctx context.Context, ticket string) (Result, error) {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Role == "" {
		d.Role = RoleServer
	}
	if d.Role != RoleServer && d.Role != RoleAgent {
		return Result{}, fmt.Errorf("role %q is not supported (use %q or %q, the role of the ticket)", d.Role, RoleServer, RoleAgent)
	}
	if d.Role == RoleAgent && d.Recovery {
		return Result{}, errors.New("a recovery is for a server; an agent ticket is an install ticket")
	}
	if !labelRE.MatchString(d.Label) {
		return Result{}, fmt.Errorf("machine label %q is not a DNS label; use lowercase letters, digits and '-', at most 63 characters", d.Label)
	}
	if err := os.MkdirAll(d.Store.Dir, 0o700); err != nil {
		return Result{}, fmt.Errorf("make %s: %w", d.Store.Dir, err)
	}
	// A second redemption would spend a new ticket, and a second redemption
	// of the SAME ticket revokes this machine's first certificate (contract
	// 10.1). So a machine that has a state folder in use never redeems.
	if _, err := d.Store.Current(); err == nil {
		return Result{}, fmt.Errorf("%w (%s exists)", ErrInstalled, filepath.Join(d.Store.Dir, currentLink))
	} else if !errors.Is(err, ErrNoCurrent) {
		return Result{}, err
	}
	if role := installedRole(d.Store.Dir); role != "" && role != d.Role {
		return Result{}, fmt.Errorf("%w as role %s; install it again only after the uninstall", ErrInstalled, role)
	}
	if d.Role == RoleAgent {
		return d.runAgent(ctx, ticket)
	}
	// Folders from an install that stopped before the switch are not in use.
	if gens, err := d.Store.Gens(); err == nil {
		for _, g := range gens {
			if err := os.RemoveAll(d.Store.genDir(g)); err != nil {
				return Result{}, fmt.Errorf("remove the incomplete %s: %w", d.Store.genDir(g), err)
			}
		}
	}

	n, err := d.Store.NewGen()
	if err != nil {
		return Result{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(d.Store.genDir(n))
		}
	}()
	key, keyPEM, err := newKey()
	if err != nil {
		return Result{}, err
	}
	if err := WriteFileAtomic(d.Store.Path(n, KeyFile), keyPEM, 0o600); err != nil {
		return Result{}, err
	}
	csr, err := newCSR(key, d.Label)
	if err != nil {
		return Result{}, err
	}
	if err := WriteFileAtomic(d.Store.Path(n, CSRFile), csr, 0o600); err != nil {
		return Result{}, err
	}

	resp, agentToken, err := d.redeemServer(ctx, RedeemRequest{Ticket: ticket, CSR: string(csr), MachineLabel: d.Label, Role: RoleServer})
	if err != nil {
		return Result{}, redeemError(err)
	}
	cert, clusterID, err := d.checkResponse(resp, key)
	if err != nil {
		return Result{}, fmt.Errorf("the platform's answer is not usable: %w; the ticket is now used, so make a new ticket in the dashboard", err)
	}

	top := func(name string) string { return filepath.Join(d.Store.Dir, name) }
	settings, err := json.MarshalIndent(Settings{APIURL: d.APIURL, ClusterID: clusterID, MachineID: resp.MachineID}, "", "  ")
	if err != nil {
		return Result{}, err
	}
	files := []stateFile{
		{d.Store.Path(n, CertFile), []byte(resp.Certificate), 0o644},
		{d.Store.Path(n, CAFile), []byte(resp.ServingCA), 0o644},
		{top(TokenFile), []byte(resp.K3sToken + "\n"), 0o600},
		{top(K3sAgentTokenFile), []byte(resp.K3sAgentToken + "\n"), 0o600},
		{top(ConfigFile), append(settings, '\n'), 0o600},
	}
	if agentToken != "" {
		// No newline: the install puts the file as it is into the Secret.
		files = append(files, stateFile{top(AgentTokenFile), []byte(agentToken), 0o600})
	}
	if resp.InstallConfig.AuthenticationConfig != "" {
		files = append(files, stateFile{top(AuthnConfigFile), []byte(resp.InstallConfig.AuthenticationConfig), 0o600})
	}
	// install.env is the last file: the install script reads it.
	files = append(files, stateFile{top(InstallEnvFile), installEnv(resp, clusterID, agentToken != ""), 0o600})
	if err := writeFiles(files); err != nil {
		return Result{}, err
	}
	// The first certificate is PENDING at the api until this machine
	// acknowledges it (contract 10.1). The install script starts k3s with
	// it and then runs "state-ack"; the renewal timer also finishes it.
	// There is no previous generation to switch back to.
	meta := Meta{
		State:        StateSwitched,
		CAGeneration: resp.CAGeneration,
		Serial:       serialHex(cert),
		NotAfter:     cert.NotAfter.UTC(),
		CreatedAt:    d.Now().UTC(),
	}
	if err := d.Store.WriteMeta(n, meta); err != nil {
		return Result{}, err
	}
	// The switch is the last step: "current" exists only for a complete
	// state folder.
	if err := d.Store.Switch(n); err != nil {
		return Result{}, err
	}
	ok = true
	return Result{Role: RoleServer, MachineID: resp.MachineID, ClusterID: clusterID, AgentToken: agentToken != ""}, nil
}

// redeemServer redeems a server ticket. A server install (not a recovery)
// makes the kubehz agent token of the cluster on this machine and sends
// only its sha256. The api refuses the hash, and leaves the ticket unused,
// when the cluster has an agent token already (another server made it:
// redeem again without the hash) or when another cluster has this token
// (make a new one). It returns the token when the api took its hash.
func (d *Redeemer) redeemServer(ctx context.Context, req RedeemRequest) (RedeemResponse, string, error) {
	token := ""
	if !d.Recovery {
		t, hash, err := newAgentToken()
		if err != nil {
			return RedeemResponse{}, "", err
		}
		token, req.AgentTokenHash = t, hash
	}
	resp, err := d.API.Redeem(ctx, req)
	for tries := 0; err != nil && req.AgentTokenHash != "" && tries < 2; tries++ {
		switch errorCode(err) {
		case "STATE_AGENT_TOKEN_EXISTS":
			token, req.AgentTokenHash = "", ""
		case "AGENT_TOKEN_CONFLICT":
			t, hash, terr := newAgentToken()
			if terr != nil {
				return RedeemResponse{}, "", terr
			}
			token, req.AgentTokenHash = t, hash
		default:
			return RedeemResponse{}, "", err
		}
		resp, err = d.API.Redeem(ctx, req)
	}
	if err != nil {
		return RedeemResponse{}, "", err
	}
	return resp, token, nil
}

// newAgentToken makes a kubehz agent token (khz_agt_ and 64 hex digits from
// the system's random source) and its sha256 in lowercase hex.
func newAgentToken() (token, hash string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("make the agent token: %w", err)
	}
	token = "khz_agt_" + hex.EncodeToString(b)
	sum := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(sum[:]), nil
}

// runAgent redeems an agent ticket: no key, no CSR, no certificate. The
// folder gets the k3s token and install.env, and install.env is the last
// file written.
func (d *Redeemer) runAgent(ctx context.Context, ticket string) (Result, error) {
	envPath := filepath.Join(d.Store.Dir, InstallEnvFile)
	if _, err := os.Lstat(envPath); err == nil {
		return Result{}, fmt.Errorf("%w (%s exists)", ErrInstalled, envPath)
	}
	resp, err := d.API.Redeem(ctx, RedeemRequest{Ticket: ticket, MachineLabel: d.Label, Role: RoleAgent})
	if err != nil {
		return Result{}, redeemError(err)
	}
	if err := checkAgentResponse(resp); err != nil {
		return Result{}, fmt.Errorf("the platform's answer is not usable: %w; the ticket is now used, so make a new ticket in the dashboard", err)
	}
	files := []stateFile{
		{filepath.Join(d.Store.Dir, TokenFile), []byte(resp.K3sToken + "\n"), 0o600},
		{envPath, agentInstallEnv(resp), 0o600},
	}
	if err := writeFiles(files); err != nil {
		return Result{}, err
	}
	return Result{Role: RoleAgent, MachineID: resp.MachineID}, nil
}

// installedRole is the ROLE of an install.env in dir, or "" without one.
// An install.env without ROLE is of a server.
func installedRole(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, InstallEnvFile))
	if err != nil {
		return ""
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(strings.TrimRight(line, "\n"), "ROLE="); ok {
			return v
		}
	}
	return RoleServer
}

type stateFile struct {
	path string
	data []byte
	perm os.FileMode
}

func writeFiles(files []stateFile) error {
	for _, f := range files {
		if err := WriteFileAtomic(f.path, f.data, f.perm); err != nil {
			return err
		}
	}
	return nil
}

// redeemError explains a failed redemption. The api leaves the ticket
// unused on every failure that is not the ticket's.
func redeemError(err error) error {
	switch {
	case errorCode(err) == "STATE_TICKET_USED":
		return fmt.Errorf("the ticket was redeemed before: %w; if that was this machine, its first certificate is revoked now. Make a new ticket in the dashboard and run the install again", err)
	case errorCode(err) == "BAD_REQUEST":
		// The api checks the request before it spends the ticket.
		return fmt.Errorf("the platform refused the redemption: %w; the ticket is not used yet, so run the install again with the options of the command in the dashboard (--role, --recovery)", err)
	case isPermanent(err):
		return fmt.Errorf("the platform refused the ticket: %w; make a new ticket in the dashboard (of the role of this install) and run the install again", err)
	}
	return fmt.Errorf("redeem the ticket: %w; the ticket is not used yet, so run the install again", err)
}

// checkResponse checks the answer of a server redemption and returns the
// client certificate and the cluster id. The api names the cluster in the
// certificate subject (O) and the machine in its common name (CN); it
// checks both on each signed request.
func (d *Redeemer) checkResponse(resp RedeemResponse, key crypto.Signer) (*x509.Certificate, string, error) {
	if !machineIDRE.MatchString(resp.MachineID) {
		return nil, "", fmt.Errorf("machine id %q is not valid", resp.MachineID)
	}
	if err := d.checkKind(resp.Kind); err != nil {
		return nil, "", err
	}
	c, err := parseCert([]byte(resp.Certificate))
	if err != nil {
		return nil, "", fmt.Errorf("client certificate: %w", err)
	}
	if err := checkClientCert(c, key, d.Now()); err != nil {
		return nil, "", err
	}
	if err := checkIssued(c, resp.Serial, resp.NotAfter, resp.CAGeneration); err != nil {
		return nil, "", err
	}
	if c.Subject.CommonName != resp.MachineID {
		return nil, "", fmt.Errorf("the client certificate names machine %q, the answer %q", c.Subject.CommonName, resp.MachineID)
	}
	clusterID := ""
	if len(c.Subject.Organization) == 1 {
		clusterID = c.Subject.Organization[0]
	}
	if !config.ValidClusterID(clusterID) {
		return nil, "", fmt.Errorf("the client certificate names no valid cluster id (subject O %q)", c.Subject.Organization)
	}
	if id := resp.InstallConfig.ClusterID; id != clusterID {
		return nil, "", fmt.Errorf("install configuration: cluster id %q is not the cluster %q of the client certificate", id, clusterID)
	}
	if err := checkCABundle([]byte(resp.ServingCA)); err != nil {
		return nil, "", err
	}
	if err := checkEndpoint(resp.Endpoint); err != nil {
		return nil, "", err
	}
	if !k3sTokenRE.MatchString(resp.K3sToken) {
		// SECURITY: never echo the token.
		return nil, "", errors.New("the k3s token has an invalid format")
	}
	if !k3sTokenRE.MatchString(resp.K3sAgentToken) {
		// SECURITY: never echo the token.
		return nil, "", errors.New("the k3s agent token is missing or has an invalid format")
	}
	if resp.K3sAgentToken == resp.K3sToken {
		return nil, "", errors.New("the k3s agent token is the k3s server token; a server needs two different tokens")
	}
	if resp.InstallConfig.ServerURL != "" {
		return nil, "", errors.New("install configuration: a server ticket has a k3s server URL (that is for role agent)")
	}
	if err := checkInstallConfig(resp.InstallConfig); err != nil {
		return nil, "", err
	}
	return c, clusterID, nil
}

// checkIssued checks the api's data of an issued certificate against the
// certificate: the serial (lowercase hex, can be null), notAfter (the api
// has milliseconds, the certificate seconds) and the client CA generation.
func checkIssued(c *x509.Certificate, serial string, notAfter time.Time, caGeneration int64) error {
	if serial != "" && serial != serialHex(c) {
		return fmt.Errorf("the answer names serial %s, the certificate has %s", serial, serialHex(c))
	}
	if !notAfter.IsZero() && notAfter.Sub(c.NotAfter).Abs() > time.Second {
		return fmt.Errorf("the answer names notAfter %s, the certificate has %s", notAfter.UTC().Format(time.RFC3339), c.NotAfter.UTC().Format(time.RFC3339))
	}
	if caGeneration < 1 {
		return fmt.Errorf("the answer names client CA generation %d (want 1 or more)", caGeneration)
	}
	return nil
}

// checkKind checks the ticket kind of a server redemption against the
// script's --recovery. A recovery removes the Node object of the lost
// server; an install does not.
func (d *Redeemer) checkKind(kind string) error {
	switch {
	case d.Recovery && kind != KindRecovery:
		return fmt.Errorf("the ticket kind is %q, but the install runs with --recovery; use a recovery ticket for a recovery", kind)
	case !d.Recovery && kind != KindInstall:
		return fmt.Errorf("the ticket kind is %q; run the install script with --recovery for a recovery ticket", kind)
	}
	return nil
}

// checkAgentResponse checks the answer of an agent redemption. An agent has
// no datastore identity: a certificate, serving CA or endpoint in the
// answer is a fault, and nothing of it is written.
func checkAgentResponse(resp RedeemResponse) error {
	if !machineIDRE.MatchString(resp.MachineID) {
		return fmt.Errorf("machine id %q is not valid", resp.MachineID)
	}
	if resp.Kind != KindInstall {
		return fmt.Errorf("the ticket kind is %q; an agent ticket is an install ticket", resp.Kind)
	}
	if resp.Certificate != "" || resp.Serial != "" || !resp.NotAfter.IsZero() || resp.CAGeneration != 0 ||
		resp.ServingCA != "" || resp.Endpoint != "" {
		return errors.New("the answer has datastore credentials, but an agent machine gets none")
	}
	// An agent node joins with the k3s agent token only (k3sToken); the
	// server token is not for it.
	if resp.K3sAgentToken != "" {
		return errors.New("the answer has a second k3s token, but an agent machine gets only the agent token")
	}
	if !k3sTokenRE.MatchString(resp.K3sToken) {
		// SECURITY: never echo the token.
		return errors.New("the k3s token has an invalid format")
	}
	ic := resp.InstallConfig
	if ic.ClusterID != "" || len(ic.TLSSANs) > 0 || ic.AuthenticationConfig != "" {
		return errors.New("install configuration: a cluster id, TLS SANs or an authentication configuration are for a server")
	}
	if !serverURLRE.MatchString(ic.ServerURL) {
		return fmt.Errorf("install configuration: k3s server URL %q is not https://<host>[:<port>]", ic.ServerURL)
	}
	u, err := url.Parse(ic.ServerURL)
	if err != nil {
		return fmt.Errorf("install configuration: k3s server URL %q: %w", ic.ServerURL, err)
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("install configuration: k3s server URL %q has port %s (want 1 to 65535)", ic.ServerURL, p)
		}
	}
	if h := u.Hostname(); strings.Contains(u.Host, "[") {
		if a, err := netip.ParseAddr(h); err != nil || !a.Is6() {
			return fmt.Errorf("install configuration: k3s server URL %q has no valid IPv6 address", ic.ServerURL)
		}
	}
	if ic.K3sVersion != "" && !desired.ValidK3sVersion(ic.K3sVersion) {
		return fmt.Errorf("install configuration: k3s version %q is not an exact k3s version", ic.K3sVersion)
	}
	return nil
}

func checkEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.Port() == "" ||
		u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("datastore endpoint %q is not https://<host>:<port>", raw)
	}
	if errs := validation.IsDNS1123Subdomain(u.Hostname()); len(errs) > 0 {
		return fmt.Errorf("datastore endpoint host %q is not a DNS name", u.Hostname())
	}
	return nil
}

func checkInstallConfig(c InstallConfig) error {
	if c.K3sVersion != "" && !desired.ValidK3sVersion(c.K3sVersion) {
		return fmt.Errorf("install configuration: k3s version %q is not an exact k3s version", c.K3sVersion)
	}
	if c.CiliumVersion != "" && !ciliumVersionRE.MatchString(c.CiliumVersion) {
		return fmt.Errorf("install configuration: Cilium version %q is not an exact version (for example 1.20.4)", c.CiliumVersion)
	}
	for name, v := range map[string]string{"cluster CIDR": c.ClusterCIDR, "service CIDR": c.ServiceCIDR} {
		if v == "" {
			continue
		}
		if err := checkCIDRs(v); err != nil {
			return fmt.Errorf("install configuration: %s %q: %w", name, v, err)
		}
	}
	if len(c.TLSSANs) > maxSANs {
		return fmt.Errorf("install configuration: %d TLS SANs, at most %d", len(c.TLSSANs), maxSANs)
	}
	for _, s := range c.TLSSANs {
		if _, err := netip.ParseAddr(s); err == nil {
			continue
		}
		if errs := validation.IsDNS1123Subdomain(s); len(errs) > 0 {
			return fmt.Errorf("install configuration: TLS SAN %q is not a DNS name or an IP address", s)
		}
	}
	if a := c.AuthenticationConfig; a != "" {
		if len(a) > maxAuthnConfig {
			return fmt.Errorf("install configuration: the authentication configuration has %d bytes, at most %d", len(a), maxAuthnConfig)
		}
		var head struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(a), &head); err != nil {
			return fmt.Errorf("install configuration: the authentication configuration is not YAML: %w", err)
		}
		if !strings.HasPrefix(head.APIVersion, "apiserver.config.k8s.io/") || head.Kind != "AuthenticationConfiguration" {
			return fmt.Errorf("install configuration: the authentication configuration has kind %q of %q (want AuthenticationConfiguration of apiserver.config.k8s.io)", head.Kind, head.APIVersion)
		}
	}
	return nil
}

// checkCIDRs accepts one IPv4 CIDR in canonical form. v1 installs
// single-stack IPv4 (the Cilium pool of the install takes IPv4 only).
func checkCIDRs(v string) error {
	pfx, err := netip.ParsePrefix(v)
	if err != nil {
		return err
	}
	if !pfx.Addr().Is4() {
		return errors.New("not an IPv4 CIDR (this version installs IPv4 only)")
	}
	if pfx.Masked() != pfx {
		return fmt.Errorf("host bits are set (want %s)", pfx.Masked())
	}
	if pfx.Bits() > 24 {
		return fmt.Errorf("prefix /%d is too small (want /24 or larger)", pfx.Bits())
	}
	return nil
}

// installEnv is the KEY=value file that the install script reads with a
// parser (never with "source"). Every value was validated and has no space,
// quote or newline.
func installEnv(resp RedeemResponse, clusterID string, agentToken bool) []byte {
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	ic := resp.InstallConfig
	lines := []string{
		"ROLE=" + RoleServer,
		"KIND=" + resp.Kind,
		"CLUSTER_ID=" + clusterID,
		"MACHINE_ID=" + resp.MachineID,
		"STATE_ENDPOINT=" + resp.Endpoint,
		"K3S_VERSION=" + ic.K3sVersion,
		"CILIUM_VERSION=" + ic.CiliumVersion,
		"CLUSTER_CIDR=" + ic.ClusterCIDR,
		"SERVICE_CIDR=" + ic.ServiceCIDR,
		"TLS_SANS=" + strings.Join(ic.TLSSANs, ","),
		"HAS_AUTHN_CONFIG=" + b(ic.AuthenticationConfig != ""),
		"HAS_AGENT_TOKEN=" + b(agentToken),
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// agentInstallEnv is install.env of an agent node: the k3s server to join
// and the k3s version. It has no datastore endpoint and no cluster id.
func agentInstallEnv(resp RedeemResponse) []byte {
	lines := []string{
		"ROLE=" + RoleAgent,
		"MACHINE_ID=" + resp.MachineID,
		"SERVER_URL=" + resp.InstallConfig.ServerURL,
		"K3S_VERSION=" + resp.InstallConfig.K3sVersion,
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}
