package staterenew

import (
	"bufio"
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Supported redeem roles. v1 installs servers only.
const RoleServer = "server"

// Limits of the redeem answer.
const (
	maxTicket      = 512
	maxAuthnConfig = 64 << 10
	maxSANs        = 20
	maxBindings    = 20
	maxGroups      = 20
)

var (
	ticketRE    = regexp.MustCompile(`^[\x21-\x7e]{16,512}$`)
	machineIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	k3sTokenRE  = regexp.MustCompile(`^[\x21-\x7e]{16,512}$`)
	labelRE     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// SSORoles are the cluster roles a static SSO binding may name.
var SSORoles = map[string]bool{"cluster-admin": true, "admin": true, "edit": true, "view": true}

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

// Redeemer is the first step of the host install: it makes the first key on
// this machine, redeems the ticket and writes the state folder.
type Redeemer struct {
	Store Store
	API   interface {
		Redeem(context.Context, RedeemRequest) (RedeemResponse, error)
	}
	APIURL string
	Label  string
	Role   string
	Now    func() time.Time
}

// Result is what the install script needs to know after a redemption. It
// holds no secret.
type Result struct {
	MachineID string
	ClusterID string
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
	if d.Role != RoleServer {
		return Result{}, fmt.Errorf("role %q is not supported; this version installs servers only (role %q)", d.Role, RoleServer)
	}
	if !labelRE.MatchString(d.Label) {
		return Result{}, fmt.Errorf("machine label %q is not a DNS label; use lowercase letters, digits and '-', at most 63 characters", d.Label)
	}
	if err := os.MkdirAll(d.Store.Dir, 0o700); err != nil {
		return Result{}, fmt.Errorf("make %s: %w", d.Store.Dir, err)
	}
	if _, err := d.Store.Current(); err == nil {
		return Result{}, fmt.Errorf("%w (%s exists)", ErrInstalled, filepath.Join(d.Store.Dir, currentLink))
	} else if !errors.Is(err, ErrNoCurrent) {
		return Result{}, err
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

	resp, err := d.API.Redeem(ctx, RedeemRequest{Ticket: ticket, CSR: string(csr), MachineLabel: d.Label, Role: d.Role})
	if err != nil {
		var se *StatusError
		if errors.As(err, &se) && se.Permanent() {
			return Result{}, fmt.Errorf("the platform refused the ticket: %w; make a new ticket in the dashboard and run the install again", err)
		}
		return Result{}, fmt.Errorf("redeem the ticket: %w", err)
	}
	cert, err := d.checkResponse(resp, key)
	if err != nil {
		return Result{}, fmt.Errorf("the platform's answer is not usable: %w; the ticket is now used, so make a new ticket in the dashboard", err)
	}
	sso, err := renderSSOBindings(resp.InstallConfig.ClusterID, resp.InstallConfig.RoleBindings)
	if err != nil {
		return Result{}, err
	}

	type file struct {
		path string
		data []byte
		perm os.FileMode
	}
	top := func(name string) string { return filepath.Join(d.Store.Dir, name) }
	settings, err := json.MarshalIndent(Settings{APIURL: d.APIURL, ClusterID: resp.InstallConfig.ClusterID, MachineID: resp.MachineID}, "", "  ")
	if err != nil {
		return Result{}, err
	}
	files := []file{
		{d.Store.Path(n, CertFile), []byte(resp.Certificate), 0o644},
		{d.Store.Path(n, CAFile), []byte(resp.ServingCA), 0o644},
		{top(TokenFile), []byte(resp.K3sToken + "\n"), 0o600},
		{top(ConfigFile), append(settings, '\n'), 0o600},
		{top(InstallEnvFile), installEnv(resp, len(sso) > 0), 0o600},
	}
	if resp.AgentToken != "" {
		files = append(files, file{top(AgentTokenFile), []byte(resp.AgentToken + "\n"), 0o600})
	}
	if resp.InstallConfig.AuthenticationConfig != "" {
		files = append(files, file{top(AuthnConfigFile), []byte(resp.InstallConfig.AuthenticationConfig), 0o600})
	}
	if len(sso) > 0 {
		files = append(files, file{top(SSOManifestFile), sso, 0o600})
	}
	for _, f := range files {
		if err := WriteFileAtomic(f.path, f.data, f.perm); err != nil {
			return Result{}, err
		}
	}
	if err := d.Store.WriteMeta(n, Meta{State: StateAcked, Serial: serialHex(cert), CreatedAt: d.Now().UTC()}); err != nil {
		return Result{}, err
	}
	// The switch is the last step: "current" exists only for a complete
	// state folder.
	if err := d.Store.Switch(n); err != nil {
		return Result{}, err
	}
	ok = true
	return Result{MachineID: resp.MachineID, ClusterID: resp.InstallConfig.ClusterID}, nil
}

func (d *Redeemer) checkResponse(resp RedeemResponse, key crypto.Signer) (*x509.Certificate, error) {
	if !machineIDRE.MatchString(resp.MachineID) {
		return nil, fmt.Errorf("machine id %q is not valid", resp.MachineID)
	}
	c, err := parseCert([]byte(resp.Certificate))
	if err != nil {
		return nil, fmt.Errorf("client certificate: %w", err)
	}
	if err := checkClientCert(c, key, d.Now()); err != nil {
		return nil, err
	}
	if err := checkCABundle([]byte(resp.ServingCA)); err != nil {
		return nil, err
	}
	if err := checkEndpoint(resp.Endpoint); err != nil {
		return nil, err
	}
	if !k3sTokenRE.MatchString(resp.K3sToken) {
		// SECURITY: never echo the token.
		return nil, errors.New("the k3s token has an invalid format")
	}
	if resp.AgentToken != "" {
		if _, err := config.ValidateToken(resp.AgentToken); err != nil {
			return nil, err
		}
	}
	if err := checkInstallConfig(resp.InstallConfig); err != nil {
		return nil, err
	}
	return c, nil
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
	if !config.ValidClusterID(c.ClusterID) {
		return fmt.Errorf("install configuration: cluster id %q is not valid", c.ClusterID)
	}
	if c.K3sVersion != "" && !desired.ValidK3sVersion(c.K3sVersion) {
		return fmt.Errorf("install configuration: k3s version %q is not an exact k3s version", c.K3sVersion)
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

// renderSSOBindings renders the static SSO role bindings of design 3.4: one
// ClusterRoleBinding per entry, for groups only.
func renderSSOBindings(clusterID string, bindings []RoleBinding) ([]byte, error) {
	if len(bindings) > maxBindings {
		return nil, fmt.Errorf("install configuration: %d role bindings, at most %d", len(bindings), maxBindings)
	}
	var docs []string
	seen := map[string]bool{}
	for _, b := range bindings {
		name := "kubehz-sso-" + b.Name
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 || b.Name == "" {
			return nil, fmt.Errorf("install configuration: role binding name %q is not valid", b.Name)
		}
		if seen[name] {
			return nil, fmt.Errorf("install configuration: role binding %q is there two times", b.Name)
		}
		seen[name] = true
		if !SSORoles[b.ClusterRole] {
			return nil, fmt.Errorf("install configuration: role binding %q names cluster role %q (allowed: %s)", b.Name, b.ClusterRole, strings.Join(ssoRoleNames(), ", "))
		}
		if len(b.Groups) == 0 || len(b.Groups) > maxGroups {
			return nil, fmt.Errorf("install configuration: role binding %q has %d groups (want 1 to %d)", b.Name, len(b.Groups), maxGroups)
		}
		crb := rbacv1.ClusterRoleBinding{
			TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "kubehz-install",
				},
				Annotations: map[string]string{"kubehz.cloud/cluster-id": clusterID},
			},
			RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: b.ClusterRole},
		}
		for _, g := range b.Groups {
			if g == "" || len(g) > 256 || strings.ContainsFunc(g, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
				return nil, fmt.Errorf("install configuration: role binding %q has a group name that is not valid", b.Name)
			}
			crb.Subjects = append(crb.Subjects, rbacv1.Subject{Kind: rbacv1.GroupKind, APIGroup: "rbac.authorization.k8s.io", Name: g})
		}
		out, err := yaml.Marshal(crb)
		if err != nil {
			return nil, err
		}
		docs = append(docs, string(out))
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return []byte("# Written by the kubehz install. The SSO role bindings of this cluster.\n---\n" + strings.Join(docs, "---\n")), nil
}

func ssoRoleNames() []string {
	out := make([]string, 0, len(SSORoles))
	for r := range SSORoles {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// installEnv is the KEY=value file that the install script reads with a
// parser (never with "source"). Every value was validated and has no space,
// quote or newline.
func installEnv(resp RedeemResponse, sso bool) []byte {
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	ic := resp.InstallConfig
	lines := []string{
		"CLUSTER_ID=" + ic.ClusterID,
		"MACHINE_ID=" + resp.MachineID,
		"STATE_ENDPOINT=" + resp.Endpoint,
		"K3S_VERSION=" + ic.K3sVersion,
		"CLUSTER_CIDR=" + ic.ClusterCIDR,
		"SERVICE_CIDR=" + ic.ServiceCIDR,
		"TLS_SANS=" + strings.Join(ic.TLSSANs, ","),
		"HAS_AUTHN_CONFIG=" + b(ic.AuthenticationConfig != ""),
		"HAS_SSO_BINDINGS=" + b(sso),
		"HAS_AGENT_TOKEN=" + b(resp.AgentToken != ""),
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}
