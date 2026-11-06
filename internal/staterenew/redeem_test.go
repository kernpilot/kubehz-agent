package staterenew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/config"
)

const (
	testTicket        = "khz_tkt_0123456789abcdef0123456789abcdef"
	testK3sToken      = "K10abcdef0123456789::server:0123456789abcdef"
	testK3sAgentToken = "K10abcdef0123456789::node:fedcba9876543210"
	testAuthn         = "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthenticationConfiguration\njwt: []\n"
	testServerURL     = "https://k3s.example.com:6443"
)

// apiRedeem answers a server redemption with exactly the fields of the
// api's contract. The certificate stays pending until the machine
// acknowledges it.
func apiRedeem(a *fakeAPI) func(RedeemRequest, any) (RedeemResponse, int) {
	return func(_ RedeemRequest, pub any) (RedeemResponse, int) {
		pc := a.issuePending(pub)
		a.pending = map[string]*pendingCert{a.pendingKey(pub): pc}
		return RedeemResponse{
			MachineID:     testMachine,
			Kind:          a.kind(),
			Certificate:   pc.pem,
			Serial:        pc.serial,
			NotAfter:      pc.notAfter,
			CAGeneration:  pc.caGen,
			ServingCA:     string(a.ca().pem),
			Endpoint:      "https://abc.state.kubehz.cloud:2379",
			K3sToken:      testK3sToken,
			K3sAgentToken: testK3sAgentToken,
			InstallConfig: InstallConfig{
				ClusterID:     testCluster,
				K3sVersion:    "v1.34.2+k3s1",
				CiliumVersion: "1.20.4",
				ClusterCIDR:   "10.42.0.0/16",
				ServiceCIDR:   "10.43.0.0/16",
			},
		}, 0
	}
}

// goodRedeem is apiRedeem with the TLS SANs of the ticket and an
// authentication configuration (not in the api contract yet).
func goodRedeem(a *fakeAPI) func(RedeemRequest, any) (RedeemResponse, int) {
	base := apiRedeem(a)
	return func(req RedeemRequest, pub any) (RedeemResponse, int) {
		r, s := base(req, pub)
		r.InstallConfig.TLSSANs = []string{"k3s.example.com", "203.0.113.7"}
		r.InstallConfig.AuthenticationConfig = testAuthn
		return r, s
	}
}

// agentRedeem answers an agent redemption as the api does: no
// certificate, no serving CA, no endpoint, no cluster id; k3sToken is the
// k3s agent token, and the server URL.
func agentRedeem(_ *fakeAPI) func(RedeemRequest, any) (RedeemResponse, int) {
	return func(RedeemRequest, any) (RedeemResponse, int) {
		return RedeemResponse{
			MachineID: "m-2",
			Kind:      KindInstall,
			K3sToken:  testK3sAgentToken,
			InstallConfig: InstallConfig{
				K3sVersion:    "v1.34.2+k3s1",
				CiliumVersion: "1.20.4",
				ClusterCIDR:   "10.42.0.0/16",
				ServiceCIDR:   "10.43.0.0/16",
				ServerURL:     testServerURL,
			},
		}, 0
	}
}

func newRedeemer(t *testing.T, srv *httptest.Server) *Redeemer {
	t.Helper()
	client, err := NewClient(srv.URL, "", "test", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	return &Redeemer{Store: Store{Dir: filepath.Join(t.TempDir(), "state")}, API: client, APIURL: srv.URL, Label: "server-1"}
}

// sha256Hex is the hash that the api stores for an agent token.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestRedeemWritesTheStateFolder(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, srv)
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if res.ClusterID != testCluster || res.MachineID != testMachine || res.Role != RoleServer || !res.AgentToken {
		t.Fatalf("result %+v", res)
	}
	req := a.redeemed[0]
	if req.MachineLabel != "server-1" || req.Role != RoleServer || !strings.Contains(req.CSR, "CERTIFICATE REQUEST") {
		t.Fatalf("redeem body %+v", req)
	}
	dir := d.Store.Dir
	if n, err := d.Store.Current(); err != nil || n != 1 {
		t.Fatalf("current = %d, %v", n, err)
	}
	for name, want := range map[string]os.FileMode{
		filepath.Join(genName(1), KeyFile): 0o600,
		TokenFile:                          0o600,
		K3sAgentTokenFile:                  0o600,
		AgentTokenFile:                     0o600,
		ConfigFile:                         0o600,
		AuthnConfigFile:                    0o600,
		InstallEnvFile:                     0o600,
	} {
		if got := fileMode(t, filepath.Join(dir, name)); got != want {
			t.Errorf("%s mode %o, want %o", name, got, want)
		}
	}
	if fileMode(t, dir) != 0o700 {
		t.Errorf("state folder mode %o", fileMode(t, dir))
	}
	// Two k3s tokens: the server joins with the server token, and its
	// agents with the agent token (k3s agent-token-file).
	if strings.TrimSpace(readFile(t, dir, TokenFile)) != testK3sToken {
		t.Error("k3s server token not written")
	}
	if strings.TrimSpace(readFile(t, dir, K3sAgentTokenFile)) != testK3sAgentToken {
		t.Error("k3s agent token not written")
	}
	// The kubehz agent token is made here; only its sha256 went to the api.
	agentToken := strings.TrimSpace(readFile(t, dir, AgentTokenFile))
	if _, err := config.ValidateToken(agentToken); err != nil {
		t.Fatalf("agent token: %v", err)
	}
	if req.AgentTokenHash != sha256Hex(agentToken) || a.agentTokenHash != req.AgentTokenHash {
		t.Fatalf("agentTokenHash %q is not the sha256 of the token on the machine", req.AgentTokenHash)
	}
	raw, _ := json.Marshal(a.redeemRaw[0])
	if strings.Contains(string(raw), agentToken) || strings.Contains(string(raw), strings.TrimPrefix(agentToken, "khz_agt_")) {
		t.Fatal("the redeem body has the agent token")
	}
	env := readFile(t, dir, InstallEnvFile)
	for _, line := range []string{
		"ROLE=server", "KIND=install", "CLUSTER_ID=c1", "MACHINE_ID=m-1", "STATE_ENDPOINT=https://abc.state.kubehz.cloud:2379",
		"K3S_VERSION=v1.34.2+k3s1", "CILIUM_VERSION=1.20.4", "CLUSTER_CIDR=10.42.0.0/16", "SERVICE_CIDR=10.43.0.0/16",
		"TLS_SANS=k3s.example.com,203.0.113.7", "HAS_AUTHN_CONFIG=1", "HAS_AGENT_TOKEN=1",
	} {
		if !strings.Contains(env, line+"\n") {
			t.Errorf("install.env has no line %q:\n%s", line, env)
		}
	}
	for _, secret := range []string{testK3sToken, testK3sAgentToken, agentToken} {
		if strings.Contains(env, secret) {
			t.Error("install.env holds a token")
		}
	}
	s, err := ReadSettings(dir)
	if err != nil || s.ClusterID != testCluster || s.MachineID != testMachine || s.APIURL != srv.URL {
		t.Fatalf("settings %+v, %v", s, err)
	}

	// The first certificate is PENDING at the api (contract 10.1): the
	// generation waits for its acknowledgement, with no previous one, and
	// carries the api's serial, notAfter and CA generation.
	m, err := d.Store.ReadMeta(1)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := (&Renewer{Store: d.Store}).readCert(1)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != StateSwitched || m.Previous != 0 || m.CAGeneration != 1 || m.Serial != serialHex(cert) ||
		!m.NotAfter.Equal(cert.NotAfter) || !m.RestartedAt.IsZero() {
		t.Fatalf("meta %+v", m)
	}
	if a.active != "" || len(a.pending) != 1 {
		t.Fatalf("the api's machine is active (%q) before an acknowledgement", a.active)
	}

	// The renewer can sign with what the install wrote.
	e := &Renewer{Store: d.Store}
	if _, err := e.signer(1); err != nil {
		t.Fatalf("signer from the installed generation: %v", err)
	}
}

// An answer with only the api's fields installs: no SANs, no
// authentication configuration.
func TestRedeemWithTheAPIShape(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	d := newRedeemer(t, srv)
	if _, err := d.Run(context.Background(), testTicket); err != nil {
		t.Fatal(err)
	}
	env := readFile(t, d.Store.Dir, InstallEnvFile)
	for _, line := range []string{"ROLE=server", "CLUSTER_ID=c1", "TLS_SANS=", "HAS_AUTHN_CONFIG=0", "HAS_AGENT_TOKEN=1"} {
		if !strings.Contains(env, line+"\n") {
			t.Errorf("install.env has no line %q:\n%s", line, env)
		}
	}
	if strings.Contains(env, "SERVER_URL") || strings.Contains(env, "SSO") {
		t.Errorf("a server's install.env names a k3s server URL or SSO bindings:\n%s", env)
	}
	if _, err := os.Stat(filepath.Join(d.Store.Dir, AuthnConfigFile)); !errors.Is(err, os.ErrNotExist) {
		t.Error("authentication configuration written without data")
	}
}

// A second server of a cluster: the cluster has its agent token. The api
// refuses the hash and leaves the ticket unused; the redemption without the
// hash installs, and this machine keeps no agent token.
func TestRedeemSecondServerMakesNoAgentToken(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	a.agentTokenHash = sha256Hex("khz_agt_" + strings.Repeat("a", 64))
	d := newRedeemer(t, srv)
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if res.AgentToken || len(a.redeemed) != 2 {
		t.Fatalf("result %+v, redemptions %d", res, len(a.redeemed))
	}
	if a.redeemed[0].AgentTokenHash == "" {
		t.Fatal("the first redemption sent no hash")
	}
	if _, ok := a.redeemRaw[1]["agentTokenHash"]; ok {
		t.Fatal("the second redemption sent a hash again")
	}
	if a.agentTokenHash != sha256Hex("khz_agt_"+strings.Repeat("a", 64)) {
		t.Fatal("the cluster's agent token changed")
	}
	if _, err := os.Stat(filepath.Join(d.Store.Dir, AgentTokenFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("an agent token file without a registered hash")
	}
	if env := readFile(t, d.Store.Dir, InstallEnvFile); !strings.Contains(env, "HAS_AGENT_TOKEN=0\n") {
		t.Fatalf("install.env:\n%s", env)
	}
}

// Another cluster has the hash (a token made two times): the api leaves the
// ticket unused, and the machine makes a new token.
func TestRedeemAgentTokenConflict(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	a.conflicts = 1
	d := newRedeemer(t, srv)
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if !res.AgentToken || len(a.redeemed) != 2 || a.redeemed[0].AgentTokenHash == a.redeemed[1].AgentTokenHash {
		t.Fatalf("result %+v, redemptions %+v", res, a.redeemed)
	}
	if got := sha256Hex(strings.TrimSpace(readFile(t, d.Store.Dir, AgentTokenFile))); got != a.agentTokenHash {
		t.Fatal("the agent token on the machine is not the registered one")
	}
}

// A recovery sends no agent token hash: the cluster has its token.
func TestRedeemRecovery(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	a.ticketKind = KindRecovery
	a.agentTokenHash = sha256Hex("khz_agt_" + strings.Repeat("a", 64))
	d := newRedeemer(t, srv)

	// Without --recovery, the api refuses the hash, and the ticket stays
	// unused.
	_, err := d.Run(context.Background(), testTicket)
	if err == nil || !strings.Contains(err.Error(), "takes no agentTokenHash") || a.ticketUsed {
		t.Fatalf("recovery ticket without --recovery: err = %v, used %v", err, a.ticketUsed)
	}

	d.Recovery = true
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if res.AgentToken {
		t.Fatal("a recovery made an agent token")
	}
	if _, ok := a.redeemRaw[len(a.redeemRaw)-1]["agentTokenHash"]; ok {
		t.Fatal("a recovery sent an agent token hash")
	}
	if _, err := os.Stat(filepath.Join(d.Store.Dir, AgentTokenFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a recovery wrote an agent token")
	}
	env := readFile(t, d.Store.Dir, InstallEnvFile)
	if !strings.Contains(env, "KIND=recovery\n") || !strings.Contains(env, "HAS_AGENT_TOKEN=0\n") {
		t.Fatalf("install.env:\n%s", env)
	}

	// --recovery with an install ticket: the answer is refused.
	b, srv2 := newFakeAPI(t)
	b.redeem = apiRedeem(b)
	d2 := newRedeemer(t, srv2)
	d2.Recovery = true
	if _, err := d2.Run(context.Background(), testTicket); err == nil || !strings.Contains(err.Error(), "use a recovery ticket") {
		t.Fatalf("install ticket with --recovery: err = %v", err)
	}
	if _, ok := b.redeemRaw[0]["agentTokenHash"]; ok {
		t.Fatal("a run with --recovery sent an agent token hash")
	}
}

func TestRedeemRefusesASecondInstall(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, srv)
	if _, err := d.Run(context.Background(), testTicket); err != nil {
		t.Fatal(err)
	}
	// A second redemption of the same ticket would revoke this machine.
	_, err := d.Run(context.Background(), testTicket)
	if !errors.Is(err, ErrInstalled) {
		t.Fatalf("err = %v, want ErrInstalled", err)
	}
	if len(a.redeemed) != 1 || a.revoked {
		t.Fatal("the second run redeemed a ticket")
	}
}

func TestRedeemRefusedTicketLeavesNoState(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, srv)
	_, err := d.Run(context.Background(), "khz_tkt_used0000000000000000")
	if err == nil || !strings.Contains(err.Error(), "make a new ticket in the dashboard") || !strings.Contains(err.Error(), "STATE_TICKET_INVALID") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), "khz_tkt_used") {
		t.Fatal("the error shows the ticket")
	}
	if gens, _ := d.Store.Gens(); len(gens) != 0 {
		t.Fatalf("generations %v left after a refused ticket", gens)
	}
	if _, err := d.Store.Current(); !errors.Is(err, ErrNoCurrent) {
		t.Fatal("current exists after a refused ticket")
	}
}

func TestRedeemUsedTicket(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	a.ticketUsed = true
	d := newRedeemer(t, srv)
	_, err := d.Run(context.Background(), testTicket)
	if err == nil || !strings.Contains(err.Error(), "redeemed before") || !strings.Contains(err.Error(), "STATE_TICKET_USED") {
		t.Fatalf("err = %v", err)
	}
	if _, err := d.Store.Current(); !errors.Is(err, ErrNoCurrent) {
		t.Fatal("current exists after a used ticket")
	}
}

func TestRedeemRoleIsTheTicketRole(t *testing.T) {
	for _, tc := range []struct{ ticket, run string }{{RoleAgent, RoleServer}, {RoleServer, RoleAgent}} {
		t.Run(tc.run+" with a "+tc.ticket+" ticket", func(t *testing.T) {
			a, srv := newFakeAPI(t)
			a.redeem = goodRedeem(a)
			a.ticketRole = tc.ticket
			d := newRedeemer(t, srv)
			d.Role = tc.run
			_, err := d.Run(context.Background(), testTicket)
			if err == nil || !strings.Contains(err.Error(), "the ticket is not used yet") {
				t.Fatalf("err = %v", err)
			}
			if a.ticketUsed {
				t.Fatal("the api spent the ticket")
			}
			if entries, _ := os.ReadDir(d.Store.Dir); len(entries) != 0 {
				t.Fatalf("files left: %v", entries)
			}
		})
	}
	a, srv := newFakeAPI(t)
	d := newRedeemer(t, srv)
	d.Role = "worker"
	if _, err := d.Run(context.Background(), testTicket); err == nil || len(a.redeemed) != 0 {
		t.Fatalf("role worker: err = %v, redeemed %d", err, len(a.redeemed))
	}
}

func TestRedeemRefusesBadAnswers(t *testing.T) {
	cases := map[string]func(*RedeemResponse){
		"endpoint not https":     func(r *RedeemResponse) { r.Endpoint = "http://abc.state.kubehz.cloud:2379" },
		"endpoint with path":     func(r *RedeemResponse) { r.Endpoint = "https://abc.state.kubehz.cloud:2379/x" },
		"endpoint without port":  func(r *RedeemResponse) { r.Endpoint = "https://abc.state.kubehz.cloud" },
		"cluster id with slash":  func(r *RedeemResponse) { r.InstallConfig.ClusterID = "../c1" },
		"another cluster id":     func(r *RedeemResponse) { r.InstallConfig.ClusterID = "c2" },
		"no cluster id":          func(r *RedeemResponse) { r.InstallConfig.ClusterID = "" },
		"Cilium version channel": func(r *RedeemResponse) { r.InstallConfig.CiliumVersion = "latest" },
		"Cilium version flag":    func(r *RedeemResponse) { r.InstallConfig.CiliumVersion = "1.20.4 --x" },
		"no k3s agent token":     func(r *RedeemResponse) { r.K3sAgentToken = "" },
		"one token for both":     func(r *RedeemResponse) { r.K3sAgentToken = r.K3sToken },
		"agent token newline":    func(r *RedeemResponse) { r.K3sAgentToken = "K10abc\nINJECT=1" },
		"kind recovery":          func(r *RedeemResponse) { r.Kind = KindRecovery },
		"k3s version channel":    func(r *RedeemResponse) { r.InstallConfig.K3sVersion = "stable" },
		"cluster CIDR host bits": func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "10.42.0.1/16" },
		"cluster CIDR text":      func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "10.42.0.0/16 --x" },
		"cluster CIDR IPv6":      func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "fd00::/56" },
		"service CIDR dual":      func(r *RedeemResponse) { r.InstallConfig.ServiceCIDR = "10.43.0.0/16,fd01::/108" },
		"server ticket with URL": func(r *RedeemResponse) { r.InstallConfig.ServerURL = testServerURL },
		"SAN with space":         func(r *RedeemResponse) { r.InstallConfig.TLSSANs = []string{"a b"} },
		"token with newline":     func(r *RedeemResponse) { r.K3sToken = "K10abc\nINJECT=1" },
		"short token":            func(r *RedeemResponse) { r.K3sToken = "x" },
		"machine id with slash":  func(r *RedeemResponse) { r.MachineID = "m/1" },
		"another machine id":     func(r *RedeemResponse) { r.MachineID = "m-9" },
		"serial of another cert": func(r *RedeemResponse) { r.Serial = "4abc" },
		"notAfter of another":    func(r *RedeemResponse) { r.NotAfter = r.NotAfter.Add(time.Hour) },
		"no CA generation":       func(r *RedeemResponse) { r.CAGeneration = 0 },
		"authn not authn": func(r *RedeemResponse) {
			r.InstallConfig.AuthenticationConfig = "apiVersion: v1\nkind: ConfigMap\n"
		},
		"serving CA not a CA": func(r *RedeemResponse) { r.ServingCA = r.Certificate },
		"certificate for another key": func(r *RedeemResponse) {
			k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			r.Certificate = selfSigned(&k.PublicKey, k, false)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a, srv := newFakeAPI(t)
			good := goodRedeem(a)
			a.redeem = func(req RedeemRequest, pub any) (RedeemResponse, int) {
				r, s := good(req, pub)
				mutate(&r)
				return r, s
			}
			d := newRedeemer(t, srv)
			if _, err := d.Run(context.Background(), testTicket); err == nil {
				t.Fatal("accepted")
			} else if strings.Contains(err.Error(), testK3sToken) || strings.Contains(err.Error(), testK3sAgentToken) || strings.Contains(err.Error(), "INJECT") {
				t.Fatalf("the error shows the token: %v", err)
			}
			if _, err := d.Store.Current(); !errors.Is(err, ErrNoCurrent) {
				t.Fatal("current exists after a refused answer")
			}
		})
	}
	// A null serial is allowed (contract 10.4): the certificate has one.
	a, srv := newFakeAPI(t)
	good := apiRedeem(a)
	a.redeem = func(req RedeemRequest, pub any) (RedeemResponse, int) {
		r, s := good(req, pub)
		r.Serial = ""
		return r, s
	}
	d := newRedeemer(t, srv)
	if _, err := d.Run(context.Background(), testTicket); err != nil {
		t.Fatalf("null serial: %v", err)
	}
}

// An agent node gets no datastore identity (contract 10.5): it sends no
// CSR, writes no key, no certificate, no generation and no renewer
// settings, only the k3s token and install.env.
func TestRedeemAgent(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = agentRedeem(a)
	a.ticketRole = RoleAgent
	d := newRedeemer(t, srv)
	d.Role = RoleAgent
	d.Label = "agent-1"
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if res.Role != RoleAgent || res.MachineID != "m-2" || res.ClusterID != "" {
		t.Fatalf("result %+v", res)
	}
	raw := a.redeemRaw[0]
	if _, ok := raw["csr"]; ok {
		t.Fatalf("an agent sent a CSR: %v", raw)
	}
	if _, ok := raw["agentTokenHash"]; ok {
		t.Fatalf("an agent sent an agent token hash: %v", raw)
	}
	if raw["role"] != RoleAgent || raw["machineLabel"] != "agent-1" {
		t.Fatalf("redeem body %v", raw)
	}
	entries, err := os.ReadDir(d.Store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if strings.Join(names, ",") != InstallEnvFile+","+TokenFile {
		t.Fatalf("an agent's state folder holds %v, want only %s and %s", names, InstallEnvFile, TokenFile)
	}
	for _, f := range []string{TokenFile, InstallEnvFile} {
		if mode := fileMode(t, filepath.Join(d.Store.Dir, f)); mode != 0o600 {
			t.Errorf("%s mode %o", f, mode)
		}
	}
	// An agent node joins with the k3s agent token only.
	if strings.TrimSpace(readFile(t, d.Store.Dir, TokenFile)) != testK3sAgentToken {
		t.Error("k3s agent token not written")
	}
	env := readFile(t, d.Store.Dir, InstallEnvFile)
	want := "ROLE=agent\nMACHINE_ID=m-2\nSERVER_URL=" + testServerURL + "\nK3S_VERSION=v1.34.2+k3s1\n"
	if env != want {
		t.Fatalf("install.env:\n%s\nwant:\n%s", env, want)
	}
	if a.active != "" || len(a.pending) != 0 {
		t.Fatal("the api issued a certificate for an agent")
	}

	// A second run does not redeem again.
	if _, err := d.Run(context.Background(), testTicket); !errors.Is(err, ErrInstalled) || len(a.redeemed) != 1 {
		t.Fatalf("second run: err = %v, redeemed %d", err, len(a.redeemed))
	}
	// A run as server on this agent node does not redeem either.
	d.Role = RoleServer
	_, err = d.Run(context.Background(), testTicket)
	if !errors.Is(err, ErrInstalled) || !strings.Contains(err.Error(), "as role agent") || len(a.redeemed) != 1 {
		t.Fatalf("run as server: err = %v, redeemed %d", err, len(a.redeemed))
	}
	if gens, _ := d.Store.Gens(); len(gens) != 0 {
		t.Fatalf("a run as server made generations %v", gens)
	}
}

func TestRedeemAgentRefusesBadAnswers(t *testing.T) {
	cases := map[string]func(*RedeemResponse){
		"a certificate":         func(r *RedeemResponse) { r.Certificate = "-----BEGIN CERTIFICATE-----" },
		"a serial":              func(r *RedeemResponse) { r.Serial = "4abc" },
		"a notAfter":            func(r *RedeemResponse) { r.NotAfter = time.Now().Add(time.Hour) },
		"a CA generation":       func(r *RedeemResponse) { r.CAGeneration = 1 },
		"a serving CA":          func(r *RedeemResponse) { r.ServingCA = "-----BEGIN CERTIFICATE-----" },
		"an endpoint":           func(r *RedeemResponse) { r.Endpoint = "https://abc.state.kubehz.cloud:2379" },
		"no server URL":         func(r *RedeemResponse) { r.InstallConfig.ServerURL = "" },
		"server URL http":       func(r *RedeemResponse) { r.InstallConfig.ServerURL = "http://k3s.example.com:6443" },
		"server URL with path":  func(r *RedeemResponse) { r.InstallConfig.ServerURL = testServerURL + "/x" },
		"server URL with quote": func(r *RedeemResponse) { r.InstallConfig.ServerURL = `https://k3s"x` },
		"server URL bad port":   func(r *RedeemResponse) { r.InstallConfig.ServerURL = "https://k3s.example.com:99999" },
		"server URL bad IPv6":   func(r *RedeemResponse) { r.InstallConfig.ServerURL = "https://[1.2.3.4]:6443" },
		"k3s version channel":   func(r *RedeemResponse) { r.InstallConfig.K3sVersion = "stable" },
		"token with newline":    func(r *RedeemResponse) { r.K3sToken = "K10abc\nINJECT=1" },
		"machine id with slash": func(r *RedeemResponse) { r.MachineID = "m/2" },
		"the server token too":  func(r *RedeemResponse) { r.K3sAgentToken = testK3sToken },
		"a cluster id":          func(r *RedeemResponse) { r.InstallConfig.ClusterID = testCluster },
		"TLS SANs":              func(r *RedeemResponse) { r.InstallConfig.TLSSANs = []string{"k3s.example.com"} },
		"kind recovery":         func(r *RedeemResponse) { r.Kind = KindRecovery },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a, srv := newFakeAPI(t)
			good := agentRedeem(a)
			a.ticketRole = RoleAgent
			a.redeem = func(req RedeemRequest, pub any) (RedeemResponse, int) {
				r, s := good(req, pub)
				mutate(&r)
				return r, s
			}
			d := newRedeemer(t, srv)
			d.Role = RoleAgent
			if _, err := d.Run(context.Background(), testTicket); err == nil {
				t.Fatal("accepted")
			} else if strings.Contains(err.Error(), testK3sToken) || strings.Contains(err.Error(), testK3sAgentToken) || strings.Contains(err.Error(), "INJECT") {
				t.Fatalf("the error shows the token: %v", err)
			}
			if _, err := os.Stat(filepath.Join(d.Store.Dir, InstallEnvFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("install.env written for a refused answer")
			}
		})
	}
	// An IPv6 server URL is valid.
	a, srv := newFakeAPI(t)
	good := agentRedeem(a)
	a.ticketRole = RoleAgent
	a.redeem = func(req RedeemRequest, pub any) (RedeemResponse, int) {
		r, s := good(req, pub)
		r.InstallConfig.ServerURL = "https://[2001:db8::1]:6443"
		return r, s
	}
	d := newRedeemer(t, srv)
	d.Role = RoleAgent
	if _, err := d.Run(context.Background(), testTicket); err != nil {
		t.Fatalf("IPv6 server URL: %v", err)
	}
}

func TestReadTicket(t *testing.T) {
	if got, err := ReadTicket(strings.NewReader(testTicket + "\n")); err != nil || got != testTicket {
		t.Fatalf("ReadTicket = %q, %v", got, err)
	}
	for name, in := range map[string]string{
		"empty":      "\n",
		"short":      "abc\n",
		"with space": "khz_tkt_0123 456789abcdef0123456789\n",
		"too long":   strings.Repeat("a", 600) + "\n",
	} {
		_, err := ReadTicket(strings.NewReader(in))
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if len(strings.TrimSpace(in)) > 4 && strings.Contains(err.Error(), strings.TrimSpace(in)[:5]) {
			t.Errorf("%s: the error shows the ticket: %v", name, err)
		}
	}
}

func TestLock(t *testing.T) {
	dir := t.TempDir()
	release, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Lock(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v, want ErrLocked", err)
	}
	release()
	release2, err := Lock(dir)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release2()
}

func TestStoreSwitchIsOneRename(t *testing.T) {
	s := Store{Dir: t.TempDir()}
	for range 2 {
		if _, err := s.NewGen(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Switch(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Switch(2); err != nil {
		t.Fatal(err)
	}
	if target, _ := os.Readlink(filepath.Join(s.Dir, currentLink)); target != "gen-2" {
		t.Fatalf("current -> %q", target)
	}
	if _, err := os.Lstat(filepath.Join(s.Dir, currentLink+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary link left")
	}
	if err := s.Remove(2); err == nil {
		t.Fatal("removed the current generation")
	}
}

// selfSigned returns a PEM certificate for pub signed by key.
func selfSigned(pub any, key *ecdsa.PrivateKey, isCA bool) string {
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "x"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: isCA, BasicConstraintsValid: true,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: nil,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

// fakeK3s is a local k3s apiserver for SystemHost: it answers /readyz/etcd
// when ready is true. It returns the path of a kubeconfig as k3s writes it.
func fakeK3s(t *testing.T, ready *atomic.Bool) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz/etcd" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !ready.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("[-]etcd failed: reason withheld"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)

	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	keyDER, _ := x509.MarshalECPrivateKey(k)
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	kc := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: %s
    server: %s
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: %s
    client-key-data: %s
`, b64(caPEM), srv.URL, b64([]byte(selfSigned(&k.PublicKey, k, false))), b64(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	path := filepath.Join(t.TempDir(), "k3s.yaml")
	if err := os.WriteFile(path, []byte(kc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSystemHost(t *testing.T) {
	var ready atomic.Bool
	path := fakeK3s(t, &ready)

	pids := []string{"100", "100", "100", "200"}
	var calls []string
	h := NewSystemHost("k3s", path, "/usr/local/bin/k3s")
	h.poll = 10 * time.Millisecond
	h.run = func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		switch {
		case name == "/usr/local/bin/k3s":
			return []byte("k3s version v1.34.2+k3s1 (abcdef12)\ngo version go1.24.4\n"), nil
		case len(args) > 0 && args[0] == "show":
			p := pids[0]
			if len(pids) > 1 {
				pids = pids[1:]
			}
			if p == "200" {
				ready.Store(true)
			}
			return []byte(p + "\n"), nil
		case len(args) > 0 && args[0] == "restart":
			return nil, nil
		}
		return nil, errors.New("unexpected command")
	}

	if v, err := h.K3sVersion(context.Background()); err != nil || v != "v1.34.2+k3s1" {
		t.Fatalf("K3sVersion = %q, %v", v, err)
	}
	if err := h.RestartK3s(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !contains(calls, "systemctl restart --no-block k3s") {
		t.Fatalf("calls %v", calls)
	}
	// The old process answers "not ready"; the wait ends only with a new
	// PID AND a ready datastore.
	if err := h.WaitDatastore(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("WaitDatastore: %v", err)
	}

	// The timeout returns the last error.
	ready.Store(false)
	h.pidBefore = 0
	err := h.WaitDatastore(context.Background(), 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatalf("WaitDatastore after the timeout = %v", err)
	}

	// The Plans check reads the local apiserver (no Plan resource: no upgrade).
	if busy, err := h.UpgradeInProgress(context.Background()); err != nil || busy {
		t.Fatalf("UpgradeInProgress = %t, %v", busy, err)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
