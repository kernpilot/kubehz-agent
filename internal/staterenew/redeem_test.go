package staterenew

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

const (
	testTicket     = "khz_tkt_0123456789abcdef0123456789abcdef"
	testK3sToken   = "K10abcdef0123456789::server:0123456789abcdef"
	testAgentToken = "khz_agt_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testAuthn      = "apiVersion: apiserver.config.k8s.io/v1\nkind: AuthenticationConfiguration\njwt: []\n"
)

func goodRedeem(a *fakeAPI) func(RedeemRequest, any) (RedeemResponse, int) {
	return func(req RedeemRequest, pub any) (RedeemResponse, int) {
		if req.Ticket != testTicket {
			return RedeemResponse{}, http.StatusGone
		}
		cert, serial := a.issue(pub)
		a.active = serial
		return RedeemResponse{
			MachineID:   "m-1",
			Certificate: cert,
			ServingCA:   string(a.ca().pem),
			Endpoint:    "https://abc.state.kubehz.cloud:2379",
			K3sToken:    testK3sToken,
			AgentToken:  testAgentToken,
			InstallConfig: InstallConfig{
				ClusterID:            testCluster,
				K3sVersion:           "v1.34.2+k3s1",
				ClusterCIDR:          "10.42.0.0/16",
				ServiceCIDR:          "10.43.0.0/16",
				TLSSANs:              []string{"k3s.example.com", "203.0.113.7"},
				AuthenticationConfig: testAuthn,
				RoleBindings: []RoleBinding{
					{Name: "admin", ClusterRole: "cluster-admin", Groups: []string{"kubehz:c1:admin"}},
					{Name: "view", ClusterRole: "view", Groups: []string{"kubehz:c1:view"}},
				},
			},
		}, 0
	}
}

func newRedeemer(t *testing.T, a *fakeAPI, srv *httptest.Server) *Redeemer {
	t.Helper()
	client, err := NewClient(srv.URL, "", "test", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	return &Redeemer{Store: Store{Dir: filepath.Join(t.TempDir(), "state")}, API: client, APIURL: srv.URL, Label: "server-1"}
}

func TestRedeemWritesTheStateFolder(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, a, srv)
	res, err := d.Run(context.Background(), testTicket)
	if err != nil {
		t.Fatal(err)
	}
	if res.ClusterID != testCluster || res.MachineID != "m-1" {
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
		AgentTokenFile:                     0o600,
		ConfigFile:                         0o600,
		AuthnConfigFile:                    0o600,
		SSOManifestFile:                    0o600,
		InstallEnvFile:                     0o600,
	} {
		if got := fileMode(t, filepath.Join(dir, name)); got != want {
			t.Errorf("%s mode %o, want %o", name, got, want)
		}
	}
	if fileMode(t, dir) != 0o700 {
		t.Errorf("state folder mode %o", fileMode(t, dir))
	}
	if strings.TrimSpace(readFile(t, dir, TokenFile)) != testK3sToken {
		t.Error("k3s token not written")
	}
	env := readFile(t, dir, InstallEnvFile)
	for _, line := range []string{
		"CLUSTER_ID=c1", "MACHINE_ID=m-1", "STATE_ENDPOINT=https://abc.state.kubehz.cloud:2379",
		"K3S_VERSION=v1.34.2+k3s1", "CLUSTER_CIDR=10.42.0.0/16", "SERVICE_CIDR=10.43.0.0/16",
		"TLS_SANS=k3s.example.com,203.0.113.7", "HAS_AUTHN_CONFIG=1", "HAS_SSO_BINDINGS=1", "HAS_AGENT_TOKEN=1",
	} {
		if !strings.Contains(env, line+"\n") {
			t.Errorf("install.env has no line %q:\n%s", line, env)
		}
	}
	if strings.Contains(env, testK3sToken) || strings.Contains(env, testAgentToken) {
		t.Error("install.env holds a token")
	}
	s, err := ReadSettings(dir)
	if err != nil || s.ClusterID != testCluster || s.MachineID != "m-1" || s.APIURL != srv.URL {
		t.Fatalf("settings %+v, %v", s, err)
	}
	docs := strings.Split(readFile(t, dir, SSOManifestFile), "---\n")
	var crbs []rbacv1.ClusterRoleBinding
	for _, doc := range docs[1:] {
		var crb rbacv1.ClusterRoleBinding
		if err := yaml.UnmarshalStrict([]byte(doc), &crb); err != nil {
			t.Fatalf("SSO manifest: %v\n%s", err, doc)
		}
		crbs = append(crbs, crb)
	}
	if len(crbs) != 2 || crbs[0].Name != "kubehz-sso-admin" || crbs[0].RoleRef.Name != "cluster-admin" ||
		crbs[0].Subjects[0].Kind != rbacv1.GroupKind || crbs[0].Subjects[0].Name != "kubehz:c1:admin" {
		t.Fatalf("SSO bindings %+v", crbs)
	}
	m, err := d.Store.ReadMeta(1)
	if err != nil || m.State != StateAcked || m.Serial != a.active {
		t.Fatalf("meta %+v, %v", m, err)
	}

	// The renewer can sign with what the install wrote.
	e := &Renewer{Store: d.Store}
	if _, err := e.signer(1); err != nil {
		t.Fatalf("signer from the installed generation: %v", err)
	}
}

func TestRedeemRefusesASecondInstall(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, a, srv)
	if _, err := d.Run(context.Background(), testTicket); err != nil {
		t.Fatal(err)
	}
	_, err := d.Run(context.Background(), testTicket)
	if !errors.Is(err, ErrInstalled) {
		t.Fatalf("err = %v, want ErrInstalled", err)
	}
	if len(a.redeemed) != 1 {
		t.Fatal("the second run redeemed a ticket")
	}
}

func TestRedeemRefusedTicketLeavesNoState(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, a, srv)
	_, err := d.Run(context.Background(), "khz_tkt_used0000000000000000")
	if err == nil || !strings.Contains(err.Error(), "make a new ticket in the dashboard") {
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

func TestRedeemRefusesBadAnswers(t *testing.T) {
	cases := map[string]func(*RedeemResponse){
		"endpoint not https":     func(r *RedeemResponse) { r.Endpoint = "http://abc.state.kubehz.cloud:2379" },
		"endpoint with path":     func(r *RedeemResponse) { r.Endpoint = "https://abc.state.kubehz.cloud:2379/x" },
		"endpoint without port":  func(r *RedeemResponse) { r.Endpoint = "https://abc.state.kubehz.cloud" },
		"cluster id with slash":  func(r *RedeemResponse) { r.InstallConfig.ClusterID = "../c1" },
		"k3s version channel":    func(r *RedeemResponse) { r.InstallConfig.K3sVersion = "stable" },
		"cluster CIDR host bits": func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "10.42.0.1/16" },
		"cluster CIDR text":      func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "10.42.0.0/16 --x" },
		"cluster CIDR IPv6":      func(r *RedeemResponse) { r.InstallConfig.ClusterCIDR = "fd00::/56" },
		"service CIDR dual":      func(r *RedeemResponse) { r.InstallConfig.ServiceCIDR = "10.43.0.0/16,fd01::/108" },
		"SAN with space":         func(r *RedeemResponse) { r.InstallConfig.TLSSANs = []string{"a b"} },
		"token with newline":     func(r *RedeemResponse) { r.K3sToken = "K10abc\nINJECT=1" },
		"short token":            func(r *RedeemResponse) { r.K3sToken = "x" },
		"bad agent token":        func(r *RedeemResponse) { r.AgentToken = "nope" },
		"machine id with slash":  func(r *RedeemResponse) { r.MachineID = "m/1" },
		"system:masters role":    func(r *RedeemResponse) { r.InstallConfig.RoleBindings[0].ClusterRole = "system:masters" },
		"binding without group":  func(r *RedeemResponse) { r.InstallConfig.RoleBindings[0].Groups = nil },
		"binding name invalid":   func(r *RedeemResponse) { r.InstallConfig.RoleBindings[0].Name = "Admin!" },
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
			d := newRedeemer(t, a, srv)
			if _, err := d.Run(context.Background(), testTicket); err == nil {
				t.Fatal("accepted")
			} else if strings.Contains(err.Error(), testK3sToken) || strings.Contains(err.Error(), "INJECT") {
				t.Fatalf("the error shows the token: %v", err)
			}
			if _, err := d.Store.Current(); !errors.Is(err, ErrNoCurrent) {
				t.Fatal("current exists after a refused answer")
			}
		})
	}
}

func TestRedeemRefusesAgentRole(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	d := newRedeemer(t, a, srv)
	d.Role = "agent"
	if _, err := d.Run(context.Background(), testTicket); err == nil || len(a.redeemed) != 0 {
		t.Fatalf("err = %v, redeemed %d", err, len(a.redeemed))
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

func TestSystemHost(t *testing.T) {
	var ready atomic.Bool
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
	defer srv.Close()

	// A kubeconfig as k3s writes it.
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
