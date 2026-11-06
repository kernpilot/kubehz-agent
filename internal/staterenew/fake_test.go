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
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testCluster = "c1"

var errCrash = errors.New("simulated crash")

// testCA is one client CA generation of the fake api.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
	gen  int64
}

func newTestCA(t *testing.T, gen int64) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(gen),
		Subject:               pkix.Name{CommonName: fmt.Sprintf("test client CA g%d", gen)},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), gen: gen}
}

type pendingCert struct {
	pem    string
	serial string
	caGen  int64
}

// fakeAPI is the machine side of the kubehz api (build contract 4.2 and
// 4.3). It verifies every signed request as the api must.
type fakeAPI struct {
	t   *testing.T
	mu  sync.Mutex
	cas []*testCA // the last one issues

	active  string                  // serial of the active machine certificate
	pending map[string]*pendingCert // by public key and CA generation

	due, paused bool
	dropRenew   int // renew answers to lose (the request is processed)
	ackStatus   int // answer every ack with this status (0: normal)

	csrs   []string
	checks []CheckRequest
	acks   int
	jti    map[string]bool

	redeemed []RedeemRequest
	redeem   func(req RedeemRequest, pub any) (RedeemResponse, int)
}

func newFakeAPI(t *testing.T) (*fakeAPI, *httptest.Server) {
	t.Helper()
	a := &fakeAPI{t: t, cas: []*testCA{newTestCA(t, 1)}, pending: map[string]*pendingCert{}, jti: map[string]bool{}}
	srv := httptest.NewServer(a)
	t.Cleanup(srv.Close)
	return a, srv
}

func (a *fakeAPI) ca() *testCA { return a.cas[len(a.cas)-1] }

func (a *fakeAPI) rotateCA() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cas = append(a.cas, newTestCA(a.t, a.ca().gen+1))
	a.pending = map[string]*pendingCert{} // a CA rotation drops every pending certificate
}

// issue signs a client certificate for pub with the newest CA.
func (a *fakeAPI) issue(pub any) (string, string) {
	ca := a.ca()
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "system:kine:" + testCluster},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		a.t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), serial.Text(16)
}

func (a *fakeAPI) problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "detail": detail})
}

// verify checks the signed request and returns the signing certificate.
func (a *fakeAPI) verify(r *http.Request, body []byte) (*x509.Certificate, error) {
	parts := strings.Split(r.Header.Get(SignatureHeader), ".")
	if len(parts) != 3 {
		return nil, errors.New("no compact JWS")
	}
	hb, err1 := b64.DecodeString(parts[0])
	pb, err2 := b64.DecodeString(parts[1])
	sig, err3 := b64.DecodeString(parts[2])
	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, err
	}
	var h sigHeader
	var p sigPayload
	if err := errors.Join(json.Unmarshal(hb, &h), json.Unmarshal(pb, &p)); err != nil {
		return nil, err
	}
	if h.Alg != "ES256" || len(h.X5C) != 1 {
		return nil, fmt.Errorf("header %+v", h)
	}
	der, err := base64.StdEncoding.DecodeString(h.X5C[0])
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	for _, ca := range a.cas {
		roots.AddCert(ca.cert)
	}
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return nil, fmt.Errorf("chain: %w", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	pub := cert.PublicKey.(*ecdsa.PublicKey)
	if len(sig) != 64 || !ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return nil, errors.New("bad signature")
	}
	sum := sha256.Sum256(body)
	switch {
	case p.Method != r.Method:
		return nil, errors.New("method mismatch")
	case p.Path != r.URL.Path:
		return nil, errors.New("path mismatch")
	case p.BodySHA256 != hex.EncodeToString(sum[:]):
		return nil, errors.New("body hash mismatch")
	case p.ClusterID != testCluster:
		return nil, errors.New("cluster mismatch")
	case time.Since(time.Unix(p.IAT, 0)).Abs() > 5*time.Minute:
		return nil, errors.New("iat out of window")
	case a.jti[p.JTI] || len(p.JTI) != 32:
		return nil, errors.New("jti replayed or malformed")
	}
	a.jti[p.JTI] = true
	return cert, nil
}

func (a *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	if r.URL.Path == pathRedeem {
		a.serveRedeem(w, r, body)
		return
	}
	cert, err := a.verify(r, body)
	if err != nil {
		a.problem(w, http.StatusUnauthorized, "signature: "+err.Error())
		return
	}
	serial := cert.SerialNumber.Text(16)
	switch r.URL.Path {
	case pathCheck:
		if serial != a.active {
			a.problem(w, http.StatusForbidden, "serial is not active")
			return
		}
		var req CheckRequest
		_ = json.Unmarshal(body, &req)
		a.checks = append(a.checks, req)
		_ = json.NewEncoder(w).Encode(CheckResponse{Due: a.due, Paused: a.paused, Reason: "test"})
	case pathRenew:
		if serial != a.active {
			a.problem(w, http.StatusForbidden, "serial is not active")
			return
		}
		var req RenewRequest
		_ = json.Unmarshal(body, &req)
		a.csrs = append(a.csrs, req.CSR)
		block, _ := pem.Decode([]byte(req.CSR))
		if block == nil {
			a.problem(w, http.StatusBadRequest, "no CSR")
			return
		}
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil {
			a.problem(w, http.StatusBadRequest, "bad CSR")
			return
		}
		pubDER, _ := x509.MarshalPKIXPublicKey(csr.PublicKey)
		k := fmt.Sprintf("%x/%d", sha256.Sum256(pubDER), a.ca().gen)
		pc := a.pending[k]
		if pc == nil {
			c, s := a.issue(csr.PublicKey)
			pc = &pendingCert{pem: c, serial: s, caGen: a.ca().gen}
			a.pending = map[string]*pendingCert{k: pc} // one pending certificate per machine
		}
		if a.dropRenew > 0 {
			a.dropRenew--
			a.problem(w, http.StatusBadGateway, "lost")
			return
		}
		_ = json.NewEncoder(w).Encode(RenewResponse{Certificate: pc.pem, CAGeneration: pc.caGen})
	case pathAck:
		a.acks++
		if a.ackStatus != 0 {
			a.problem(w, a.ackStatus, "ack refused")
			return
		}
		for k, pc := range a.pending {
			if pc.serial == serial {
				a.active = serial
				delete(a.pending, k)
				_ = json.NewEncoder(w).Encode(AckResponse{Serial: serial})
				return
			}
		}
		a.problem(w, http.StatusConflict, "no pending certificate for this serial")
	default:
		a.problem(w, http.StatusNotFound, "no route")
	}
}

func (a *fakeAPI) serveRedeem(w http.ResponseWriter, r *http.Request, body []byte) {
	if r.Header.Get(SignatureHeader) != "" {
		a.problem(w, http.StatusBadRequest, "redeem is not signed")
		return
	}
	var req RedeemRequest
	if err := json.Unmarshal(body, &req); err != nil {
		a.problem(w, http.StatusBadRequest, "bad body")
		return
	}
	a.redeemed = append(a.redeemed, req)
	block, _ := pem.Decode([]byte(req.CSR))
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		a.problem(w, http.StatusBadRequest, "bad CSR")
		return
	}
	resp, status := a.redeem(req, csr.PublicKey)
	if status != 0 {
		a.problem(w, status, "ticket refused")
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// fakeHost is a k3s server: it records the generation that each restart
// loads, and "connects" when connect allows that generation.
type fakeHost struct {
	store    Store
	restarts []int
	connect  func(gen int) error
	plans    bool
	plansErr error
}

func (h *fakeHost) K3sVersion(context.Context) (string, error) { return "v1.34.2+k3s1", nil }

func (h *fakeHost) RestartK3s(context.Context) error {
	cur, err := h.store.Current()
	if err != nil {
		return err
	}
	h.restarts = append(h.restarts, cur)
	return nil
}

func (h *fakeHost) WaitDatastore(context.Context, time.Duration) error {
	if len(h.restarts) == 0 {
		return errors.New("k3s never restarted")
	}
	if h.connect == nil {
		return nil
	}
	return h.connect(h.restarts[len(h.restarts)-1])
}

func (h *fakeHost) UpgradeInProgress(context.Context) (bool, error) { return h.plans, h.plansErr }

// renewEnv is one machine with an installed generation 1.
type renewEnv struct {
	t     *testing.T
	api   *fakeAPI
	host  *fakeHost
	store Store
	now   time.Time
	r     *Renewer
}

func newRenewEnv(t *testing.T) *renewEnv {
	t.Helper()
	api, srv := newFakeAPI(t)
	store := Store{Dir: t.TempDir()}
	n, err := store.NewGen()
	if err != nil || n != 1 {
		t.Fatalf("NewGen = %d, %v", n, err)
	}
	key, keyPEM, err := newKey()
	if err != nil {
		t.Fatal(err)
	}
	cert, serial := api.issue(&key.PublicKey)
	api.active = serial
	mustWrite(t, store.Path(1, KeyFile), keyPEM)
	mustWrite(t, store.Path(1, CertFile), []byte(cert))
	mustWrite(t, store.Path(1, CAFile), []byte("serving CA\n"))
	if err := store.WriteMeta(1, Meta{State: StateAcked, CAGeneration: 1, Serial: serial}); err != nil {
		t.Fatal(err)
	}
	if err := store.Switch(1); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(srv.URL, testCluster, "test", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	e := &renewEnv{t: t, api: api, store: store, host: &fakeHost{store: store}, now: time.Now().UTC().Truncate(time.Second)}
	e.r = &Renewer{
		Store: store, API: client, Host: e.host,
		Now: func() time.Time { return e.now },
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return e
}

func mustWrite(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := WriteFileAtomic(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (e *renewEnv) current() int {
	e.t.Helper()
	n, err := e.store.Current()
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *renewEnv) meta(n int) Meta {
	e.t.Helper()
	m, err := e.store.ReadMeta(n)
	if err != nil {
		e.t.Fatal(err)
	}
	return m
}

func (e *renewEnv) serialOf(n int) string {
	e.t.Helper()
	c, err := e.r.readCert(n)
	if err != nil {
		e.t.Fatal(err)
	}
	return c.SerialNumber.Text(16)
}

// assertComplete checks the crash invariant: "current" points to a
// generation with a key, a certificate for that key, the serving CA and a
// state file.
func (e *renewEnv) assertComplete() {
	e.t.Helper()
	n := e.current()
	for _, f := range []string{KeyFile, CertFile, CAFile, metaFile} {
		if _, err := os.Stat(e.store.Path(n, f)); err != nil {
			e.t.Fatalf("current %s is not complete: %v", genName(n), err)
		}
	}
	s, err := e.r.signer(n)
	if err != nil {
		e.t.Fatalf("current %s: %v", genName(n), err)
	}
	if err := checkClientCert(s.Cert, s.Key, time.Now()); err != nil {
		e.t.Fatalf("current %s: %v", genName(n), err)
	}
}

func (e *renewEnv) gens() []int {
	e.t.Helper()
	g, err := e.store.Gens()
	if err != nil {
		e.t.Fatal(err)
	}
	return g
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Mode().Perm()
}

func readFile(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
