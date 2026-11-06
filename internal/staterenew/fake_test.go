package staterenew

import (
	"bytes"
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

const (
	testCluster = "c1"
	testMachine = "m-1"
)

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
		NotAfter:              time.Now().Add(60 * 24 * time.Hour),
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
	pem      string
	serial   string
	caGen    int64
	notAfter time.Time
}

// fakeAPI is the machine side of the kubehz api (build contract 4.2, 4.3
// and 10). It enforces what the api enforces: strict request bodies, the
// signed method, path (the raw request path, without the query) and body,
// the role rules of a redemption, a first certificate that stays pending
// until it is acknowledged, an ack that answers the same serial again, and
// the revocation of an unacknowledged machine when its ticket is redeemed
// a second time.
type fakeAPI struct {
	t   *testing.T
	mu  sync.Mutex
	cas []*testCA // the last one issues

	active  string                  // serial of the active machine certificate
	pending map[string]*pendingCert // by public key and CA generation
	revoked bool                    // the machine is revoked

	due, paused bool
	dropRenew   int // renew answers to lose (the request is processed)
	ackStatus   int // answer every ack with this status (0: normal)
	ackFailures int // answer this many acks with 503 first

	csrs      []string
	checks    []CheckRequest
	ackBodies []CheckRequest
	acks      int
	signed    []string // the signed path of each signed request
	jti       map[string]bool

	ticketRole string // the role of testTicket ("" is server)
	ticketUsed bool
	redeemed   []RedeemRequest
	redeemRaw  []map[string]any
	redeem     func(req RedeemRequest, pub any) (RedeemResponse, int)
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

// issue signs a server client certificate for pub with the newest CA. The
// subject is the api's: O the cluster, OU the role, CN the machine.
func (a *fakeAPI) issue(pub any) (string, string) {
	c := a.issuePending(pub)
	return c.pem, c.serial
}

func (a *fakeAPI) issuePending(pub any) *pendingCert {
	ca := a.ca()
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	// The api's notAfter has milliseconds; the certificate has seconds.
	notAfter := time.Now().Add(30*24*time.Hour + 123*time.Millisecond)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{testCluster}, OrganizationalUnit: []string{RoleServer}, CommonName: testMachine},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		a.t.Fatal(err)
	}
	return &pendingCert{
		pem:      string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		serial:   serial.Text(16),
		caGen:    ca.gen,
		notAfter: notAfter.UTC(),
	}
}

func (a *fakeAPI) pendingKey(pub any) string {
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	return fmt.Sprintf("%x/%d", sha256.Sum256(pubDER), a.ca().gen)
}

// problem writes an error as the api does: the h3 envelope with
// {code, message, help} under data.
func (a *fakeAPI) problem(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"statusCode":    status,
		"statusMessage": msg,
		"message":       msg,
		"data":          map[string]any{"code": code, "message": msg, "help": "test help"},
	})
}

// strictJSON decodes like the api's .strict() schemas: an unknown field is
// an error. An empty body is {}.
func strictJSON(body []byte, v any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
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
	if len(cert.Subject.Organization) != 1 || cert.Subject.Organization[0] != testCluster {
		return nil, errors.New("subject names another cluster")
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	pub := cert.PublicKey.(*ecdsa.PublicKey)
	if len(sig) != 64 || !ecdsa.Verify(pub, digest[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return nil, errors.New("bad signature")
	}
	// The api compares the signed path with its request path: the raw
	// path with /api, without the query.
	reqPath, _, _ := strings.Cut(r.RequestURI, "?")
	sum := sha256.Sum256(body)
	switch {
	case p.Method != r.Method:
		return nil, errors.New("method mismatch")
	case p.Path != reqPath:
		return nil, fmt.Errorf("path mismatch: signed %q, request %q", p.Path, reqPath)
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
	a.signed = append(a.signed, p.Path)
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
		a.problem(w, http.StatusUnauthorized, "STATE_SIGNATURE_INVALID", "signature: "+err.Error())
		return
	}
	if a.revoked {
		a.problem(w, http.StatusForbidden, "STATE_MACHINE_REVOKED", "the machine is revoked")
		return
	}
	serial := cert.SerialNumber.Text(16)
	switch r.URL.Path {
	case pathCheck:
		var req CheckRequest
		if err := strictJSON(body, &req); err != nil {
			a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "body: "+err.Error())
			return
		}
		if serial != a.active {
			a.problem(w, http.StatusForbidden, "STATE_MACHINE_UNKNOWN", "serial is not active")
			return
		}
		a.checks = append(a.checks, req)
		_ = json.NewEncoder(w).Encode(CheckResponse{Due: a.due, Paused: a.paused, Reason: "not-due"})
	case pathRenew:
		var req RenewRequest
		if err := strictJSON(body, &req); err != nil {
			a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "body: "+err.Error())
			return
		}
		if serial != a.active {
			a.problem(w, http.StatusForbidden, "STATE_MACHINE_UNKNOWN", "serial is not active")
			return
		}
		a.csrs = append(a.csrs, req.CSR)
		block, _ := pem.Decode([]byte(req.CSR))
		if block == nil {
			a.problem(w, http.StatusBadRequest, "STATE_CSR_INVALID", "no CSR")
			return
		}
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil {
			a.problem(w, http.StatusBadRequest, "STATE_CSR_INVALID", "bad CSR")
			return
		}
		k := a.pendingKey(csr.PublicKey)
		pc := a.pending[k]
		if pc == nil {
			pc = a.issuePending(csr.PublicKey)
			a.pending = map[string]*pendingCert{k: pc} // one pending certificate per machine
		}
		if a.dropRenew > 0 {
			a.dropRenew--
			a.problem(w, http.StatusBadGateway, "SEED_UNAVAILABLE", "lost")
			return
		}
		_ = json.NewEncoder(w).Encode(RenewResponse{Certificate: pc.pem, CAGeneration: pc.caGen, Serial: pc.serial, NotAfter: pc.notAfter})
	case pathAck:
		a.acks++
		var req CheckRequest
		if err := strictJSON(body, &req); err != nil {
			a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "body: "+err.Error())
			return
		}
		a.ackBodies = append(a.ackBodies, req)
		if a.ackStatus != 0 {
			a.problem(w, a.ackStatus, "TEST_ACK_REFUSED", "ack refused")
			return
		}
		if a.ackFailures > 0 {
			a.ackFailures--
			a.problem(w, http.StatusServiceUnavailable, "STATE_RENEW_UNAVAILABLE", "limiter unavailable")
			return
		}
		// A second ack of the active serial answers the same result.
		if serial == a.active {
			_ = json.NewEncoder(w).Encode(AckResponse{Serial: serial})
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
		a.problem(w, http.StatusForbidden, "STATE_MACHINE_UNKNOWN", "not the pending serial of this machine")
	default:
		a.problem(w, http.StatusNotFound, "NOT_FOUND", "no route")
	}
}

func (a *fakeAPI) serveRedeem(w http.ResponseWriter, r *http.Request, body []byte) {
	if r.Header.Get(SignatureHeader) != "" {
		a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "redeem is not signed")
		return
	}
	var req RedeemRequest
	if err := strictJSON(body, &req); err != nil {
		a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "body: "+err.Error())
		return
	}
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	a.redeemed = append(a.redeemed, req)
	a.redeemRaw = append(a.redeemRaw, raw)
	if req.Ticket != testTicket {
		a.problem(w, http.StatusUnauthorized, "STATE_TICKET_INVALID", "the ticket is not valid")
		return
	}
	if a.ticketUsed {
		// A second redemption revokes a machine that did not acknowledge
		// its first certificate.
		if a.active == "" {
			a.revoked = true
		}
		a.problem(w, http.StatusConflict, "STATE_TICKET_USED", "the ticket is used")
		return
	}
	role := a.ticketRole
	if role == "" {
		role = RoleServer
	}
	_, hasCSR := raw["csr"]
	switch {
	case req.Role != role:
		a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "the role is not the role of the ticket")
		return
	case role == RoleAgent && hasCSR:
		a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "an agent machine sends no CSR")
		return
	case role == RoleServer && !hasCSR:
		a.problem(w, http.StatusBadRequest, "BAD_REQUEST", "a server machine needs a CSR")
		return
	}
	var pub any
	if role == RoleServer {
		block, _ := pem.Decode([]byte(req.CSR))
		if block == nil {
			a.problem(w, http.StatusBadRequest, "STATE_CSR_INVALID", "no CSR")
			return
		}
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil {
			a.problem(w, http.StatusBadRequest, "STATE_CSR_INVALID", "bad CSR")
			return
		}
		pub = csr.PublicKey
	}
	resp, status := a.redeem(req, pub)
	if status != 0 {
		a.problem(w, status, "TEST_REFUSED", "ticket refused")
		return
	}
	a.ticketUsed = true
	_ = json.NewEncoder(w).Encode(wireRedeem(resp))
}

// wireRedeem encodes a redeem answer as the api does: null for the absent
// certificate fields, serving CA, endpoint and server URL, and the install
// configuration fields that the install does not read.
func wireRedeem(resp RedeemResponse) map[string]any {
	b, _ := json.Marshal(resp)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"certificate", "serial", "servingCA", "endpoint"} {
		if m[k] == "" {
			m[k] = nil
		}
	}
	if resp.NotAfter.IsZero() {
		m["notAfter"] = nil
	}
	if resp.CAGeneration == 0 {
		m["caGeneration"] = nil
	}
	ic, _ := m["installConfig"].(map[string]any)
	if _, ok := ic["serverUrl"]; !ok {
		ic["serverUrl"] = nil
	}
	ic["ciliumVersion"] = "1.20.1"
	ic["serverFlags"] = []string{"--flannel-backend=none"}
	return m
}

// fakeHost is a k3s server: it records the generation that each restart
// loads, and "connects" when connect allows that generation.
type fakeHost struct {
	store    Store
	restarts []int
	waits    int
	connect  func(gen int) error
	plans    bool
	plansErr error
	started  bool // k3s runs already (the install started it)
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
	h.waits++
	gen := 0
	switch {
	case len(h.restarts) > 0:
		gen = h.restarts[len(h.restarts)-1]
	case h.started:
		cur, err := h.store.Current()
		if err != nil {
			return err
		}
		gen = cur
	default:
		return errors.New("k3s never started")
	}
	if h.connect == nil {
		return nil
	}
	return h.connect(gen)
}

func (h *fakeHost) UpgradeInProgress(context.Context) (bool, error) { return h.plans, h.plansErr }

// renewEnv is one machine with an installed, acknowledged generation 1.
type renewEnv struct {
	t     *testing.T
	api   *fakeAPI
	srv   *httptest.Server
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
	return newEnvOn(t, api, srv, store)
}

// newEnvOn is a renewEnv for an existing state folder.
func newEnvOn(t *testing.T, api *fakeAPI, srv *httptest.Server, store Store) *renewEnv {
	t.Helper()
	client, err := NewClient(srv.URL, testCluster, "test", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	e := &renewEnv{t: t, api: api, srv: srv, store: store, host: &fakeHost{store: store}, now: time.Now().UTC().Truncate(time.Second)}
	e.r = &Renewer{
		Store: store, API: client, Host: e.host,
		Now:          func() time.Time { return e.now },
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		AckRetryWait: time.Millisecond,
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
