package staterenew

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var bg = context.Background()

func TestRenewNotDue(t *testing.T) {
	e := newRenewEnv(t)
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if len(e.api.checks) != 1 || len(e.api.csrs) != 0 || len(e.host.restarts) != 0 {
		t.Fatalf("checks %d, renewals %d, restarts %v", len(e.api.checks), len(e.api.csrs), e.host.restarts)
	}
	if e.api.checks[0].K3sVersion != "v1.34.2+k3s1" {
		t.Fatalf("check body %+v", e.api.checks[0])
	}
}

func TestRenewPaused(t *testing.T) {
	t.Run("by the platform", func(t *testing.T) {
		e := newRenewEnv(t)
		e.api.due, e.api.paused = true, true
		if err := e.r.Run(bg); err != nil {
			t.Fatal(err)
		}
		if len(e.api.csrs) != 0 || len(e.host.restarts) != 0 || len(e.gens()) != 1 {
			t.Fatal("renewed while the platform paused renewals")
		}
	})
	t.Run("by the local upgrade Plans", func(t *testing.T) {
		e := newRenewEnv(t)
		e.api.due, e.host.plans = true, true
		if err := e.r.Run(bg); err != nil {
			t.Fatal(err)
		}
		if len(e.api.csrs) != 0 || len(e.host.restarts) != 0 {
			t.Fatal("renewed while the upgrade Plans exist")
		}
	})
	t.Run("unreadable Plans do not block", func(t *testing.T) {
		e := newRenewEnv(t)
		e.api.due, e.host.plansErr = true, errors.New("apiserver cut from the datastore")
		if err := e.r.Run(bg); err != nil {
			t.Fatal(err)
		}
		if e.current() != 2 {
			t.Fatal("a fenced apiserver blocked the renewal")
		}
	})
}

func TestRenewHappyPath(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due = true
	oldSerial := e.api.active
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if e.current() != 2 {
		t.Fatalf("current = %d, want 2", e.current())
	}
	m := e.meta(2)
	if m.State != StateAcked || m.Previous != 1 || m.CAGeneration != 1 {
		t.Fatalf("meta %+v", m)
	}
	if e.api.active != e.serialOf(2) || e.api.active == oldSerial {
		t.Fatal("the api did not switch to the new serial")
	}
	if !slices.Equal(e.host.restarts, []int{2}) {
		t.Fatalf("restarts %v, want one on gen-2", e.host.restarts)
	}
	if mode := fileMode(t, e.store.Path(2, KeyFile)); mode != 0o600 {
		t.Fatalf("new key mode %o", mode)
	}
	if mode := fileMode(t, e.store.genDir(2)); mode != 0o700 {
		t.Fatalf("new generation folder mode %o", mode)
	}
	if readFile(t, e.store.genDir(2), CAFile) != "serving CA\n" {
		t.Fatal("serving CA not carried to the new generation")
	}
	e.assertComplete()

	// The next renewal removes generations older than the previous one.
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if got := e.gens(); !slices.Equal(got, []int{2, 3}) {
		t.Fatalf("generations %v, want [2 3]", got)
	}
}

func TestRenewLostResponseReusesTheCSR(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due, e.api.dropRenew = true, 1
	if err := e.r.Run(bg); err == nil {
		t.Fatal("a lost answer is not an error")
	}
	if e.current() != 1 || e.meta(2).State != StateKeyed {
		t.Fatalf("current %d, gen-2 %+v", e.current(), e.meta(2))
	}
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if len(e.api.csrs) != 2 || e.api.csrs[0] != e.api.csrs[1] {
		t.Fatal("the retry did not send the same CSR")
	}
	if e.current() != 2 || e.api.active != e.serialOf(2) {
		t.Fatal("renewal did not finish after the retry")
	}
}

func TestRenewSwitchBack(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due = true
	oldSerial := e.api.active
	e.host.connect = func(gen int) error {
		if gen == 2 {
			return errors.New("x509: certificate signed by unknown authority")
		}
		return nil
	}
	err := e.r.Run(bg)
	if err == nil || !strings.Contains(err.Error(), "switched back to gen-1") {
		t.Fatalf("err = %v", err)
	}
	if e.current() != 1 {
		t.Fatalf("current = %d, want 1 after the switch back", e.current())
	}
	if !slices.Equal(e.host.restarts, []int{2, 1}) {
		t.Fatalf("restarts %v, want [2 1]", e.host.restarts)
	}
	if m := e.meta(2); m.State != StateFailed || m.FailedAt.IsZero() {
		t.Fatalf("gen-2 meta %+v", m)
	}
	if e.api.active != oldSerial || e.api.acks != 0 {
		t.Fatal("the new certificate was acknowledged")
	}
	e.assertComplete()

	// The next run waits. The failure stays on the host: the api's check
	// body has no field for it (the strict fake refuses unknown fields).
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(readFile(t, e.store.Dir, lastErrorFile), "did not connect") {
		t.Fatal("the last error is not kept on the host")
	}
	if len(e.host.restarts) != 2 {
		t.Fatal("tried again before the wait passed")
	}

	// After the wait it tries again with a new key.
	e.host.connect = nil
	e.now = e.now.Add(DefaultRetryAfterSwitchBack + time.Minute)
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if e.api.active != e.serialOf(e.current()) || e.current() == 1 {
		t.Fatal("the retry did not renew")
	}
	if _, err := os.Stat(e.store.Path(1, metaFile)); err != nil {
		t.Fatal("the previous generation was removed")
	}
	if _, err := os.Stat(filepath.Join(e.store.Dir, lastErrorFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("last error kept after a successful renewal")
	}
}

func TestRenewNoSwitchBackAfterCAChange(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due = true
	e.api.rotateCA() // a revocation fence: kine trusts only the new CA
	e.host.connect = func(gen int) error {
		if gen == 1 {
			return errors.New("old CA")
		}
		return errors.New("kine not restarted yet")
	}
	err := e.r.Run(bg)
	if err == nil || !strings.Contains(err.Error(), "client CA changed") {
		t.Fatalf("err = %v", err)
	}
	if e.current() != 2 || e.meta(2).State != StateSwitched {
		t.Fatal("switched back to a certificate of the old CA")
	}
	// The fence completes: the next run connects, without another restart.
	e.host.connect = nil
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if e.meta(2).State != StateAcked || !slices.Equal(e.host.restarts, []int{2}) {
		t.Fatalf("gen-2 %+v, restarts %v", e.meta(2), e.host.restarts)
	}
}

func TestRenewAckRefused(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due, e.api.ackStatus = true, http.StatusForbidden
	if err := e.r.Run(bg); err == nil {
		t.Fatal("a refused acknowledgement is not an error")
	}
	if e.current() != 1 || e.meta(2).State != StateFailed {
		t.Fatal("no switch back after a refused acknowledgement")
	}
}

func TestRenewAckTransientRetries(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due, e.api.ackStatus = true, http.StatusServiceUnavailable
	if err := e.r.Run(bg); err == nil {
		t.Fatal("a failed acknowledgement is not an error")
	}
	if e.current() != 2 || e.meta(2).State != StateSwitched {
		t.Fatal("a transient error switched back")
	}
	e.api.ackStatus = 0
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if e.meta(2).State != StateAcked || len(e.host.restarts) != 1 {
		t.Fatalf("gen-2 %+v, restarts %v", e.meta(2), e.host.restarts)
	}
}

func TestRenewPendingTooOld(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due, e.api.dropRenew = true, 1
	_ = e.r.Run(bg) // gen-2 keyed, answer lost
	e.now = e.now.Add(DefaultPendingMaxAge + time.Hour)
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if len(e.api.csrs) != 2 || e.api.csrs[0] == e.api.csrs[1] {
		t.Fatal("an old pending generation was not dropped for a new key")
	}
	if e.api.active != e.serialOf(e.current()) {
		t.Fatal("renewal did not finish")
	}
}

// A crash at any step leaves "current" on one complete generation, and the
// next run finishes the renewal.
func TestRenewCrashPoints(t *testing.T) {
	for _, point := range []string{"keyed", "issued", "meta-switched", "switched", "restarted", "acked"} {
		t.Run(point, func(t *testing.T) {
			e := newRenewEnv(t)
			e.api.due = true
			crashed := false
			e.r.crash = func(p string) error {
				if p == point && !crashed {
					crashed = true
					return errCrash
				}
				return nil
			}
			if err := e.r.Run(bg); !errors.Is(err, errCrash) {
				t.Fatalf("err = %v, want the crash", err)
			}
			e.assertComplete()
			e.api.due = false // the next run finishes the started renewal, due or not
			for range 2 {
				if err := e.r.Run(bg); err != nil {
					t.Fatalf("after the crash: %v", err)
				}
				e.assertComplete()
			}
			if e.current() != 2 || e.meta(2).State != StateAcked {
				t.Fatalf("current %d, gen-2 %+v", e.current(), e.meta(2))
			}
			if e.api.active != e.serialOf(2) {
				t.Fatal("the api's active serial is not the one in use")
			}
			if last := e.host.restarts[len(e.host.restarts)-1]; last != 2 {
				t.Fatalf("k3s last loaded gen-%d", last)
			}
			if len(e.api.csrs) != 1 && point != "keyed" {
				t.Fatalf("%d renew calls, want 1", len(e.api.csrs))
			}
		})
	}
}

// A crash during a switch back still ends on the previous generation, with
// k3s restarted on it.
func TestRenewCrashDuringSwitchBack(t *testing.T) {
	for _, point := range []string{"marked-failed", "switched-back"} {
		t.Run(point, func(t *testing.T) {
			e := newRenewEnv(t)
			e.api.due = true
			e.host.connect = func(gen int) error {
				if gen == 2 {
					return errors.New("no connection")
				}
				return nil
			}
			crashed := false
			e.r.crash = func(p string) error {
				if p == point && !crashed {
					crashed = true
					return errCrash
				}
				return nil
			}
			if err := e.r.Run(bg); !errors.Is(err, errCrash) {
				t.Fatalf("err = %v, want the crash", err)
			}
			e.assertComplete()
			restarts, csrs := len(e.host.restarts), len(e.api.csrs)
			_ = e.r.Run(bg)
			e.assertComplete()
			if e.current() != 1 {
				t.Fatalf("current = %d, want 1", e.current())
			}
			// The second run only finishes the switch back: one restart on
			// gen-1 and no new certificate request.
			if got := e.host.restarts[restarts:]; len(got) != 1 || got[0] != 1 {
				t.Fatalf("restarts in the second run %v, want [1]", got)
			}
			if len(e.api.csrs) != csrs {
				t.Fatalf("the second run sent %d new CSRs, want 0", len(e.api.csrs)-csrs)
			}
			if m := e.meta(2); m.State != StateFailed || m.FailedAt.IsZero() {
				t.Fatalf("gen-2 %+v", m)
			}
		})
	}
}

func TestRenewRequiresAnInstall(t *testing.T) {
	r := &Renewer{Store: Store{Dir: t.TempDir()}}
	if err := r.Run(bg); err == nil || !strings.Contains(err.Error(), "run the install first") {
		t.Fatalf("err = %v", err)
	}
}

func TestSignatureIsBoundToTheRequest(t *testing.T) {
	e := newRenewEnv(t)
	s, err := e.r.signer(1)
	if err != nil {
		t.Fatal(err)
	}
	a := e.api
	body := []byte(`{"csr":"x"}`)
	jws, err := s.Sign(http.MethodPost, pathRenew, body, testCluster, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	req := func(path string, b []byte, sig string) error {
		r, _ := http.NewRequest(http.MethodPost, "http://x"+path, nil)
		r.RequestURI = path // as the server sees it
		r.Header.Set(SignatureHeader, sig)
		_, err := a.verify(r, b)
		return err
	}
	if err := req(pathRenew, body, jws); err != nil {
		t.Fatalf("valid signature refused: %v", err)
	}
	if err := req(pathRenew, body, jws); err == nil {
		t.Fatal("replayed jti accepted")
	}
	jws2, _ := s.Sign(http.MethodPost, pathRenew, body, testCluster, time.Now())
	if err := req(pathAck, body, jws2); err == nil {
		t.Fatal("signature accepted on another path")
	}
	jws3, _ := s.Sign(http.MethodPost, pathRenew, body, testCluster, time.Now())
	if err := req(pathRenew, []byte(`{"csr":"y"}`), jws3); err == nil {
		t.Fatal("signature accepted for another body")
	}
	// The api compares the path without the query, and with /api.
	jws4, _ := s.Sign(http.MethodPost, pathRenew, body, testCluster, time.Now())
	if err := req(pathRenew+"?x=1", body, jws4); err != nil {
		t.Fatalf("a query changed the signed path: %v", err)
	}
	jws5, _ := s.Sign(http.MethodPost, strings.TrimPrefix(pathRenew, "/api"), body, testCluster, time.Now())
	if err := req(pathRenew, body, jws5); err == nil {
		t.Fatal("a path without /api accepted")
	}
	parts := strings.Split(jws, ".")
	if len(parts) != 3 || strings.ContainsAny(jws, "=+/") {
		t.Fatalf("not a compact base64url JWS: %s", jws)
	}
}

// newRedeemedEnv is a machine right after the install's redemption: gen-1
// is switched and pending at the api, and the install started k3s with it.
func newRedeemedEnv(t *testing.T) *renewEnv {
	t.Helper()
	api, srv := newFakeAPI(t)
	api.redeem = apiRedeem(api)
	d := newRedeemer(t, srv)
	if _, err := d.Run(bg, testTicket); err != nil {
		t.Fatal(err)
	}
	e := newEnvOn(t, api, srv, d.Store)
	e.host.started = true
	return e
}

func count(list []string, s string) int {
	n := 0
	for _, x := range list {
		if x == s {
			n++
		}
	}
	return n
}

// The first certificate is pending until the machine acknowledges it
// (contract 10.1). Acknowledge sends renew/ack signed with it, after k3s
// reads from the datastore, and does not restart k3s.
func TestAcknowledgeFirstCertificate(t *testing.T) {
	e := newRedeemedEnv(t)
	if e.api.active != "" {
		t.Fatal("active before the acknowledgement")
	}
	if err := e.r.Acknowledge(bg); err != nil {
		t.Fatal(err)
	}
	if e.api.active != e.serialOf(1) || count(e.api.signed, pathAck) != 1 {
		t.Fatalf("active %q, signed %v", e.api.active, e.api.signed)
	}
	if m := e.meta(1); m.State != StateAcked || m.Serial != e.serialOf(1) || m.CAGeneration != 1 {
		t.Fatalf("meta %+v", m)
	}
	if len(e.host.restarts) != 0 || e.host.waits != 1 {
		t.Fatalf("restarts %v, waits %d: want no restart and one wait", e.host.restarts, e.host.waits)
	}
	if e.api.ackBodies[0].K3sVersion != "v1.34.2+k3s1" {
		t.Fatalf("ack body %+v", e.api.ackBodies[0])
	}
	// A second run sends nothing.
	if err := e.r.Acknowledge(bg); err != nil || count(e.api.signed, pathAck) != 1 {
		t.Fatalf("second run: %v, signed %v", err, e.api.signed)
	}
	// The machine is active: the timer's check works with the certificate.
	if err := e.r.Run(bg); err != nil || len(e.api.checks) != 1 {
		t.Fatalf("check after the acknowledgement: %v, checks %d", err, len(e.api.checks))
	}
}

func TestAcknowledgeWaitsForTheDatastore(t *testing.T) {
	e := newRedeemedEnv(t)
	fails := 2
	e.host.connect = func(int) error {
		if fails > 0 {
			fails--
			return errors.New("kine not reachable")
		}
		return nil
	}
	if err := e.r.Acknowledge(bg); err != nil {
		t.Fatal(err)
	}
	if e.host.waits != 3 || count(e.api.signed, pathAck) != 1 || e.api.active != e.serialOf(1) {
		t.Fatalf("waits %d, signed %v: want the ack only after the datastore answered", e.host.waits, e.api.signed)
	}
	if len(e.host.restarts) != 0 || e.current() != 1 {
		t.Fatal("a failed wait restarted k3s or switched")
	}
}

func TestAcknowledgeRetriesATransientFailure(t *testing.T) {
	e := newRedeemedEnv(t)
	e.api.ackFailures = 2
	if err := e.r.Acknowledge(bg); err != nil {
		t.Fatal(err)
	}
	if e.api.acks != 3 || e.api.active != e.serialOf(1) || e.meta(1).State != StateAcked {
		t.Fatalf("acks %d, active %q, meta %+v", e.api.acks, e.api.active, e.meta(1))
	}
}

func TestAcknowledgeStopsAtTheTimeout(t *testing.T) {
	e := newRedeemedEnv(t)
	e.api.ackStatus = http.StatusServiceUnavailable
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	err := e.r.Acknowledge(ctx)
	if err == nil || !strings.Contains(err.Error(), "run the install script again") {
		t.Fatalf("err = %v", err)
	}
	if e.meta(1).State != StateSwitched || e.current() != 1 || len(e.gens()) != 1 {
		t.Fatal("a transient failure changed the state folder")
	}
}

// A second redemption of the same ticket revokes a machine that did not
// acknowledge its first certificate (contract 10.1). The refusal is final:
// Acknowledge stops at once and says what to do.
func TestAcknowledgeAfterTheTicketWasReused(t *testing.T) {
	e := newRedeemedEnv(t)
	other := newRedeemer(t, e.srv)
	if _, err := other.Run(bg, testTicket); err == nil || !strings.Contains(err.Error(), "STATE_TICKET_USED") {
		t.Fatalf("second redemption: %v", err)
	}
	if !e.api.revoked {
		t.Fatal("the fake did not revoke the unacknowledged machine")
	}
	err := e.r.Acknowledge(bg)
	if err == nil || !strings.Contains(err.Error(), "STATE_MACHINE_REVOKED") || !strings.Contains(err.Error(), "new ticket") {
		t.Fatalf("err = %v", err)
	}
	if n := count(e.api.signed, pathAck); n != 1 {
		t.Fatalf("%d acks: a final refusal is not tried again", n)
	}
	if e.meta(1).State != StateSwitched {
		t.Fatal("the generation changed state")
	}
	if !strings.Contains(readFile(t, e.store.Dir, lastErrorFile), "refused the acknowledgement of the first certificate") {
		t.Fatal("the refusal is not kept on the host")
	}
}

// The renewal timer also finishes a first certificate that the install did
// not acknowledge, with no restart of k3s and no check before the ack.
func TestTimerFinishesTheFirstCertificate(t *testing.T) {
	e := newRedeemedEnv(t)
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if e.api.active != e.serialOf(1) || e.meta(1).State != StateAcked {
		t.Fatalf("active %q, meta %+v", e.api.active, e.meta(1))
	}
	if len(e.host.restarts) != 0 {
		t.Fatalf("restarts %v: the timer restarted k3s for the first certificate", e.host.restarts)
	}
	if len(e.api.checks) != 0 || e.api.signed[0] != pathAck {
		t.Fatalf("signed %v: want the ack first", e.api.signed)
	}
}

func TestAcknowledgeRefusesARenewal(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due = true
	e.r.crash = func(p string) error {
		if p == "switched" {
			return errCrash
		}
		return nil
	}
	if err := e.r.Run(bg); !errors.Is(err, errCrash) {
		t.Fatalf("err = %v", err)
	}
	e.r.crash = nil
	err := e.r.Acknowledge(bg)
	if err == nil || !strings.Contains(err.Error(), "not the first certificate") {
		t.Fatalf("err = %v", err)
	}
	if count(e.api.signed, pathAck) != 0 || len(e.host.restarts) != 0 {
		t.Fatal("Acknowledge acted on a renewal")
	}
}

func TestRenewRefusesAnExpiredCertificate(t *testing.T) {
	e := newRenewEnv(t)
	e.now = e.now.Add(31 * 24 * time.Hour)
	err := e.r.Run(bg)
	if err == nil || !strings.Contains(err.Error(), "recovery ticket") {
		t.Fatalf("err = %v", err)
	}
	if len(e.api.signed) != 0 {
		t.Fatal("signed a request with an expired certificate")
	}
}

// The signed path is the request path: with /api, without a query
// (contract 10.3). An api URL with a path would break that, so the client
// refuses one.
func TestSignedPathIsTheRequestPath(t *testing.T) {
	e := newRenewEnv(t)
	e.api.due = true
	if err := e.r.Run(bg); err != nil {
		t.Fatal(err)
	}
	if want := []string{pathCheck, pathRenew, pathAck}; !slices.Equal(e.api.signed, want) {
		t.Fatalf("signed paths %v, want %v", e.api.signed, want)
	}
	for _, p := range e.api.signed {
		if !strings.HasPrefix(p, "/api/state/") || strings.Contains(p, "?") {
			t.Fatalf("signed path %q", p)
		}
	}
	for _, bad := range []string{
		"https://api.example.com/prefix", "https://api.example.com/api", "https://api.example.com?x=1",
		"https://api.example.com/?", "https://user@api.example.com", "https://api.example.com#f",
	} {
		if _, err := NewClient(bad, testCluster, "test", nil); err == nil {
			t.Errorf("api URL %s accepted", bad)
		}
	}
	if _, err := NewClient("https://api.example.com/", testCluster, "test", nil); err != nil {
		t.Errorf("api URL with a trailing slash: %v", err)
	}
}

func TestProblemDetail(t *testing.T) {
	code, detail := problemDetail([]byte(`{"statusCode":409,"statusMessage":"x","data":{"code":"STATE_TICKET_USED","message":"The ticket\nis used","help":"h"}}`))
	if code != "STATE_TICKET_USED" || detail != "The ticket is used" {
		t.Fatalf("api envelope: %q, %q", code, detail)
	}
	if code, _ := problemDetail([]byte(`{"data":{"code":"bad code; rm -rf /"}}`)); code != "" {
		t.Fatalf("a code that is not a code: %q", code)
	}
	if code, detail := problemDetail([]byte(`{"title":"t","detail":"d"}`)); code != "" || detail != "d" {
		t.Fatalf("problem response: %q, %q", code, detail)
	}
	if code, detail := problemDetail([]byte(`<html>`)); code != "" || detail != "" {
		t.Fatalf("not JSON: %q, %q", code, detail)
	}
}
