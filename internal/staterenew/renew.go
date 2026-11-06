// Package staterenew is the HOST side of managed state (design "self-hosted
// with managed state", sections 4.1 and 5.1): it redeems the install
// ticket (kubehz-agent state-redeem) and renews the datastore client
// certificate (kubehz-agent state-renew, from a systemd timer every five
// minutes).
//
// Renewal, step by step:
//
//  1. renew/check, signed with the current key. Not due, or paused while
//     an upgrade runs: stop.
//  2. A new generation folder gen-<n>: a new key (0600) and a CSR, synced
//     to disk before use.
//  3. renew with the CSR, signed with the CURRENT key. The answer is a
//     pending certificate. If the answer is lost, the next run sends the
//     SAME CSR again.
//  4. Switch the symlink "current" to gen-<n> with one rename(2), and
//     restart k3s.
//  5. Wait up to five minutes until k3s reads from the datastore.
//  6. Connected: renew/ack signed with the NEW key. Not connected: switch
//     "current" back, restart k3s and report.
//
// A crash at any step leaves "current" on one complete generation, and the
// next run continues from the state in gen-<n>/meta.json.
//
// The first certificate of an install is pending at the api too, until the
// machine acknowledges it (contract 10.1). The install writes it as gen-1
// in state "switched", with no previous generation, and starts k3s with
// it. Then "kubehz-agent state-ack" (Acknowledge) waits for the datastore
// and sends renew/ack. A run of the timer finishes it as well (step 6).
//
// A role agent machine has no certificate: it never runs the renewer.
package staterenew

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// Host is the machine side of the renewer.
type Host interface {
	// K3sVersion returns the local k3s version, for example v1.37.2+k3s1.
	K3sVersion(ctx context.Context) (string, error)
	// RestartK3s restarts the k3s service.
	RestartK3s(ctx context.Context) error
	// WaitDatastore returns nil as soon as k3s reads from the datastore, or
	// the last error when timeout passes first.
	WaitDatastore(ctx context.Context, timeout time.Duration) error
	// UpgradeInProgress reports whether the agent's upgrade Plans exist
	// (the local pause signal of design 4.3 step 9).
	UpgradeInProgress(ctx context.Context) (bool, error)
}

// API is the platform side of the renewer.
type API interface {
	Check(ctx context.Context, s Signer, req CheckRequest) (CheckResponse, error)
	Renew(ctx context.Context, s Signer, req RenewRequest) (RenewResponse, error)
	Ack(ctx context.Context, s Signer, req CheckRequest) (AckResponse, error)
}

// Defaults of the renewer.
const (
	// DefaultConnectTimeout is how long k3s gets to connect with a new pair
	// (design 5.1).
	DefaultConnectTimeout = 5 * time.Minute
	// DefaultPendingMaxAge: the api drops a pending certificate after 24
	// hours. One hour less, so the renewer never acknowledges a dropped one.
	DefaultPendingMaxAge = 23 * time.Hour
	// DefaultRetryAfterSwitchBack is the wait after a switch back before the
	// next attempt. Each attempt restarts k3s two times, so the renewer does
	// not try again at each run of the timer.
	DefaultRetryAfterSwitchBack = 30 * time.Minute
	// DefaultAckRetryWait is the wait between two attempts of Acknowledge.
	DefaultAckRetryWait = 10 * time.Second
)

// Renewer runs one renewal pass.
type Renewer struct {
	Store          Store
	API            API
	Host           Host
	Now            func() time.Time
	Log            *slog.Logger
	ConnectTimeout time.Duration
	PendingMaxAge  time.Duration
	RetryAfter     time.Duration
	AckRetryWait   time.Duration
	// CommonName is the CSR subject (the api replaces it).
	CommonName string

	// crash is a test hook: a non-nil error stops the pass at that point,
	// with the files as they are (a simulated crash).
	crash func(point string) error
}

func (r *Renewer) defaults() {
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
	if r.ConnectTimeout <= 0 {
		r.ConnectTimeout = DefaultConnectTimeout
	}
	if r.PendingMaxAge <= 0 {
		r.PendingMaxAge = DefaultPendingMaxAge
	}
	if r.RetryAfter <= 0 {
		r.RetryAfter = DefaultRetryAfterSwitchBack
	}
	if r.AckRetryWait <= 0 {
		r.AckRetryWait = DefaultAckRetryWait
	}
	if r.CommonName == "" {
		r.CommonName = "kubehz-state-client"
	}
}

func (r *Renewer) hook(point string) error {
	if r.crash == nil {
		return nil
	}
	return r.crash(point)
}

// Run does one renewal pass.
func (r *Renewer) Run(ctx context.Context) error {
	r.defaults()
	cur, err := r.Store.Current()
	if err != nil {
		return fmt.Errorf("find the current certificate in %s: %w; run the install first", r.Store.Dir, err)
	}
	curMeta, err := r.Store.ReadMeta(cur)
	if err != nil {
		// The install writes meta.json; a missing one means an older
		// layout. The generation in use is the active one.
		curMeta = Meta{Generation: cur, State: StateAcked}
	}

	switch curMeta.State {
	case StateSwitched:
		// A switch did not finish: a crash, or an acknowledgement that did
		// not reach the api.
		r.Log.Info("continuing a certificate switch that did not finish", "generation", cur)
		return r.finish(ctx, cur, curMeta)
	case StateFailed:
		// A crash between "mark failed" and the switch back.
		if curMeta.Previous > 0 {
			return r.switchBack(ctx, cur, curMeta, curMeta.Error)
		}
	}
	var retryIn time.Duration
	if n, m, ok := r.newestFailed(cur); ok && !m.RestartedAt.IsZero() {
		if m.FailedAt.IsZero() {
			// A crash after the switch back, before the restart: k3s still
			// runs with the files of the failed generation.
			r.Log.Info("finishing a switch back that did not finish", "generation", n)
			return r.completeSwitchBack(ctx, n, m)
		}
		retryIn = m.FailedAt.Add(r.RetryAfter).Sub(r.Now())
	}

	pendingN, pending := r.findPending(cur)

	signer, err := r.signer(cur)
	if err != nil {
		return err
	}
	if na := signer.Cert.NotAfter; !r.Now().Before(na) {
		// The api refuses a signature of an expired certificate, and k3s
		// cannot connect with it: only a recovery ticket helps.
		return fmt.Errorf("the client certificate of %s expired at %s; make a recovery ticket in the dashboard and run the install script with --recovery", genName(cur), na.UTC().Format(time.RFC3339))
	}
	version := r.k3sVersion(ctx)
	chk, err := r.API.Check(ctx, signer, CheckRequest{K3sVersion: version})
	if err != nil {
		if last := r.lastError(); last != "" {
			return fmt.Errorf("renewal check failed: %w (the last renewal failed: %s)", err, last)
		}
		return fmt.Errorf("renewal check failed: %w", err)
	}
	if chk.Paused {
		r.Log.Info("renewal paused by the platform (an upgrade operation runs)", "reason", chk.Reason)
		return nil
	}
	if retryIn > 0 && (chk.Due || pending != nil) {
		r.Log.Info("renewal waits after a switch back", "retryIn", retryIn.Round(time.Second).String(), "lastError", r.lastError())
		return nil
	}
	if !chk.Due && pending == nil {
		r.Log.Debug("renewal not due", "reason", chk.Reason)
		return nil
	}
	busy, err := r.Host.UpgradeInProgress(ctx)
	switch {
	case err != nil:
		// The apiserver may be cut from the datastore (a revocation fence):
		// then the renewal must go on. The platform's pause is the primary
		// signal.
		r.Log.Warn("cannot read the upgrade Plans; continuing with the platform's answer", "error", err.Error())
	case busy:
		r.Log.Info("renewal waits: the agent's upgrade Plans exist (an upgrade runs on this cluster)")
		return nil
	}

	if pending != nil && r.Now().Sub(pending.CreatedAt) > r.PendingMaxAge {
		pending.State = StateFailed
		pending.FailedAt = r.Now().UTC()
		pending.Error = fmt.Sprintf("pending for more than %s; the platform dropped the pending certificate", r.PendingMaxAge)
		if err := r.Store.WriteMeta(pendingN, *pending); err != nil {
			return err
		}
		r.Log.Warn("dropping a pending certificate generation", "generation", pendingN, "reason", pending.Error)
		pending = nil
	}
	if pending == nil {
		r.removeFailed(cur)
		n, m, err := r.newGeneration()
		if err != nil {
			return err
		}
		pendingN, pending = n, &m
		if err := r.hook("keyed"); err != nil {
			return err
		}
	}
	r.Log.Info("renewing the datastore client certificate", "reason", chk.Reason, "generation", pendingN, "state", pending.State)

	if pending.State == StateKeyed {
		if err := r.renew(ctx, signer, cur, pendingN, pending, version); err != nil {
			return err
		}
		if err := r.hook("issued"); err != nil {
			return err
		}
	}

	pending.Previous = cur
	pending.State = StateSwitched
	pending.RestartedAt = time.Time{}
	if err := r.Store.WriteMeta(pendingN, *pending); err != nil {
		return err
	}
	if err := r.hook("meta-switched"); err != nil {
		return err
	}
	if err := r.Store.Switch(pendingN); err != nil {
		return err
	}
	r.Log.Info("switched the datastore certificate", "from", genName(cur), "to", genName(pendingN))
	if err := r.hook("switched"); err != nil {
		return err
	}
	return r.finish(ctx, pendingN, *pending)
}

// findPending returns the newest generation after cur that is keyed,
// issued or switched (switched but not current: a crash before the
// symlink switch). A generation folder without meta.json is an incomplete
// key step and is removed.
func (r *Renewer) findPending(cur int) (int, *Meta) {
	gens, err := r.Store.Gens()
	if err != nil {
		return 0, nil
	}
	for i := len(gens) - 1; i >= 0; i-- {
		n := gens[i]
		if n <= cur {
			break
		}
		m, err := r.Store.ReadMeta(n)
		if err != nil {
			r.Log.Warn("removing an incomplete certificate generation", "generation", n, "error", err.Error())
			_ = r.Store.Remove(n)
			continue
		}
		switch m.State {
		case StateKeyed, StateIssued, StateSwitched:
			return n, &m
		}
	}
	return 0, nil
}

func (r *Renewer) newGeneration() (int, Meta, error) {
	n, err := r.Store.NewGen()
	if err != nil {
		return 0, Meta{}, err
	}
	key, keyPEM, err := newKey()
	if err != nil {
		return 0, Meta{}, err
	}
	if err := WriteFileAtomic(r.Store.Path(n, KeyFile), keyPEM, 0o600); err != nil {
		return 0, Meta{}, err
	}
	csr, err := newCSR(key, r.CommonName)
	if err != nil {
		return 0, Meta{}, err
	}
	if err := WriteFileAtomic(r.Store.Path(n, CSRFile), csr, 0o600); err != nil {
		return 0, Meta{}, err
	}
	m := Meta{State: StateKeyed, CreatedAt: r.Now().UTC()}
	if err := r.Store.WriteMeta(n, m); err != nil {
		return 0, Meta{}, err
	}
	return n, m, nil
}

// renew sends the generation's CSR (the same bytes on every retry) and
// writes the pending certificate.
func (r *Renewer) renew(ctx context.Context, signer Signer, cur, n int, m *Meta, version string) error {
	csr, err := os.ReadFile(r.Store.Path(n, CSRFile))
	if err != nil {
		return fmt.Errorf("read the certificate signing request of %s: %w", genName(n), err)
	}
	keyPEM, err := os.ReadFile(r.Store.Path(n, KeyFile))
	if err != nil {
		return fmt.Errorf("read the key of %s: %w", genName(n), err)
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return err
	}
	resp, err := r.API.Renew(ctx, signer, RenewRequest{CSR: string(csr), K3sVersion: version})
	if err != nil {
		if isPermanent(err) {
			r.abandon(n, m, "the platform refused the renewal: "+err.Error())
		}
		return fmt.Errorf("renewal request failed: %w", err)
	}
	cert, err := parseCert([]byte(resp.Certificate))
	if err == nil {
		err = checkClientCert(cert, key, r.Now())
	}
	if err == nil {
		err = checkIssued(cert, resp.Serial, resp.NotAfter, resp.CAGeneration)
	}
	if err != nil {
		r.abandon(n, m, "the platform answered an unusable certificate: "+err.Error())
		return fmt.Errorf("renewal answer refused: %w", err)
	}
	ca, err := os.ReadFile(r.Store.Path(cur, CAFile))
	if err != nil {
		return fmt.Errorf("read the serving CA of %s: %w", genName(cur), err)
	}
	if err := WriteFileAtomic(r.Store.Path(n, CertFile), []byte(resp.Certificate), 0o644); err != nil {
		return err
	}
	if err := WriteFileAtomic(r.Store.Path(n, CAFile), ca, 0o644); err != nil {
		return err
	}
	m.State = StateIssued
	m.CAGeneration = resp.CAGeneration
	m.Serial = serialHex(cert)
	m.NotAfter = cert.NotAfter.UTC()
	return r.Store.WriteMeta(n, *m)
}

func (r *Renewer) abandon(n int, m *Meta, why string) {
	m.State = StateFailed
	m.FailedAt = r.Now().UTC()
	m.Error = why
	if err := r.Store.WriteMeta(n, *m); err != nil {
		r.Log.Warn("cannot mark a certificate generation failed", "generation", n, "error", err.Error())
	}
	r.setLastError(why)
}

// finish restarts k3s on the switched generation (unless that already
// happened), waits for the datastore, and acknowledges or switches back.
// The first generation of an install (no previous one) is never restarted
// here: the install script starts k3s with it.
func (r *Renewer) finish(ctx context.Context, n int, m Meta) error {
	if m.RestartedAt.IsZero() && m.Previous > 0 {
		if err := r.Host.RestartK3s(ctx); err != nil {
			return fmt.Errorf("restart k3s after the certificate switch: %w; the next run tries again", err)
		}
		m.RestartedAt = r.Now().UTC()
		if err := r.Store.WriteMeta(n, m); err != nil {
			return err
		}
		if err := r.hook("restarted"); err != nil {
			return err
		}
	}
	if err := r.Host.WaitDatastore(ctx, r.ConnectTimeout); err != nil {
		return r.switchBack(ctx, n, m, fmt.Sprintf("k3s did not connect to the datastore with %s within %s: %v", genName(n), r.ConnectTimeout, err))
	}

	signer, err := r.signer(n)
	if err != nil {
		return err
	}
	resp, err := r.API.Ack(ctx, signer, CheckRequest{K3sVersion: r.k3sVersion(ctx)})
	if err != nil {
		if !isPermanent(err) {
			return fmt.Errorf("acknowledgement failed; the next run sends it again: %w", err)
		}
		if m.Previous == 0 {
			// The first certificate of an install. The api acknowledges the
			// same serial again, so a refusal is final: the machine was
			// revoked (for example by a second redemption of its ticket), or
			// the pending certificate expired. Keep the files: the
			// uninstall removes them.
			why := "the platform refused the acknowledgement of the first certificate: " + err.Error()
			r.setLastError(why)
			return fmt.Errorf("%w; remove this install (sudo kubehz-k3s-uninstall.sh), then install again with a new ticket", &ackRefusedError{msg: why, err: err})
		}
		// A refused acknowledgement can be a repeat: the first one reached
		// the api, but the answer was lost. Then the new serial is already
		// the active one, and a check signed with the new key succeeds.
		// Never switch back to a serial that the api replaced.
		if _, cerr := r.API.Check(ctx, signer, CheckRequest{K3sVersion: r.k3sVersion(ctx)}); cerr != nil {
			return r.switchBack(ctx, n, m, "the platform refused the acknowledgement: "+err.Error())
		}
		r.Log.Info("the new certificate is already active (an earlier acknowledgement reached the platform)", "generation", n)
	}
	if err := r.hook("acked"); err != nil {
		return err
	}
	if resp.Serial != "" {
		m.Serial = resp.Serial
	}
	m.State = StateAcked
	m.Error = ""
	if err := r.Store.WriteMeta(n, m); err != nil {
		return err
	}
	r.setLastError("")
	r.prune(n, m.Previous)
	r.Log.Info("datastore client certificate renewed", "generation", n, "serial", m.Serial)
	return nil
}

// ackRefusedError is a final refusal of a first certificate.
type ackRefusedError struct {
	msg string
	err error
}

func (e *ackRefusedError) Error() string { return e.msg }
func (e *ackRefusedError) Unwrap() error { return e.err }

// Acknowledge finishes an install: the first certificate (gen-1, no
// previous generation) is pending at the api until this machine
// acknowledges it (contract 10.1). The install script runs it after k3s
// started with that certificate. It waits for the datastore and sends
// renew/ack, signed with the certificate. It tries again after a transient
// failure until ctx ends, and stops at a final refusal. A certificate that
// is acknowledged already is a success; a second ack is not sent.
//
// It never redeems: a second redemption of the same ticket would revoke
// this machine.
func (r *Renewer) Acknowledge(ctx context.Context) error {
	r.defaults()
	cur, err := r.Store.Current()
	if err != nil {
		return fmt.Errorf("find the current certificate in %s: %w; run the install first", r.Store.Dir, err)
	}
	for {
		m, err := r.Store.ReadMeta(cur)
		if err != nil {
			return err
		}
		switch {
		case m.State == StateAcked:
			r.Log.Info("the datastore client certificate is acknowledged", "generation", cur, "serial", m.Serial)
			return nil
		case m.State != StateSwitched || m.Previous != 0:
			return fmt.Errorf("%s is not the first certificate of an install (state %q, previous %d); the renewal timer finishes it", genName(cur), m.State, m.Previous)
		}
		err = r.finish(ctx, cur, m)
		if err == nil {
			return nil
		}
		var refused *ackRefusedError
		if errors.As(err, &refused) {
			return err
		}
		r.Log.Warn("the acknowledgement did not finish; trying again", "error", err.Error(), "wait", r.AckRetryWait.String())
		t := time.NewTimer(r.AckRetryWait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%w; run the install script again: it continues with the same certificate", err)
		case <-t.C:
		}
	}
}

// switchBack returns to the previous generation. It does not when the CA
// changed (a revocation): the old certificate cannot connect either, so the
// new one stays and the next run waits again.
func (r *Renewer) switchBack(ctx context.Context, n int, m Meta, why string) error {
	prev := m.Previous
	if prev <= 0 {
		r.setLastError(why)
		return fmt.Errorf("%s; there is no previous certificate to switch back to", why)
	}
	if r.caChanged(prev, n, m) {
		r.setLastError(why)
		return fmt.Errorf("%s; the client CA changed, so the previous certificate cannot connect either; the next run waits again", why)
	}
	// Mark the generation failed BEFORE the switch: after a crash between
	// the two steps, the next run sees a failed current and switches back.
	// FailedAt stays zero until k3s restarted on the previous generation.
	m.State = StateFailed
	m.Error = why
	m.FailedAt = time.Time{}
	if m.RestartedAt.IsZero() {
		// The restart marks that k3s used these files; a switch back
		// always ends with a restart.
		m.RestartedAt = r.Now().UTC()
	}
	r.setLastError(why)
	if err := r.Store.WriteMeta(n, m); err != nil {
		return err
	}
	if err := r.hook("marked-failed"); err != nil {
		return err
	}
	if err := r.Store.Switch(prev); err != nil {
		return err
	}
	if err := r.hook("switched-back"); err != nil {
		return err
	}
	if err := r.completeSwitchBack(ctx, n, m); err != nil {
		return err
	}
	r.Log.Error("the new datastore certificate did not work; switched back", "generation", n, "previous", prev, "reason", why)
	return fmt.Errorf("%s; switched back to %s", why, genName(prev))
}

// completeSwitchBack restarts k3s on the previous generation and records
// the time of the failure, which starts the wait before the next attempt.
func (r *Renewer) completeSwitchBack(ctx context.Context, n int, m Meta) error {
	if err := r.Host.RestartK3s(ctx); err != nil {
		return fmt.Errorf("%s; switched back to %s, but the restart of k3s failed: %w; the next run tries again", m.Error, genName(m.Previous), err)
	}
	m.FailedAt = r.Now().UTC()
	return r.Store.WriteMeta(n, m)
}

// newestFailed returns the newest failed generation after cur.
func (r *Renewer) newestFailed(cur int) (int, Meta, bool) {
	gens, err := r.Store.Gens()
	if err != nil {
		return 0, Meta{}, false
	}
	for i := len(gens) - 1; i >= 0 && gens[i] > cur; i-- {
		if m, err := r.Store.ReadMeta(gens[i]); err == nil && m.State == StateFailed {
			return gens[i], m, true
		}
	}
	return 0, Meta{}, false
}

// removeFailed removes the failed generations after cur. They are of no
// further use once the wait after the failure has passed.
func (r *Renewer) removeFailed(cur int) {
	gens, err := r.Store.Gens()
	if err != nil {
		return
	}
	for _, g := range gens {
		if g <= cur {
			continue
		}
		if m, err := r.Store.ReadMeta(g); err == nil && m.State == StateFailed {
			if err := r.Store.Remove(g); err != nil {
				r.Log.Warn("cannot remove a failed certificate generation", "generation", g, "error", err.Error())
			}
		}
	}
}

// caChanged reports whether generation n was issued by another client CA
// than prev (a CA rotation after a revocation).
func (r *Renewer) caChanged(prev, n int, m Meta) bool {
	pm, err := r.Store.ReadMeta(prev)
	if err == nil && pm.CAGeneration > 0 && m.CAGeneration > 0 {
		return pm.CAGeneration != m.CAGeneration
	}
	oldCert, err1 := r.readCert(prev)
	newCert, err2 := r.readCert(n)
	if err1 != nil || err2 != nil || len(oldCert.AuthorityKeyId) == 0 || len(newCert.AuthorityKeyId) == 0 {
		return false
	}
	return string(oldCert.AuthorityKeyId) != string(newCert.AuthorityKeyId)
}

func (r *Renewer) readCert(n int) (*x509.Certificate, error) {
	b, err := os.ReadFile(r.Store.Path(n, CertFile))
	if err != nil {
		return nil, err
	}
	return parseCert(b)
}

// prune removes every generation except keep and prev.
func (r *Renewer) prune(keep, prev int) {
	gens, err := r.Store.Gens()
	if err != nil {
		return
	}
	for _, g := range gens {
		if g == keep || g == prev {
			continue
		}
		if err := r.Store.Remove(g); err != nil {
			r.Log.Warn("cannot remove an old certificate generation", "generation", g, "error", err.Error())
		}
	}
}

func (r *Renewer) signer(n int) (Signer, error) {
	keyPEM, err := os.ReadFile(r.Store.Path(n, KeyFile))
	if err != nil {
		return Signer{}, fmt.Errorf("read the client key of %s: %w", genName(n), err)
	}
	key, err := parseKey(keyPEM)
	if err != nil {
		return Signer{}, err
	}
	cert, err := r.readCert(n)
	if err != nil {
		return Signer{}, fmt.Errorf("read the client certificate of %s: %w", genName(n), err)
	}
	return Signer{Key: key, Cert: cert}, nil
}

func (r *Renewer) k3sVersion(ctx context.Context) string {
	v, err := r.Host.K3sVersion(ctx)
	if err != nil {
		r.Log.Warn("cannot read the local k3s version", "error", err.Error())
		return ""
	}
	return v
}

func (r *Renewer) lastError() string {
	b, err := os.ReadFile(r.Store.Dir + "/" + lastErrorFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func (r *Renewer) setLastError(msg string) {
	path := r.Store.Dir + "/" + lastErrorFile
	if msg == "" {
		_ = os.Remove(path)
		return
	}
	if rs := []rune(msg); len(rs) > 500 {
		msg = string(rs[:500])
	}
	if err := WriteFileAtomic(path, []byte(msg+"\n"), 0o600); err != nil {
		r.Log.Warn("cannot record the last renewal error", "error", err.Error())
	}
}

func serialHex(c *x509.Certificate) string {
	if c == nil || c.SerialNumber == nil {
		return ""
	}
	return c.SerialNumber.Text(16)
}
