package staterenew

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRedeemMain(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = goodRedeem(a)
	dir := filepath.Join(t.TempDir(), "state")
	var out, errb bytes.Buffer

	// The ticket is never an argument.
	if code := RedeemMain(context.Background(), []string{"--dir", dir, "--api-url", srv.URL, testTicket}, strings.NewReader(""), &out, &errb, "test"); code != ExitUsage {
		t.Fatalf("ticket as argument: exit %d", code)
	}
	errb.Reset()
	if code := RedeemMain(context.Background(), []string{"--dir", dir, "--api-url", srv.URL, "--label", "server-1"}, strings.NewReader(""), &out, &errb, "test"); code != ExitFail {
		t.Fatalf("no ticket: exit %d", code)
	}
	errb.Reset()
	code := RedeemMain(context.Background(), []string{"--dir", dir, "--api-url", srv.URL, "--label", "server-1"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test")
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if !strings.Contains(out.String(), "cluster c1, machine m-1; this install made the cluster's kubehz agent token") {
		t.Fatalf("stdout %q", out.String())
	}
	agentToken := strings.TrimSpace(readFile(t, dir, AgentTokenFile))
	for _, secret := range []string{testTicket, testK3sToken, testK3sAgentToken, agentToken} {
		if strings.Contains(out.String()+errb.String(), secret) {
			t.Fatal("output shows a secret")
		}
	}
}

// --recovery reaches the redemption: no agent token hash.
func TestRedeemMainRecovery(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	a.ticketKind = KindRecovery
	dir := filepath.Join(t.TempDir(), "state")
	var out, errb bytes.Buffer
	code := RedeemMain(context.Background(), []string{"--dir", dir, "--api-url", srv.URL, "--label", "server-1", "--recovery"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test")
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if _, ok := a.redeemRaw[0]["agentTokenHash"]; ok || out.String() != "redeemed: cluster c1, machine m-1\n" {
		t.Fatalf("body %v, stdout %q", a.redeemRaw[0], out.String())
	}
	errb.Reset()
	code = RedeemMain(context.Background(), []string{"--dir", t.TempDir(), "--api-url", srv.URL, "--role", "agent", "--recovery"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test")
	if code != ExitFail || len(a.redeemed) != 1 {
		t.Fatalf("agent with --recovery: exit %d, redemptions %d", code, len(a.redeemed))
	}
}

func TestRedeemMainAgent(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = agentRedeem(a)
	a.ticketRole = RoleAgent
	dir := filepath.Join(t.TempDir(), "state")
	var out, errb bytes.Buffer
	code := RedeemMain(context.Background(), []string{"--dir", dir, "--api-url", srv.URL, "--label", "agent-1", "--role", "agent"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test")
	if code != ExitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if out.String() != "redeemed: agent machine m-2\n" {
		t.Fatalf("stdout %q", out.String())
	}
	if _, ok := a.redeemRaw[0]["csr"]; ok {
		t.Fatal("an agent sent a CSR")
	}
	if strings.Contains(out.String()+errb.String(), testK3sAgentToken) {
		t.Fatal("output shows the k3s token")
	}
}

// AckMain is the install step after k3s started: it waits for the
// datastore, acknowledges the first certificate, and exits 0 only then.
func TestAckMain(t *testing.T) {
	a, srv := newFakeAPI(t)
	a.redeem = apiRedeem(a)
	dir := filepath.Join(t.TempDir(), "state")
	var out, errb bytes.Buffer
	ctx := context.Background()
	if code := RedeemMain(ctx, []string{"--dir", dir, "--api-url", srv.URL, "--label", "server-1"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test"); code != ExitOK {
		t.Fatalf("redeem: exit %d: %s", code, errb.String())
	}
	if code := AckMain(ctx, []string{"--dir", dir, "extra"}, &errb, "test"); code != ExitUsage {
		t.Fatalf("extra argument: exit %d", code)
	}
	var ready atomic.Bool
	kc := fakeK3s(t, &ready)
	noK3s := filepath.Join(t.TempDir(), "k3s")
	args := []string{"--dir", dir, "--kubeconfig", kc, "--k3s", noK3s}

	// The datastore does not answer: no ack, and the timeout ends the run.
	errb.Reset()
	if code := AckMain(ctx, append(args, "--timeout", "300ms"), &errb, "test"); code != ExitFail {
		t.Fatalf("datastore down: exit %d: %s", code, errb.String())
	}
	if a.active != "" || a.acks != 0 {
		t.Fatalf("acknowledged before the datastore answered (acks %d)", a.acks)
	}

	ready.Store(true)
	errb.Reset()
	if code := AckMain(ctx, append(args, "--timeout", "30s"), &errb, "test"); code != ExitOK {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if a.active == "" || a.acks != 1 {
		t.Fatalf("not acknowledged (acks %d)", a.acks)
	}
	// Again: already acknowledged, nothing is sent.
	if code := AckMain(ctx, append(args, "--timeout", "30s"), &errb, "test"); code != ExitOK || a.acks != 1 {
		t.Fatalf("second run: exit %d, acks %d", code, a.acks)
	}
}

func TestRedeemMainRefusesPlainHTTP(t *testing.T) {
	var out, errb bytes.Buffer
	code := RedeemMain(context.Background(), []string{"--dir", t.TempDir(), "--api-url", "http://api.example.com", "--label", "s"}, strings.NewReader(testTicket+"\n"), &out, &errb, "test")
	if code != ExitUsage || !strings.Contains(errb.String(), "use https") {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
}

func TestRenewMain(t *testing.T) {
	e := newRenewEnv(t)
	var errb bytes.Buffer
	// No renew.json yet.
	if code := RenewMain(context.Background(), []string{"--dir", e.store.Dir}, &errb, "test"); code != ExitFail || !strings.Contains(errb.String(), "run the install first") {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if code := RenewMain(context.Background(), []string{"--dir", e.store.Dir, "extra"}, &errb, "test"); code != ExitUsage {
		t.Fatalf("extra argument: exit %d", code)
	}
	// A second run while one holds the lock stops quietly.
	release, err := Lock(e.store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	errb.Reset()
	if code := RenewMain(context.Background(), []string{"--dir", e.store.Dir}, &errb, "test"); code != ExitOK || !strings.Contains(errb.String(), "another state command runs") {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
}
