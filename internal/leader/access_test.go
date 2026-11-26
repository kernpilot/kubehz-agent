package leader

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
)

// These tests run Run with the PRODUCTION timings (a 5 s retry period, a
// 5 minute access re-check) inside a synctest bubble, so an hour of agent
// time passes in no real time and every count below is exact.

// leaseForbidden is the apiserver's answer to a ServiceAccount without the
// Lease Role: the base install.
func leaseForbidden() error {
	return apierrors.NewForbidden(
		schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
		LeaseName, errors.New("no RBAC for leases"))
}

// captureKlog sends klog's output to a buffer for the rest of the test.
// client-go's election library logs every failed attempt there, not to the
// agent's own logger.
func captureKlog(t *testing.T) *syncBuffer {
	t.Helper()
	var buf syncBuffer
	klog.SetOutput(&buf)
	klog.LogToStderr(false)
	t.Cleanup(func() { klog.LogToStderr(true) })
	return &buf
}

// productionConfig is the election as the agent runs it, with a logger the
// test can read.
func productionConfig(client *fake.Clientset, logs *syncBuffer) Config {
	return Config{
		Client:    client,
		Namespace: "kubehz-system",
		Identity:  "replica-a",
		Logger:    slog.New(slog.NewTextHandler(logs, nil)),
	}
}

// leaseVerbs lists the verbs of every Lease request the client saw, in order.
func leaseVerbs(client *fake.Clientset) []string {
	var verbs []string
	for _, a := range client.Actions() {
		if a.GetResource().Resource == "leases" {
			verbs = append(verbs, a.GetVerb())
		}
	}
	return verbs
}

// THE BASE INSTALL. No Lease verb at all: the agent reads the Lease once,
// warns once, and reads again every five minutes. It never starts the
// election, so client-go does not log a refusal every retry period, and it
// never acts. Reporting does not wait for it.
func TestRun_RefusedLeaseReadDoesNotContend(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, leaseForbidden()
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)
		obs := NewObserver()
		cfg.Observer = obs

		ctx, cancel := context.WithCancel(context.Background())
		var acted atomic.Bool
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- Run(ctx, cfg, func(context.Context) { acted.Store(true) }) }()

		// The first heartbeat waits for the first read of the Lease. A refused
		// read is a read: the heartbeat must not wait out the minute.
		obs.WaitFirstRead(ctx, time.Minute)
		if waited := time.Since(start); waited > 0 {
			t.Errorf("the first read took %s; the first heartbeat must not wait for it", waited)
		}

		// One hour and two minutes: the reads at 0, 5, ..., 60 minutes.
		time.Sleep(62 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		if acted.Load() {
			t.Fatal("acted without the Lease")
		}
		verbs := leaseVerbs(client)
		if len(verbs) != 13 {
			t.Errorf("%d Lease requests in 62 minutes, want 13 (one read every 5 minutes): %v", len(verbs), verbs)
		}
		for _, v := range verbs {
			if v != "get" {
				t.Fatalf("Lease requests %v: the agent contended for a Lease it may not read", verbs)
			}
		}
		out := logs.String()
		if n := strings.Count(out, "level=WARN"); n != 1 {
			t.Errorf("%d warnings, want exactly one:\n%s", n, out)
		}
		for _, want := range []string{"NOT acting", "deploy/managed", "5m0s"} {
			if !strings.Contains(out, want) {
				t.Errorf("log = %q, want it to contain %q", out, want)
			}
		}
		if got := klogOut.String(); strings.Contains(got, "lease lock") {
			t.Errorf("client-go logged the refused Lease, so the election ran:\n%s", got)
		}
		if obs.PeerHolds() {
			t.Error("a refused read made the Observer see a peer; the agent would stop reporting")
		}
	})
}

// A grant applied later (the managed overlay) starts acting at the next
// re-check, without a restart.
func TestRun_LaterGrantStartsActingWithoutRestart(t *testing.T) {
	captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		var granted atomic.Bool
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			if granted.Load() {
				return false, nil, nil // the default tracker answers
			}
			return true, nil, leaseForbidden()
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		leading := make(chan time.Time, 1)
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			done <- Run(ctx, cfg, func(lctx context.Context) {
				leading <- time.Now()
				<-lctx.Done()
			})
		}()

		time.Sleep(12 * time.Minute)
		granted.Store(true)

		select {
		case at := <-leading:
			if elapsed := at.Sub(start); elapsed < 15*time.Minute || elapsed >= 15*time.Minute+DefaultRetryPeriod {
				t.Errorf("acting started after %s, want it at the re-check at 15m0s", elapsed)
			}
		case <-time.After(time.Hour):
			t.Error("the grant never started acting")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		out := logs.String()
		if n := strings.Count(out, "level=WARN"); n != 1 {
			t.Errorf("%d warnings, want exactly one:\n%s", n, out)
		}
		if !strings.Contains(out, "readable now") {
			t.Errorf("log = %q, want the agent to say that it contends now", out)
		}
	})
}

// The managed install. The access read is the one request added before the
// election, and the election then runs as before: it reads, creates the
// Lease and acts at once.
func TestRun_ReadableLeaseKeepsTheElection(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		leading := make(chan time.Time, 1)
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			done <- Run(ctx, cfg, func(lctx context.Context) {
				leading <- time.Now()
				<-lctx.Done()
			})
		}()

		select {
		case at := <-leading:
			if elapsed := at.Sub(start); elapsed != 0 {
				t.Errorf("acting started after %s, want at once", elapsed)
			}
		case <-time.After(time.Hour):
			t.Error("a readable Lease never started acting")
		}
		verbs := leaseVerbs(client)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		// The first renewal runs at once, beside the callback, so only the
		// requests up to the create are fixed.
		if want := []string{"get", "get", "create"}; len(verbs) < len(want) || strings.Join(verbs[:len(want)], ",") != strings.Join(want, ",") {
			t.Errorf("Lease requests up to acting = %v, want them to start with %v", verbs, want)
		}
		if out := logs.String(); strings.Contains(out, "level=WARN") {
			t.Errorf("a readable Lease logged a warning:\n%s", out)
		}
		if got := klogOut.String(); strings.Contains(got, "lease lock") {
			t.Errorf("client-go logged a Lease error:\n%s", got)
		}
	})
}

// Any answer other than Forbidden keeps the election as it was: a server
// error is retried every retry period, client-go logs each failure, and the
// two-minute warning names the causes. (This test is also the control for
// the klog capture: the line the base-install test must NOT see shows here.)
func TestRun_OtherLeaseReadErrorsStillContend(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewGenericServerResponse(http.StatusInternalServerError, "get",
				schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, LeaseName, "etcd is down", 0, true)
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		var acted atomic.Bool
		done := make(chan error, 1)
		go func() { done <- Run(ctx, cfg, func(context.Context) { acted.Store(true) }) }()

		time.Sleep(10 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		if acted.Load() {
			t.Fatal("acted without the Lease")
		}
		// The election retries every 5 to 11 s (a 1.2 jitter): at least 54 reads
		// in 10 minutes, where the re-check alone would make 3.
		if n := len(leaseVerbs(client)); n < 54 {
			t.Errorf("%d Lease reads in 10 minutes, want the election's retries (54 or more)", n)
		}
		out := logs.String()
		if strings.Contains(out, "refuses") {
			t.Errorf("a server error was taken for a refused read:\n%s", out)
		}
		if !strings.Contains(out, "no acting lease after 2m0s") {
			t.Errorf("log = %q, want the two-minute warning of the election", out)
		}
		if got := klogOut.String(); !strings.Contains(got, "Error retrieving lease lock") {
			t.Errorf("klog = %q, want client-go's line for the failed read (the capture works)", got)
		}
	})
}
