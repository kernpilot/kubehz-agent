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

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
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
// client-go's election library logs every failed attempt through klog (its
// klog.FromContext finds this logger), not through the agent's own logger.
func captureKlog(t *testing.T) *syncBuffer {
	t.Helper()
	var buf syncBuffer
	klog.SetLoggerWithOptions(textlogger.NewLogger(textlogger.NewConfig(textlogger.Output(&buf))), klog.ContextualLogger(true))
	t.Cleanup(klog.ClearLogger)
	return &buf
}

// productionConfig is the election with the production timings, as the agent
// runs it: a MayLead (the agent's refuses only while its heartbeats fail, so
// this one always lets the process lead) and a logger the test can read.
func productionConfig(client *fake.Clientset, logs *syncBuffer) Config {
	return Config{
		Client:    client,
		Namespace: "kubehz-system",
		Identity:  "replica-a",
		MayLead:   func() error { return nil },
		Logger:    slog.New(slog.NewTextHandler(logs, nil)),
	}
}

// klogLeaseErrors counts client-go's lines for a failed read of the Lease.
func klogLeaseErrors(out *syncBuffer) int {
	return strings.Count(out.String(), "Error retrieving lease lock")
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
		if n := klogLeaseErrors(klogOut); n != 0 {
			t.Errorf("client-go logged %d failed Lease reads, so the election ran:\n%s", n, klogOut.String())
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
		if n := klogLeaseErrors(klogOut); n != 0 {
			t.Errorf("client-go logged %d failed Lease reads:\n%s", n, klogOut.String())
		}
	})
}

// Any answer other than the apiserver's own Forbidden keeps the election as
// it was: it is retried every retry period, client-go logs each failure, and
// the two-minute warning names the causes. A 403 page without a Status (from
// a proxy, say) is not the apiserver's refusal either. (This test is also the
// control for the klog capture: the line the base-install test must NOT see
// shows here.)
func TestRun_OtherLeaseReadErrorsStillContend(t *testing.T) {
	leases := schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}
	for name, readErr := range map[string]error{
		"a server error": apierrors.NewGenericServerResponse(http.StatusInternalServerError, "get", leases, LeaseName, "etcd is down", 0, true),
		"a 403 page":     apierrors.NewGenericServerResponse(http.StatusForbidden, "get", leases, LeaseName, "<html>403</html>", 0, true),
	} {
		t.Run(name, func(t *testing.T) {
			klogOut := captureKlog(t)
			synctest.Test(t, func(t *testing.T) {
				client := fake.NewClientset()
				client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, readErr
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
				// The election retries every 5 to 11 s (a 1.2 jitter): at least
				// 54 reads in 10 minutes, where the re-check alone would make 3.
				if n := len(leaseVerbs(client)); n < 54 {
					t.Errorf("%d Lease reads in 10 minutes, want the election's retries (54 or more)", n)
				}
				out := logs.String()
				if strings.Contains(out, "refuse") {
					t.Errorf("the error was taken for a refused read:\n%s", out)
				}
				if !strings.Contains(out, "no acting lease after 2m0s") {
					t.Errorf("log = %q, want the two-minute warning of the election", out)
				}
				if n := klogLeaseErrors(klogOut); n < 54 {
					t.Errorf("client-go logged %d failed Lease reads, want one per retry (54 or more): the capture does not see them", n)
				}
			})
		})
	}
}

// A refusal that comes after the access read ends the election: here the
// access read fails for another reason (the apiserver still starts), so the
// election runs, and its first read is refused (the base install). client-go
// logs that one read; then the access read takes over again.
func TestRun_RefusalAfterAFailedFirstReadEndsTheElection(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		var reads atomic.Int32
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			if reads.Add(1) == 1 {
				return true, nil, apierrors.NewServiceUnavailable("the apiserver is starting")
			}
			return true, nil, leaseForbidden()
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		var acted atomic.Bool
		done := make(chan error, 1)
		go func() { done <- Run(ctx, cfg, func(context.Context) { acted.Store(true) }) }()

		time.Sleep(62 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		if acted.Load() {
			t.Fatal("acted without the Lease")
		}
		// The failed access read, the refused read of the election, the read
		// of the release after the term, and the access reads at 5 s and then
		// every 5 minutes up to 60m5s (13).
		if verbs := leaseVerbs(client); len(verbs) != 16 {
			t.Errorf("%d Lease requests in 62 minutes, want 16: %v", len(verbs), verbs)
		}
		if n := klogLeaseErrors(klogOut); n != 1 {
			t.Errorf("client-go logged %d failed Lease reads, want only the one refused read of the election:\n%s", n, klogOut.String())
		}
		out := logs.String()
		if n := strings.Count(out, "level=WARN"); n != 1 || !strings.Contains(out, "refuses this agent a read") {
			t.Errorf("want exactly one warning, the refused read:\n%s", out)
		}
	})
}

// A grant removed while this process stands by (a peer holds the Lease) ends
// the election at its next read, so the standby does not repeat the refused
// read every retry period either.
func TestRun_RefusalWhileStandingByEndsTheElection(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		peer, hour, now := "replica-b", int32(3600), metav1.NewMicroTime(time.Now())
		if _, err := client.CoordinationV1().Leases("kubehz-system").Create(context.Background(), &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: LeaseName, Namespace: "kubehz-system"},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &peer, LeaseDurationSeconds: &hour, AcquireTime: &now, RenewTime: &now,
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		var revoked atomic.Bool
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			if revoked.Load() {
				return true, nil, leaseForbidden()
			}
			return false, nil, nil
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		var acted atomic.Bool
		done := make(chan error, 1)
		go func() { done <- Run(ctx, cfg, func(context.Context) { acted.Store(true) }) }()

		time.Sleep(10 * time.Minute)
		before := len(leaseVerbs(client))
		revoked.Store(true)
		time.Sleep(52 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		if acted.Load() {
			t.Fatal("a standby acted while a peer held the Lease")
		}
		// The refused read of the election (within 11 s), the read of the
		// release, and the access reads 5 s later and then every 5 minutes up
		// to 62 minutes (11).
		if after := len(leaseVerbs(client)) - before; after != 13 {
			t.Errorf("%d Lease requests in the 52 minutes after the grant was removed, want 13", after)
		}
		if n := klogLeaseErrors(klogOut); n != 1 {
			t.Errorf("client-go logged %d failed Lease reads, want only the one refused read of the election:\n%s", n, klogOut.String())
		}
		// Two warnings: the standby's own (a peer holds the Lease, after
		// AcquireWarnAfter) and the refused read, each once.
		out := logs.String()
		if strings.Count(out, "level=WARN") != 2 ||
			strings.Count(out, "no acting lease after") != 1 ||
			strings.Count(out, "refuses this agent a read") != 1 {
			t.Errorf("want two warnings, the standby's and the refused read's, each once:\n%s", out)
		}
	})
}

// A grant removed while this process leads stops acting at the next renewal
// (within RetryPeriod), not after the library has tried for RenewDeadline.
// Then the access read takes over, as on the base install.
func TestRun_RefusalWhileLeadingStopsActingAtTheNextRenewal(t *testing.T) {
	klogOut := captureKlog(t)
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewClientset()
		var revoked atomic.Bool
		client.PrependReactor("*", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
			if revoked.Load() {
				return true, nil, leaseForbidden()
			}
			return false, nil, nil
		})
		var logs syncBuffer
		cfg := productionConfig(client, &logs)

		ctx, cancel := context.WithCancel(context.Background())
		leading, stopped := make(chan struct{}), make(chan time.Time, 1)
		done := make(chan error, 1)
		go func() {
			done <- Run(ctx, cfg, func(actCtx context.Context) {
				close(leading)
				<-actCtx.Done()
				stopped <- time.Now()
			})
		}()
		select {
		case <-leading:
		case <-time.After(time.Hour):
			t.Fatal("never acted with the grant in place")
		}

		// Between two renewals, so the next one is at most RetryPeriod away.
		time.Sleep(10*time.Minute + 2*time.Second)
		revokedAt := time.Now()
		before := len(leaseVerbs(client))
		revoked.Store(true)
		select {
		case at := <-stopped:
			if d := at.Sub(revokedAt); d > DefaultRetryPeriod {
				t.Errorf("acting stopped %v after the grant was removed, want at most %v", d, DefaultRetryPeriod)
			}
		case <-time.After(time.Hour):
			t.Fatal("acting did not stop after the grant was removed")
		}
		time.Sleep(52 * time.Minute)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}

		// The renewal (its update, then the refused read), the read of the
		// release, and the access reads 5 s later and then every 5 minutes
		// (11).
		if after := len(leaseVerbs(client)) - before; after != 14 {
			t.Errorf("%d Lease requests in the 52 minutes after the grant was removed, want 14", after)
		}
		if n := klogLeaseErrors(klogOut); n != 1 {
			t.Errorf("client-go logged %d failed Lease reads, want only the one refused read of the renewal:\n%s", n, klogOut.String())
		}
		out := logs.String()
		if strings.Count(out, "level=WARN") != 2 ||
			strings.Count(out, "refused a read of the acting lease; acting stopped and drained") != 1 ||
			strings.Count(out, "refuses this agent a read") != 1 {
			t.Errorf("want two warnings, the stop and the refused read, each once:\n%s", out)
		}
	})
}
