// Package leader gates the ACTING loops on a Kubernetes Lease.
//
// The live view is harmless to duplicate — two replicas reporting the same
// cluster is latest-wins. Acting is not: two agents reading one desired
// document would both patch the same MachineDeployment and both delete
// machines, and every guardrail that counts (the per-pool cooldown, the
// in-flight budget, the one-pool-at-a-time roll) is per-PROCESS in-memory
// state that a second process does not see. Two replicas would therefore
// double every bound.
//
// So the agent does not act unless it can PROVE it is the only actor, and the
// proof is a Lease in the agent's own namespace: exactly one holder at a time,
// arbitrated by the apiserver. No Lease — no RBAC, no coordination API, an
// unreachable apiserver, or simply a peer holding it — means no acting. The
// agent keeps reporting, the same fail-toward-report-only posture every other
// guard has, and says so in its log (see AcquireWarnAfter).
//
// The callback's context is cancelled the moment the lease is lost, so the
// acting loops stop with it; the successor starts with a FRESH cooldown
// baseline, which is the conservative direction.
package leader

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// LeaseName is the Lease the acting loops contend for. It lives in the
// agent's own namespace (kubehz-system) and is created on first acquisition.
const LeaseName = "kubehz-live-agent"

// Default timings. Deliberately slower than the controller-manager's 15/10/2:
// this agent polls once a minute, so a 30 s lease costs the apiserver one
// renewal every 5 s instead of every 2 and still hands over well inside one
// poll interval.
const (
	DefaultLeaseDuration = 30 * time.Second
	DefaultRenewDeadline = 20 * time.Second
	DefaultRetryPeriod   = 5 * time.Second
	// DefaultAcquireWarnAfter is how long the agent waits for the lease before
	// it says, in its OWN log, that it is reporting but not acting. Contending
	// forever is silent by design — a standby replica must not shout — and a
	// missing RBAC grant looks exactly the same from here, so the one warning
	// names both causes.
	DefaultAcquireWarnAfter = 2 * time.Minute
)

// Config configures Run. Client, Namespace and Identity are required; the
// rest default.
type Config struct {
	Client    kubernetes.Interface
	Namespace string
	// Name is the Lease name (defaults to LeaseName).
	Name string
	// Identity must be unique per process — see Identity.
	Identity      string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	// AcquireWarnAfter bounds the silence: without the lease by then, the agent
	// logs that it is reporting and not acting (default DefaultAcquireWarnAfter).
	AcquireWarnAfter time.Duration
	Logger           *slog.Logger
}

// Identity builds this process's lease identity: the pod name (the Deployment
// injects it through the downward API) plus a per-PROCESS UUID. The UUID
// matters — two pods can share a name across a restart, and a stale holder
// entry must never look like this process still holding the lease.
func Identity(podName string) string {
	if podName == "" {
		podName, _ = os.Hostname()
	}
	if podName == "" {
		podName = "kubehz-agent"
	}
	return podName + "_" + string(uuid.NewUUID())
}

// Run contends for the Lease until ctx is done, running onLeading while this
// process holds it. onLeading's context is cancelled when the lease is lost,
// and Run then contends again — a lost lease is a pause, not an exit. It
// returns ctx.Err() when the caller cancels, or a configuration error.
func Run(ctx context.Context, cfg Config, onLeading func(context.Context)) error {
	if cfg.Client == nil {
		return fmt.Errorf("leader election: no Kubernetes client")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("leader election: no namespace")
	}
	if cfg.Identity == "" {
		return fmt.Errorf("leader election: no identity")
	}
	if cfg.Name == "" {
		cfg.Name = LeaseName
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = DefaultLeaseDuration
	}
	if cfg.RenewDeadline <= 0 {
		cfg.RenewDeadline = DefaultRenewDeadline
	}
	if cfg.RetryPeriod <= 0 {
		cfg.RetryPeriod = DefaultRetryPeriod
	}
	if cfg.AcquireWarnAfter <= 0 {
		cfg.AcquireWarnAfter = DefaultAcquireWarnAfter
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: cfg.Name, Namespace: cfg.Namespace},
		Client:    cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
			// No EventRecorder on purpose: the agent writes no Events, so it
			// needs no events RBAC.
		},
	}

	for {
		// One warning per attempt if the lease stays out of reach. The library
		// retries a held lease, a denied one and an unreachable apiserver the
		// same way, and none of them reaches this code as an error, so the
		// warning names all three causes and the agent keeps contending.
		acquired := make(chan struct{})
		var once sync.Once
		go func() {
			select {
			case <-acquired:
			case <-ctx.Done():
			case <-time.After(cfg.AcquireWarnAfter):
				log.Warn("no acting lease after "+cfg.AcquireWarnAfter.String()+
					"; the agent is REPORTING but NOT acting — another replica holds it, the Lease RBAC (deploy/managed) is missing, or the apiserver is unreachable",
					"lease", cfg.Namespace+"/"+cfg.Name, "identity", cfg.Identity)
			}
		}()

		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: cfg.LeaseDuration,
			RenewDeadline: cfg.RenewDeadline,
			RetryPeriod:   cfg.RetryPeriod,
			// Release on cancel so a rolling update hands over in seconds
			// instead of waiting out the lease.
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(lctx context.Context) {
					once.Do(func() { close(acquired) })
					log.Info("acting leader acquired; desired-state loop starting",
						"lease", cfg.Namespace+"/"+cfg.Name, "identity", cfg.Identity)
					onLeading(lctx)
				},
				OnStoppedLeading: func() {
					log.Warn("acting leader lost; desired-state loop stopped (reporting continues)",
						"lease", cfg.Namespace+"/"+cfg.Name, "identity", cfg.Identity)
				},
			},
		})
		if err != nil {
			once.Do(func() { close(acquired) })
			return fmt.Errorf("leader election: %w", err)
		}

		elector.Run(ctx)
		once.Do(func() { close(acquired) })
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Lease lost (or never acquired, and Run returned). Stand by and
		// contend again — never act while another identity may hold it.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cfg.RetryPeriod):
		}
	}
}
