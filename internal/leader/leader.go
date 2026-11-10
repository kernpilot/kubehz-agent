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
// The handover order is the safety property, so Run owns it instead of the
// election library:
//
//  1. Acting stops first. The acting callback's context is cancelled the
//     moment a renewal fails, or the moment the agent shuts down.
//  2. Run then WAITS for the callback to return. It does not contend again,
//     and it does not return, while acting can still run.
//  3. Only then can the lease pass on. After a failed renewal the lease
//     expires on its own; on a clean shutdown Run keeps renewing until the
//     callback has returned, and then releases the lease itself (release).
//
// A holder can also STEP DOWN (Config.MayLead): when a condition outside the
// Lease says this process must not hold it, Run ends the term in the same
// order as a shutdown and then stays out of the race until the condition
// clears. The agent uses this for its reports, see Config.MayLead.
//
// The library's own ReleaseOnCancel is off on purpose: client-go releases
// the lease BEFORE it cancels the leading callback's context, so a slow
// release would keep this replica acting while a peer takes over.
//
// The successor starts with a FRESH cooldown baseline, which is the
// conservative direction.
package leader

import (
	"bytes"
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
//
// The three numbers also set the DRAIN MARGIN. After its last successful
// renewal the holder stops acting at most RetryPeriod+RenewDeadline later
// (25 s). A peer takes the lease no earlier than LeaseDuration after it saw
// that renewal (30 s). The difference (5 s) is the time the acting callback
// has to return after its context is cancelled. Run refuses timings that
// leave no margin.
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
	// Observer, when set, records every read of the Lease, so the reporting
	// side can tell when ANOTHER replica holds it (see Observer).
	Observer *Observer
	// MayLead, when set, is a condition outside the Lease that this process
	// must meet to hold it. A non-nil error means it must not. Run then does
	// not contend, and a holder STEPS DOWN: acting stops, Run waits for the
	// callback to return, and then it releases the lease (the shutdown
	// order). Run asks every RetryPeriod, logs the error, and contends again
	// once MayLead returns nil. While it stays out of the race it still reads
	// the Lease every RetryPeriod, so the Observer keeps seeing the holder.
	MayLead func() error
	Logger  *slog.Logger
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
// process holds it. onLeading's context is cancelled when the lease is lost
// or ctx ends, and Run waits for onLeading to RETURN before it contends again
// or returns itself — a lost lease is a pause, not an exit. It returns
// ctx.Err() when the caller cancels, or a configuration error.
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
	// No drain margin = a window in which a peer may act while this replica
	// still does (see the Default timings).
	if cfg.LeaseDuration <= cfg.RenewDeadline+cfg.RetryPeriod {
		return fmt.Errorf("leader election: lease duration %s must exceed renew deadline %s plus retry period %s",
			cfg.LeaseDuration, cfg.RenewDeadline, cfg.RetryPeriod)
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	var lock resourcelock.Interface = &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: cfg.Name, Namespace: cfg.Namespace},
		Client:    cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
			// No EventRecorder on purpose: the agent writes no Events, so it
			// needs no events RBAC.
		},
	}
	if cfg.Observer != nil {
		lock = cfg.Observer.wrap(lock, cfg.Identity)
	}

	for {
		if !awaitMayLead(ctx, cfg, lock, log) {
			return ctx.Err()
		}
		if err := term(ctx, cfg, lock, onLeading, log); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Lease lost (or never acquired, and the elector returned). Stand by
		// and contend again — never act while another identity may hold it.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cfg.RetryPeriod):
		}
	}
}

// awaitMayLead returns true once cfg.MayLead lets this process contend, and
// false when ctx ends first. While it waits, the elector does not run, so it
// reads the Lease itself every RetryPeriod: the Observer must go on seeing
// who holds it.
func awaitMayLead(ctx context.Context, cfg Config, lock resourcelock.Interface, log *slog.Logger) bool {
	if cfg.MayLead == nil {
		return true
	}
	err := cfg.MayLead()
	if err == nil {
		return true
	}
	lease := cfg.Namespace + "/" + cfg.Name
	log.Warn("not contending for the acting lease: "+err.Error(), "lease", lease, "identity", cfg.Identity)
	tick := time.NewTicker(cfg.RetryPeriod)
	defer tick.Stop()
	for {
		if cfg.Observer != nil {
			rctx, cancel := context.WithTimeout(ctx, cfg.RenewDeadline)
			_, _, _ = lock.Get(rctx) // the Observer records the result
			cancel()
		}
		if cfg.MayLead() == nil {
			log.Info("contending for the acting lease again", "lease", lease, "identity", cfg.Identity)
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
		}
	}
}

// term runs ONE election: contend, act while the lease holds, and drain. It
// returns only after the acting callback has returned (or can no longer
// start), so the caller never contends again while acting may still run.
func term(ctx context.Context, cfg Config, lock resourcelock.Interface, onLeading func(context.Context), log *slog.Logger) error {
	lease := cfg.Namespace + "/" + cfg.Name

	// termCtx ends the term: the caller's cancel (a shutdown) or a step-down
	// (MayLead). Both take the same path below.
	termCtx, stopTerm := context.WithCancel(ctx)
	defer stopTerm()

	// The elector renews on its OWN context, detached from the term: until
	// the acting callback has returned the lease must stay ours, or a peer
	// could take it while a write is still in flight.
	electCtx, stopElector := context.WithCancel(context.WithoutCancel(ctx))
	defer stopElector()

	cb := &callback{drained: make(chan struct{})}
	acquired := make(chan struct{})
	var acquiredOnce sync.Once
	electorDone := make(chan struct{})

	// One warning per attempt if the lease stays out of reach. The library
	// retries a held lease, a denied one and an unreachable apiserver the
	// same way, and none of them reaches this code as an error, so the
	// warning names all three causes and the agent keeps contending.
	go func() {
		select {
		case <-acquired:
		case <-electorDone:
		case <-termCtx.Done():
		case <-time.After(cfg.AcquireWarnAfter):
			log.Warn("no acting lease after "+cfg.AcquireWarnAfter.String()+
				"; the agent is REPORTING but NOT acting — another replica holds it, the Lease RBAC (deploy/managed) is missing, or the apiserver is unreachable",
				"lease", lease, "identity", cfg.Identity)
		}
	}()

	// Shutdown and step-down order: stop acting, wait for the callback to
	// return, and only THEN stop renewing. Never the other way around.
	go func() {
		select {
		case <-electorDone:
			return
		case <-termCtx.Done():
		}
		cb.drain()
		stopElector()
	}()

	// Step-down: the term ends once MayLead says this process must not hold
	// the lease, whether it holds it already or still contends.
	if cfg.MayLead != nil {
		go func() {
			tick := time.NewTicker(cfg.RetryPeriod)
			defer tick.Stop()
			for {
				select {
				case <-electorDone:
					return
				case <-termCtx.Done():
					return
				case <-tick.C:
				}
				if err := cfg.MayLead(); err != nil {
					log.Warn("stepping down from the acting lease: "+err.Error(), "lease", lease, "identity", cfg.Identity)
					stopTerm()
					return
				}
			}
		}()
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: cfg.LeaseDuration,
		RenewDeadline: cfg.RenewDeadline,
		RetryPeriod:   cfg.RetryPeriod,
		// Off: see the package comment. Run releases by itself, after the
		// callback has returned, and only on a clean shutdown.
		ReleaseOnCancel: false,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(lctx context.Context) {
				if !cb.start(termCtx, lctx) {
					return
				}
				defer cb.finish()
				acquiredOnce.Do(func() { close(acquired) })
				// Acting stops on whichever comes first: the lease lost (the
				// library cancels lctx), or the term ending (a shutdown or a
				// step-down).
				actCtx, cancel := context.WithCancel(lctx)
				defer cancel()
				stop := context.AfterFunc(termCtx, cancel)
				defer stop()
				log.Info("acting leader acquired; desired-state loop starting",
					"lease", lease, "identity", cfg.Identity)
				onLeading(actCtx)
			},
			// Required by the library. The outcome is logged after the drain,
			// where it is known whether this replica acted at all.
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		close(electorDone)
		return fmt.Errorf("leader election: %w", err)
	}

	elector.Run(electCtx)
	close(electorDone)
	// The library cancelled the leading context before Run returned. JOIN the
	// callback: acting has stopped before anything else happens.
	led := cb.drain()

	if termCtx.Err() != nil {
		switch {
		case led && ctx.Err() != nil:
			log.Info("agent stopping; acting stopped and drained", "lease", lease, "identity", cfg.Identity)
		case led:
			log.Info("stepped down; acting stopped and drained", "lease", lease, "identity", cfg.Identity)
		}
		release(cfg, log)
		return nil
	}
	if led {
		log.Warn("acting lease lost; acting stopped and drained (reporting continues)",
			"lease", lease, "identity", cfg.Identity)
	}
	return nil
}

// callback tracks one term's acting callback, so Run can wait for it, and a
// late start (the library launches the callback in its own goroutine) can
// never begin acting after the term ended.
type callback struct {
	mu      sync.Mutex
	sealed  bool
	running bool
	drained chan struct{}
}

// start reports whether the callback may run: not once the term is sealed,
// and not when the lease or the agent is already gone.
func (c *callback) start(ctx, lctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed || ctx.Err() != nil || lctx.Err() != nil {
		c.sealed = true
		return false
	}
	c.running = true
	return true
}

// finish marks the callback as returned.
func (c *callback) finish() { close(c.drained) }

// drain seals the term against a late start and, when the callback ran,
// waits until it has returned. It reports whether the callback ran.
func (c *callback) drain() bool {
	c.mu.Lock()
	c.sealed = true
	running := c.running
	c.mu.Unlock()
	if running {
		<-c.drained
	}
	return running
}

// release hands the lease over after a clean shutdown or a step-down, so a
// successor acts within one retry period instead of waiting out the lease.
// It runs only after the acting callback has returned. It is a
// compare-and-swap: it writes only while the stored Lease still names THIS
// process, and the update carries the resourceVersion it just read. A Lease
// that a successor already holds is never touched. (client-go's own release
// compares against its cached view, not the stored object.)
func release(cfg Config, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RenewDeadline)
	defer cancel()
	leases := cfg.Client.CoordinationV1().Leases(cfg.Namespace)
	l, err := leases.Get(ctx, cfg.Name, metav1.GetOptions{})
	if err != nil {
		log.Debug("acting lease not released (cannot read it)", "error", err.Error())
		return
	}
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != cfg.Identity {
		return
	}
	// The shape client-go writes on release: no holder, a one-second term.
	now := metav1.NewMicroTime(time.Now())
	noHolder, oneSecond := "", int32(1)
	l.Spec.HolderIdentity = &noHolder
	l.Spec.LeaseDurationSeconds = &oneSecond
	l.Spec.AcquireTime = &now
	l.Spec.RenewTime = &now
	if _, err := leases.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		log.Warn("could not release the acting lease; a successor waits for it to expire",
			"lease", cfg.Namespace+"/"+cfg.Name, "error", err.Error())
		return
	}
	log.Info("acting lease released", "lease", cfg.Namespace+"/"+cfg.Name, "identity", cfg.Identity)
}

// Observer records what this process last read of the acting Lease, so the
// REPORTING side can tell whether another replica holds it.
//
// It exists for the action reports. The platform keeps ONE snapshot of a
// cluster's actions, latest-wins, and reads a heartbeat without actions as
// "clear". A standby replica has no actions (it does not act), so its beats
// would erase the holder's outcomes and refusals. With an Observer the
// standby stays silent while a peer holds the lease, and the holder reports
// the same live view.
//
// "Holds" uses the rule the election itself uses: the record names another
// identity, and the last successful read found it CHANGED (a renewal) less
// than its own lease duration before, by this process's clock. A record that
// stops changing (the holder died) stops counting once a read finds it
// unchanged for one lease duration, which is the read at which a standby
// takes the lease. A read that FAILS proves nothing about the holder: the
// peer keeps counting, however long this process cannot read the Lease.
type Observer struct {
	now func() time.Time

	mu       sync.Mutex
	identity string
	raw      []byte
	holder   string
	duration time.Duration
	seenAt   time.Time // local time the record last changed
	readAt   time.Time // local time of the last successful read

	firstRead     chan struct{}
	firstReadOnce sync.Once
}

// NewObserver builds an Observer for Config.Observer.
func NewObserver() *Observer {
	return &Observer{now: time.Now, firstRead: make(chan struct{})}
}

// PeerHolds reports whether another process holds a still-valid acting lease.
// A nil Observer never sees a peer.
func (o *Observer) PeerHolds() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.holder == "" || o.holder == o.identity || o.seenAt.IsZero() {
		return false
	}
	// Expiry needs evidence: a successful read of the unchanged record one
	// lease duration after it last changed.
	return o.readAt.Sub(o.seenAt) < o.duration
}

// WaitFirstRead blocks until the election has read the Lease once (with any
// result), ctx ends, or maxWait passes, so the first heartbeat does not race
// the first observation. A nil Observer returns at once.
func (o *Observer) WaitFirstRead(ctx context.Context, maxWait time.Duration) {
	if o == nil {
		return
	}
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case <-o.firstRead:
	case <-ctx.Done():
	case <-t.C:
	}
}

// wrap returns lock with every read and every successful write recorded.
func (o *Observer) wrap(lock resourcelock.Interface, identity string) resourcelock.Interface {
	o.mu.Lock()
	o.identity = identity
	o.mu.Unlock()
	return &observedLock{Interface: lock, obs: o}
}

func (o *Observer) read(rec *resourcelock.LeaderElectionRecord, raw []byte, err error) {
	defer o.firstReadOnce.Do(func() { close(o.firstRead) })
	if err != nil || rec == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.now()
	if !bytes.Equal(raw, o.raw) {
		o.raw = append(o.raw[:0], raw...)
		o.seenAt = now
	}
	o.readAt = now
	o.holder = rec.HolderIdentity
	o.duration = time.Duration(rec.LeaseDurationSeconds) * time.Second
}

// wrote records this process's own successful create or update: from then on
// the holder is the identity this process just wrote (itself, as a rule).
func (o *Observer) wrote(rec resourcelock.LeaderElectionRecord) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.raw = nil // the next read sets a new baseline
	o.holder = rec.HolderIdentity
	o.duration = time.Duration(rec.LeaseDurationSeconds) * time.Second
	o.seenAt = o.now()
	o.readAt = o.seenAt
}

// observedLock feeds an Observer from the election's own lock calls.
type observedLock struct {
	resourcelock.Interface
	obs *Observer
}

func (l *observedLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	rec, raw, err := l.Interface.Get(ctx)
	l.obs.read(rec, raw, err)
	return rec, raw, err
}

func (l *observedLock) Create(ctx context.Context, rec resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, rec)
	if err == nil {
		l.obs.wrote(rec)
	}
	return err
}

func (l *observedLock) Update(ctx context.Context, rec resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, rec)
	if err == nil {
		l.obs.wrote(rec)
	}
	return err
}
