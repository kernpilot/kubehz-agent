// Package agent wires the pieces together: in-cluster informers on
// nodes/pods/events feed a debounced Coalescer, which builds a schema-2 payload
// and hands it to the outbound Sender; alongside it, the desired-state Poller
// pulls the platform's intent and the Executor acts on it locally (P3),
// reporting outcomes through the actions store into the same heartbeat.
// There is NO inbound listener and NO analytics anywhere in this program —
// the only network egress is the authenticated heartbeat POST plus the
// authenticated desired-state GET, both connections OPENED BY the agent
// (the privacy guarantee, enforced by construction: there simply is no other
// outbound code path and no inbound one at all).
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/actions"
	"github.com/kernpilot/kubehz-agent/internal/buildinfo"
	"github.com/kernpilot/kubehz-agent/internal/collector"
	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/desired"
	"github.com/kernpilot/kubehz-agent/internal/executor"
	"github.com/kernpilot/kubehz-agent/internal/inventory"
	"github.com/kernpilot/kubehz-agent/internal/kube"
	"github.com/kernpilot/kubehz-agent/internal/machineissues"
	"github.com/kernpilot/kubehz-agent/internal/publisher"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

const (
	// resyncPeriod 0 = watch-only. Freshness is guaranteed by the Coalescer's
	// periodic full push re-listing the (watch-updated) caches; a resync would
	// only add a periodic UpdateFunc storm for no benefit to a reporter.
	resyncPeriod = 0
	// versionRefresh re-reads the cluster server version (changes only on a k8s
	// upgrade) off the hot path.
	versionRefresh = 5 * time.Minute
	// backoffBase/Max bound the Sender's retry spacing.
	backoffBase = 1 * time.Second
	backoffMax  = 5 * time.Minute
	// helmSyncTimeout bounds how long the OPTIONAL helm-release Secret watch
	// may take to sync before the agent gives up on it. Its RBAC lives in the
	// opt-in deploy/inventory overlay, so on most clusters the initial LIST is
	// Forbidden and the reflector would retry forever — this turns that into
	// one log line and a fallback to pod labels.
	helmSyncTimeout = 20 * time.Second
)

// Agent is the long-running managed-tier live-view + desired-state agent.
type Agent struct {
	cfg    *config.Config
	client kubernetes.Interface
	// dyn drives the P3 scaling executor. May be nil: the desired-state loop
	// is then disabled and the agent is a pure live-view reporter.
	dyn dynamic.Interface
	log *slog.Logger

	mu            sync.RWMutex
	serverVersion string
}

// New builds an Agent. dyn may be nil (disables the desired-state pull loop —
// pure report-only); logger may be nil (uses slog.Default).
func New(cfg *config.Config, client kubernetes.Interface, dyn dynamic.Interface, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{cfg: cfg, client: client, dyn: dyn, log: logger}
}

// Run blocks until ctx is cancelled, driving the informer→debounce→publish loop.
func (a *Agent) Run(ctx context.Context) error {
	// Best-effort initial cluster version (per-node kubelet version is reported
	// regardless, so a failure here is non-fatal — fail toward report-only).
	if v, err := kube.ServerVersion(a.client); err == nil {
		a.setVersion(v)
	} else {
		a.log.Warn("could not read server version at startup", "error", err.Error())
	}
	go a.refreshVersionLoop(ctx)

	factory := informers.NewSharedInformerFactory(a.client, resyncPeriod)
	nodeInf := factory.Core().V1().Nodes()
	podInf := factory.Core().V1().Pods()

	// Events get their own factory so a server-side field selector can restrict
	// the list+watch to type=Warning — the only kind the payload reports. On a
	// busy cluster the full event stream dwarfs everything else; filtering at
	// the apiserver keeps the cache small and stops Normal-event churn from
	// waking the coalescer for pushes that would change nothing.
	eventFactory := informers.NewSharedInformerFactoryWithOptions(a.client, resyncPeriod,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("type", corev1.EventTypeWarning).String()
		}))
	eventInf := eventFactory.Core().V1().Events()

	changes := make(chan struct{}, 1)
	handler := changeHandler(changes)
	for _, inf := range []cache.SharedIndexInformer{
		nodeInf.Informer(), podInf.Informer(), eventInf.Informer(),
	} {
		// managedFields are pure apply-tracking bookkeeping — often kilobytes
		// per object — and nothing in the payload reads them. Dropping them
		// before objects enter the cache bounds memory on pod-dense clusters.
		if err := inf.SetTransform(stripManagedFields); err != nil {
			return fmt.Errorf("set informer transform: %w", err)
		}
		if _, err := inf.AddEventHandler(handler); err != nil {
			return fmt.Errorf("register informer handler: %w", err)
		}
	}

	a.log.Info("starting informers", "resources", "nodes,pods,events(type=Warning)")
	factory.Start(ctx.Done())
	eventFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(),
		nodeInf.Informer().HasSynced,
		podInf.Informer().HasSynced,
		eventInf.Informer().HasSynced,
	) {
		return fmt.Errorf("informer caches failed to sync (context cancelled)")
	}
	a.log.Info("informer caches synced")

	src := collector.ListerSource{
		NodeLister:  nodeInf.Lister(),
		PodLister:   podInf.Lister(),
		EventLister: eventInf.Lister(),
	}

	// notifyChange rides the same change channel as the informers: any store
	// that updates between pushes (actions, machine issues, inventory) wakes
	// the coalescer so its data reaches the dashboard on the next debounced
	// push.
	notifyChange := func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	}

	// OBSERVED inventory (helm releases). Optional and additive: the watch is
	// restricted to helm's own release Secrets at the apiserver and its objects
	// are stripped of their payload before they are cached (see
	// inventory.ProjectHelmRelease). Nil when disabled, and unusable until (or
	// unless) it syncs — Observe then falls back to the pod labels the live
	// view already watches, and to nothing at all if those say nothing either.
	helmSecrets := a.helmReleaseLister(ctx, handler, notifyChange)

	pub := publisher.New(a.cfg.APIURL, a.cfg.ClusterID, a.cfg.AgentToken, buildinfo.Version, nil)

	// ClusterInventory (lok8s.dev) manager: a light periodic GET at the
	// full-beat cadence threads the lo-written deployment inventory (spec)
	// into every beat. PURE OBSERVATION, ungated like machineIssues — and
	// fail-soft: on a cluster that was never lok8s-deployed (no CRD/CR) the
	// snapshot stays nil and the payload simply carries no inventory block.
	// The write-back half: availableUpdates the server computes from that
	// inventory ride the heartbeat RESPONSE and land on the CR's STATUS
	// subresource (kubectl-visible, idempotent, spec never touched) — wired
	// here, BEFORE the sender starts, so the handler is set once up front.
	var invManager *inventory.Manager
	if a.dyn != nil {
		invManager = inventory.NewManager(a.dyn, a.cfg.FullInterval, notifyChange, a.log)
		pub.OnAvailableUpdates(invManager.HandleUpdates)
		go invManager.Run(ctx)
	}

	sender := publisher.NewSender(pub, backoffBase, backoffMax, a.log)
	go sender.Run(ctx)

	a.log.Info("publishing live view",
		"endpoint", pub.URL(),
		"clusterID", a.cfg.ClusterID,
		"fullInterval", a.cfg.FullInterval.String(),
		"debounce", a.cfg.Debounce.String(),
		"minGap", a.cfg.MinGap.String(),
		"reportNamespaces", a.cfg.ReportNamespaces,
	)

	// P3 desired-state loop: Poller pulls the platform's intent, the Executor
	// acts LOCALLY (MachineDeployment replica patches; execution is entirely
	// server-gated), and the actions store threads outcomes into every beat.
	// The store's notify rides the same change channel as the informers, so a
	// done/failed action reaches the dashboard on the next debounced push.
	actionStore := actions.New(notifyChange)

	// machineIssues collector: PURE OBSERVATION, deliberately ungated by the
	// execution flags — it lists Machines/MachineDeployments and reads the
	// Warning-event cache to surface machine-controller provisioning failures
	// on every beat. Fail-soft: without the managed overlay's machines read
	// RBAC (or without the CRD at all) it reports nothing and never blocks.
	issueStore := machineissues.NewStore(notifyChange)
	if a.dyn != nil {
		issueCollector := machineissues.New(a.dyn, a.cfg.MDNamespace,
			func() ([]*corev1.Event, error) { return eventInf.Lister().List(labels.Everything()) },
			issueStore, a.cfg.FullInterval, a.log)
		go issueCollector.Run(ctx)
	}

	if a.dyn != nil {
		exec := executor.New(a.dyn, actionStore, executor.Options{
			Namespace:       a.cfg.MDNamespace,
			MaxReplicas:     a.cfg.MaxReplicas,
			ObservedVersion: a.getVersion,
			// Node health for the P5 healer comes from the SAME informer cache
			// the live view reports from — one watch, one truth. Pods feed the
			// post-heal eviction unwedge (executor/unwedge.go) from the same
			// cache.
			Nodes:           func() ([]*corev1.Node, error) { return nodeInf.Lister().List(labels.Everything()) },
			Pods:            func() ([]*corev1.Pod, error) { return podInf.Lister().List(labels.Everything()) },
			EvictionTimeout: a.cfg.HealEvictionTimeout,
			Logger:          a.log,
		})
		dclient := desired.NewClient(a.cfg.APIURL, a.cfg.ClusterID, a.cfg.AgentToken, buildinfo.Version, nil)
		poller := desired.NewPoller(dclient, exec, a.cfg.DesiredPoll, backoffBase, backoffMax, a.log)
		go poller.Run(ctx)
		a.log.Info("desired-state pull loop started",
			"endpoint", dclient.URL(),
			"interval", a.cfg.DesiredPoll.String(),
			"mdNamespace", a.cfg.MDNamespace,
			"maxReplicas", a.cfg.MaxReplicas,
			"healEvictionTimeout", a.cfg.HealEvictionTimeout.String(),
		)
	} else {
		a.log.Warn("desired-state loop disabled (no dynamic client) — running report-only")
	}

	coalescer := publisher.NewCoalescer(publisher.Cadence{
		FullInterval: a.cfg.FullInterval,
		Debounce:     a.cfg.Debounce,
		MinGap:       a.cfg.MinGap,
	}, nil)

	coalescer.Run(ctx, changes, func(reason publisher.PushReason) {
		payload := collector.BuildPayload(src, collector.Meta{
			ClusterID:        a.cfg.ClusterID,
			ServerVersion:    a.getVersion(),
			AgentVersion:     buildinfo.Version,
			ReportNamespaces: a.cfg.ReportNamespaces,
		})
		// Thread the CURRENT desired-state action reports and machine issues
		// into every beat: the server persists both latest-wins and treats an
		// absent key as "clear", so the snapshots must ride along while they
		// are non-empty.
		payload.Actions = actionStore.Snapshot()
		payload.MachineIssues = issueStore.Snapshot()
		if invManager != nil {
			payload.Inventory = invManager.Snapshot()
		}
		// The DECLARED inventory (the lo-written ClusterInventory CR) wins
		// whenever it exists — it carries lok8sVersion/kind/provider/specHash
		// and curated categories, none of which are observable. Only when
		// there is none does the OBSERVED producer fill the block, so the
		// addon overview stops being dark on every cluster that was not
		// deployed by a recent lo.
		if payload.Inventory == nil && a.cfg.ObservedInventory {
			payload.Inventory = inventory.Observe(inventory.Sources{
				Secrets: helmSecrets,
				Pods:    func() ([]*corev1.Pod, error) { return podInf.Lister().List(labels.Everything()) },
			})
		}
		state.ApplyCaps(payload)
		sender.Enqueue(payload)
		a.log.Debug("queued live-view push",
			"reason", string(reason),
			"nodes", len(payload.Nodes),
			"pods", payload.Workloads.Pods.Total,
			"warnings", len(payload.Events),
			"actions", len(payload.Actions),
			"machineIssues", len(payload.MachineIssues),
			"inventory", payload.Inventory != nil,
		)
	})
	return ctx.Err()
}

// helmReleaseLister starts the OPTIONAL helm-release Secret informer and
// returns a lister for it, or nil when the watch is disabled or unusable.
//
// Three things keep this narrow:
//
//   - the list+watch is field-selected to `type=helm.sh/release.v1` at the
//     apiserver, so no other Secret is ever transferred;
//   - inventory.ProjectHelmRelease runs as the informer TRANSFORM, dropping
//     every Secret's Data before it reaches the cache and keeping only the six
//     release metadata strings the heartbeat can carry;
//   - the sync happens OFF the startup path and is time-bounded. The RBAC
//     lives in the opt-in deploy/inventory overlay, so on a cluster without it
//     the initial LIST is Forbidden; rather than delaying the first heartbeat
//     or leaving a reflector retrying forever, the returned lister reports
//     "not synced" (the caller falls back to pod labels) and the watch is
//     stopped after one log line.
//
// Failure is never fatal: every path degrades and the live view continues.
func (a *Agent) helmReleaseLister(ctx context.Context, handler cache.ResourceEventHandler, notify func()) func() ([]*corev1.Secret, error) {
	if !a.cfg.ObservedInventory {
		return nil
	}
	// Its own stop channel (not the agent's ctx): a watch that never syncs has
	// to be stoppable on its own, without touching anything else.
	stopCh := make(chan struct{})

	factory := informers.NewSharedInformerFactoryWithOptions(a.client, resyncPeriod,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("type", inventory.HelmReleaseSecretType).String()
		}))
	secretInf := factory.Core().V1().Secrets()
	if err := secretInf.Informer().SetTransform(inventory.ProjectHelmRelease); err != nil {
		// Unreachable in practice (the informer has not started yet), but a
		// transform that did not take would mean caching release payloads —
		// refuse the watch instead.
		a.log.Warn("helm release watch disabled (transform not installed)", "error", err.Error())
		close(stopCh)
		return nil
	}
	if _, err := secretInf.Informer().AddEventHandler(handler); err != nil {
		a.log.Warn("helm release watch disabled (handler not registered)", "error", err.Error())
		close(stopCh)
		return nil
	}

	// synced gates the lister: an unsynced cache is EMPTY, and an empty cache
	// is indistinguishable from "this cluster runs no helm releases" — which
	// would silently suppress the pod-label fallback. Reporting an error until
	// the cache is real keeps the fallback honest.
	var synced atomic.Bool
	lister := secretInf.Lister()

	go func() {
		factory.Start(stopCh)
		syncCtx, cancelSync := context.WithTimeout(ctx, helmSyncTimeout)
		defer cancelSync()
		if !cache.WaitForCacheSync(syncCtx.Done(), secretInf.Informer().HasSynced) {
			close(stopCh)
			a.log.Info("helm release inventory unavailable (fail-soft; needs cluster-wide secrets list/watch — deploy/inventory) — falling back to helm labels on pods",
				"timeout", helmSyncTimeout.String())
			return
		}
		synced.Store(true)
		a.log.Info("observing helm releases for the inventory block",
			"secretType", inventory.HelmReleaseSecretType)
		if notify != nil {
			notify() // ship the now-exact inventory on the next debounced push
		}
		<-ctx.Done()
		close(stopCh)
	}()

	return func() ([]*corev1.Secret, error) {
		if !synced.Load() {
			return nil, errHelmCacheNotSynced
		}
		return lister.List(labels.Everything())
	}
}

// errHelmCacheNotSynced marks "the helm-release cache is not usable (yet)" —
// never surfaced to a user, it only tells Observe to use the pod fallback.
var errHelmCacheNotSynced = errors.New("helm release cache not synced")

func (a *Agent) setVersion(v string) {
	a.mu.Lock()
	a.serverVersion = v
	a.mu.Unlock()
}

func (a *Agent) getVersion() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.serverVersion
}

func (a *Agent) refreshVersionLoop(ctx context.Context) {
	t := time.NewTicker(versionRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if v, err := kube.ServerVersion(a.client); err == nil {
				a.setVersion(v)
			}
		}
	}
}

// stripManagedFields drops ObjectMeta.managedFields before an object enters an
// informer cache. It mutates the freshly-decoded object (safe: transforms run
// pre-store, before anyone else can see it) and never fails — on an unexpected
// shape the object is stored unchanged (fail toward report-only).
func stripManagedFields(obj any) (any, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if inner, err := stripManagedFields(d.Obj); err == nil {
			d.Obj = inner
		}
		return d, nil
	}
	if m, err := meta.Accessor(obj); err == nil && len(m.GetManagedFields()) > 0 {
		m.SetManagedFields(nil)
	}
	return obj, nil
}

// changeHandler signals the Coalescer (non-blocking) on any add/update/delete
// across the watched resources. A single buffered slot is enough: the signal
// only says "something changed"; the payload is always rebuilt from the current
// caches at push time.
func changeHandler(changes chan<- struct{}) cache.ResourceEventHandlerFuncs {
	notify := func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { notify() },
		UpdateFunc: func(any, any) { notify() },
		DeleteFunc: func(any) { notify() },
	}
}
