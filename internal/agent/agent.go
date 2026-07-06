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
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
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
	"github.com/kernpilot/kubehz-agent/internal/kube"
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

	pub := publisher.New(a.cfg.APIURL, a.cfg.ClusterID, a.cfg.AgentToken, buildinfo.Version, nil)
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
	actionStore := actions.New(func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	})
	if a.dyn != nil {
		exec := executor.New(a.dyn, a.cfg.MDNamespace, a.cfg.MaxReplicas, actionStore, a.getVersion, a.log)
		dclient := desired.NewClient(a.cfg.APIURL, a.cfg.ClusterID, a.cfg.AgentToken, buildinfo.Version, nil)
		poller := desired.NewPoller(dclient, exec, a.cfg.DesiredPoll, backoffBase, backoffMax, a.log)
		go poller.Run(ctx)
		a.log.Info("desired-state pull loop started",
			"endpoint", dclient.URL(),
			"interval", a.cfg.DesiredPoll.String(),
			"mdNamespace", a.cfg.MDNamespace,
			"maxReplicas", a.cfg.MaxReplicas,
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
		// Thread the CURRENT desired-state action reports into every beat:
		// the server persists them latest-wins and treats an absent actions[]
		// as "clear", so the snapshot must ride along while it is non-empty.
		payload.Actions = actionStore.Snapshot()
		state.ApplyCaps(payload)
		sender.Enqueue(payload)
		a.log.Debug("queued live-view push",
			"reason", string(reason),
			"nodes", len(payload.Nodes),
			"pods", payload.Workloads.Pods.Total,
			"warnings", len(payload.Events),
			"actions", len(payload.Actions),
		)
	})
	return ctx.Err()
}

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
