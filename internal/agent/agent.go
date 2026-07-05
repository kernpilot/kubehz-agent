// Package agent wires the pieces together: in-cluster informers on
// nodes/pods/events feed a debounced Coalescer, which builds a schema-2 payload
// and hands it to the outbound Sender. There is NO inbound listener and NO
// analytics anywhere in this program — the only network egress is the
// authenticated heartbeat POST (the privacy guarantee, enforced by
// construction: there simply is no other outbound code path).
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/kernpilot/kubehz-agent/internal/buildinfo"
	"github.com/kernpilot/kubehz-agent/internal/collector"
	"github.com/kernpilot/kubehz-agent/internal/config"
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

// Agent is the long-running managed-tier live-view agent.
type Agent struct {
	cfg    *config.Config
	client kubernetes.Interface
	log    *slog.Logger

	mu            sync.RWMutex
	serverVersion string
}

// New builds an Agent. logger may be nil (uses slog.Default).
func New(cfg *config.Config, client kubernetes.Interface, logger *slog.Logger) *Agent {
	if logger == nil {
		logger = slog.Default()
	}
	return &Agent{cfg: cfg, client: client, log: logger}
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
	eventInf := factory.Core().V1().Events()

	changes := make(chan struct{}, 1)
	handler := changeHandler(changes)
	for _, inf := range []cache.SharedIndexInformer{
		nodeInf.Informer(), podInf.Informer(), eventInf.Informer(),
	} {
		if _, err := inf.AddEventHandler(handler); err != nil {
			return fmt.Errorf("register informer handler: %w", err)
		}
	}

	a.log.Info("starting informers", "resources", "nodes,pods,events")
	factory.Start(ctx.Done())
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
		state.ApplyCaps(payload)
		sender.Enqueue(payload)
		a.log.Debug("queued live-view push",
			"reason", string(reason),
			"nodes", len(payload.Nodes),
			"pods", payload.Workloads.Pods.Total,
			"warnings", len(payload.Events),
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
