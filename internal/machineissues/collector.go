// Package machineissues surfaces machine-controller PROVISIONING FAILURES on
// the heartbeat (kubehz-api d57c206, machineIssues[]): a pool that never
// converges is otherwise INVISIBLE — its machines never become nodes, so the
// node list the live view reports simply doesn't shrink or grow.
//
// Pure observation, deliberately UNGATED: this loop runs regardless of the
// server's execution flags (it writes nothing to the cluster) and ships
// independently of the acting executors. Three grounded issue sources (see
// internal/machines for the CRD grounding):
//
//  1. TERMINAL errors — Machine.status.errorReason/.errorMessage, which
//     machine-controller sets only for "manual intervention required" failures
//     (InvalidConfiguration, CreateError, JoinClusterTimeoutError, …).
//  2. RETRY-LOOP errors — transient failures are NOT status fields; the
//     controller records them as Warning events on the Machine (reason
//     "ReconcilingError"). This is where the dogfooded "webhook accepted the
//     spec but hcloud rejects every create: unsupported location for server
//     type" case lives, so the collector reads the agent's existing Warning-
//     event cache for events whose involvedObject is a Machine.
//  3. NODE-JOIN TIMEOUT — a machine whose node never appeared within a
//     conservative window (agent-synthesized reason "NodeJoinTimeout"): the
//     machine exists, nothing reports an error, and still no node joins.
//
// Fail-soft by design: no Machines API (no CRD, or the read RBAC of the
// managed overlay is absent on a registered-tier cluster) → the snapshot is
// empty and the agent carries on — the collector must never block or crash
// the live view.
package machineissues

import (
	"context"
	"log/slog"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"github.com/kernpilot/kubehz-agent/internal/machines"
	"github.com/kernpilot/kubehz-agent/internal/state"
)

const (
	// listTimeout bounds each apiserver list so a wedged connection can never
	// stall the collection loop.
	listTimeout = 15 * time.Second
	// nodeJoinTimeout is the OBSERVATIONAL "machine has no node yet" window.
	// Matches the server's healing-policy default (nodeStartupTimeoutSeconds
	// 600) but is deliberately a constant: issue reporting ships ungated and
	// must not depend on the desired-state doc that gates acting.
	nodeJoinTimeout = 10 * time.Minute
	// reasonNodeJoinTimeout is the agent-synthesized reason for source 3 —
	// distinct from machine-controller's own terminal "JoinClusterTimeoutError"
	// (which only fires when its -join-cluster-timeout flag is set).
	reasonNodeJoinTimeout = "NodeJoinTimeout"
	// unknownPool labels an issue whose machine has no resolvable owning
	// MachineDeployment (the server requires a non-empty pool).
	unknownPool = "unknown"
	// maxIssues mirrors the server cap; the store never holds more (oldest
	// first, so an ongoing incident keeps a stable, deterministic view).
	maxIssues = state.MaxMachineIssues
)

// EventSource supplies the current Warning events (the agent wires the
// type=Warning informer lister; tests wire fixtures).
type EventSource func() ([]*corev1.Event, error)

// Collector periodically lists Machines (+ MachineDeployments for pool
// resolution), maps failures to machineIssues entries, and publishes them to
// the Store. It keeps a first-seen map so `since` is stable across passes.
type Collector struct {
	dyn       dynamic.Interface
	namespace string
	events    EventSource
	store     *Store
	interval  time.Duration
	log       *slog.Logger

	now       func() time.Time       // injectable clock for tests
	firstSeen map[issueKey]time.Time // issue → first observed
	lastErr   string                 // last list failure (log-throttling)
	afterFunc func(time.Duration) <-chan time.Time
}

type issueKey struct {
	pool    string
	machine string
	reason  string
}

// New builds a Collector. events and logger may be nil (no event source →
// only status/timeout issues; slog.Default).
func New(dyn dynamic.Interface, namespace string, events EventSource, store *Store, interval time.Duration, logger *slog.Logger) *Collector {
	if events == nil {
		events = func() ([]*corev1.Event, error) { return nil, nil }
	}
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Collector{
		dyn:       dyn,
		namespace: namespace,
		events:    events,
		store:     store,
		interval:  interval,
		log:       logger,
		now:       time.Now,
		firstSeen: make(map[issueKey]time.Time),
		afterFunc: time.After,
	}
}

// Run collects until ctx is cancelled. The first pass fires immediately so a
// restarted agent re-reports an ongoing incident without waiting an interval
// (`since` restarts at the observation time — first-seen is in-memory only,
// which is honest: it is the AGENT-observed timestamp, spec'd as exactly that).
func (c *Collector) Run(ctx context.Context) {
	for {
		c.collectOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-c.afterFunc(c.interval):
		}
	}
}

// collectOnce performs one list+map pass and publishes the result.
func (c *Collector) collectOnce(ctx context.Context) {
	lctx, cancel := context.WithTimeout(ctx, listTimeout)
	machineList, err := machines.ListMachines(lctx, c.dyn, c.namespace)
	cancel()
	if err != nil {
		// Fail-soft: no Machines API (CRD absent / RBAC absent) → clear and
		// carry on. Log once per distinct error, not once per minute.
		if msg := err.Error(); msg != c.lastErr {
			c.lastErr = msg
			c.log.Info("machine issue collection unavailable (fail-soft; needs the managed overlay's machines read RBAC)",
				"namespace", c.namespace, "error", msg)
		}
		c.publish(nil)
		return
	}
	c.lastErr = ""

	// MDs are only needed for pool naming; a failure degrades resolution, not
	// the collection ("unknown" pools are still visible issues).
	lctx, cancel = context.WithTimeout(ctx, listTimeout)
	mds, mdErr := machines.ListMachineDeployments(lctx, c.dyn, c.namespace)
	cancel()
	if mdErr != nil {
		c.log.Debug("machinedeployment list failed; pool names degrade to ownerRef/unknown",
			"namespace", c.namespace, "error", mdErr.Error())
	}
	resolver := machines.NewPoolResolver(mds)

	events, evErr := c.events()
	if evErr != nil {
		events = nil
	}

	c.publish(c.mapIssues(machineList, resolver, events))
}

// rawIssue is an issue before first-seen stamping.
type rawIssue struct {
	key     issueKey
	message string
}

// mapIssues derives the current issue set from the machine list + events.
func (c *Collector) mapIssues(machineList []unstructured.Unstructured, resolver *machines.PoolResolver, events []*corev1.Event) []state.MachineIssue {
	now := c.now()

	byName := make(map[string]*unstructured.Unstructured, len(machineList))
	for i := range machineList {
		byName[machineList[i].GetName()] = &machineList[i]
	}

	seen := make(map[issueKey]bool)
	var raws []rawIssue
	add := func(pool, machine, reason, message string) {
		if reason == "" {
			return
		}
		if pool == "" {
			pool = unknownPool
		}
		key := issueKey{pool: pool, machine: machine, reason: reason}
		if seen[key] {
			return
		}
		seen[key] = true
		raws = append(raws, rawIssue{key: key, message: message})
	}

	for i := range machineList {
		m := &machineList[i]
		if machines.Deleting(m) {
			continue // being deprovisioned; its errors are on their way out
		}
		pool, _ := resolver.PoolFor(m)

		// 1. Terminal machine-controller errors (status.errorReason/Message).
		if reason := machines.ErrorReason(m); reason != "" {
			add(pool, m.GetName(), reason, machines.ErrorMessage(m))
		}

		// 3. Node never joined within the observation window.
		if machines.NodeRefName(m) == "" {
			if age := machines.Age(m, now); age >= nodeJoinTimeout {
				add(pool, m.GetName(), reasonNodeJoinTimeout,
					"machine has no Node "+age.Truncate(time.Minute).String()+" after creation")
			}
		}
	}

	// 2. Retry-loop errors: Warning events whose involvedObject is a Machine
	// that still exists (a deleted machine's events are stale noise). Newest
	// event per (machine, reason) wins the message.
	newest := make(map[issueKey]time.Time)
	for _, ev := range events {
		if ev == nil || ev.InvolvedObject.Kind != "Machine" || ev.InvolvedObject.Namespace != c.namespace {
			continue
		}
		m := byName[ev.InvolvedObject.Name]
		if m == nil || machines.Deleting(m) {
			continue
		}
		pool, _ := resolver.PoolFor(m)
		if pool == "" {
			pool = unknownPool
		}
		key := issueKey{pool: pool, machine: m.GetName(), reason: ev.Reason}
		ts := eventTime(ev)
		if seen[key] && !ts.After(newest[key]) {
			continue
		}
		if seen[key] {
			// Replace the message with the newer event's.
			for i := range raws {
				if raws[i].key == key {
					raws[i].message = ev.Message
					break
				}
			}
		} else {
			seen[key] = true
			raws = append(raws, rawIssue{key: key, message: ev.Message})
		}
		newest[key] = ts
	}

	// Stamp first-seen (stable `since` across passes) and prune keys that
	// recovered, so memory never grows past the live issue set.
	current := make(map[issueKey]bool, len(raws))
	issues := make([]state.MachineIssue, 0, len(raws))
	for _, r := range raws {
		current[r.key] = true
		first, ok := c.firstSeen[r.key]
		if !ok {
			first = now
			c.firstSeen[r.key] = now
		}
		issues = append(issues, state.MachineIssue{
			Pool:    r.key.pool,
			Machine: r.key.machine,
			Reason:  r.key.reason,
			Message: r.message,
			Since:   first.UTC().Format(time.RFC3339),
		})
	}
	for key := range c.firstSeen {
		if !current[key] {
			delete(c.firstSeen, key)
		}
	}

	// Deterministic, oldest-first: an over-cap incident keeps a stable view
	// instead of flapping with list order, and the cap keeps the longest-
	// standing failures (the ones a human most needs to see).
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Since != issues[j].Since {
			return issues[i].Since < issues[j].Since
		}
		if issues[i].Pool != issues[j].Pool {
			return issues[i].Pool < issues[j].Pool
		}
		if issues[i].Machine != issues[j].Machine {
			return issues[i].Machine < issues[j].Machine
		}
		return issues[i].Reason < issues[j].Reason
	})
	if len(issues) > maxIssues {
		issues = issues[:maxIssues]
	}
	return issues
}

func (c *Collector) publish(issues []state.MachineIssue) {
	if c.store != nil {
		c.store.Set(issues)
	}
}

// eventTime picks the freshest timestamp an Event carries (series → last →
// event time → creation), tolerating the fields' many historical homes.
func eventTime(ev *corev1.Event) time.Time {
	if ev.Series != nil && !ev.Series.LastObservedTime.IsZero() {
		return ev.Series.LastObservedTime.Time
	}
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	return ev.CreationTimestamp.Time
}
