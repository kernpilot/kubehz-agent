package inventory

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

const (
	// getTimeout bounds each apiserver GET so a wedged connection can never
	// stall the poll loop; patchTimeout does the same for the status write
	// (which runs on the Sender goroutine — it must never wedge the beat loop).
	getTimeout   = 15 * time.Second
	patchTimeout = 15 * time.Second
	// fieldManager identifies this agent's status writes in managedFields —
	// its own manager, distinct from `lo` (which owns spec and never writes
	// status) and from any other in-cluster observer.
	fieldManager = "kubehz-agent"
)

// Manager owns the agent's view of the ClusterInventory CR: a periodic GET
// (full-beat cadence — the CR changes only on lo deploys) feeding Snapshot(),
// which the coalescer threads into every heartbeat payload.
//
// Fail-soft: an absent CRD/CR or missing read RBAC yields a nil Snapshot (no
// inventory block on the beat) and one log line per distinct error — never a
// crash, never a retry storm.
type Manager struct {
	dyn      dynamic.Interface
	interval time.Duration
	notify   func()
	log      *slog.Logger

	now       func() time.Time                     // injectable clock for tests
	afterFunc func(time.Duration) <-chan time.Time // injectable timer for tests

	mu      sync.Mutex
	inv     *state.Inventory // last read spec; nil = CR absent/unreadable
	present bool             // the CR currently exists (gates status writes)
	lastErr string           // last GET failure (log-throttling)

	// statusUpdates is the CR's current status.availableUpdates — refreshed
	// on every poll and after every successful write — the compare-before-
	// patch baseline that makes HandleUpdates idempotent.
	statusUpdates []state.AvailableUpdate
	// warnedRBAC: a Forbidden status patch warns ONCE, then stays quiet —
	// a base-RBAC-not-yet-upgraded cluster must not warn on every beat.
	warnedRBAC   bool
	lastPatchErr string // last non-RBAC patch failure (log-throttling)
}

// NewManager builds a Manager polling at interval (the full-beat cadence;
// floored at 1m when non-positive). notify and logger may be nil.
func NewManager(dyn dynamic.Interface, interval time.Duration, notify func(), logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Manager{
		dyn:       dyn,
		interval:  interval,
		notify:    notify,
		log:       logger,
		now:       time.Now,
		afterFunc: time.After,
	}
}

// Run polls until ctx is cancelled. The first fetch fires immediately so the
// very first heartbeat already carries the inventory block.
func (m *Manager) Run(ctx context.Context) {
	for {
		m.fetchOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-m.afterFunc(m.interval):
		}
	}
}

// Snapshot returns a COPY of the current inventory (nil when the CR is
// absent/unreadable). A copy, not the shared pointer: the caller (coalescer →
// ApplyCaps → sender) mutates and marshals it concurrently with the next poll.
func (m *Manager) Snapshot() *state.Inventory {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inv == nil {
		return nil
	}
	cp := *m.inv
	if m.inv.Addons != nil {
		cp.Addons = make([]state.Addon, len(m.inv.Addons))
		copy(cp.Addons, m.inv.Addons)
	}
	return &cp
}

// fetchOnce performs one GET and updates the snapshot, notifying the coalescer
// only when the inventory actually changed (a lo deploy happened).
func (m *Manager) fetchOnce(ctx context.Context) {
	gctx, cancel := context.WithTimeout(ctx, getTimeout)
	u, err := m.dyn.Resource(GVR).Get(gctx, CRName, metav1.GetOptions{})
	cancel()

	if err != nil {
		// Fail-soft: NotFound covers both "CRD not installed" (the apiserver
		// 404s the whole resource path) and "CR not written yet" — the normal
		// state of every cluster that was never lok8s-deployed, so it is not
		// even log-worthy beyond debug. Anything else (RBAC, timeout) logs
		// once per distinct error.
		if apierrors.IsNotFound(err) {
			m.setState(nil, nil, false)
			return
		}
		if msg := err.Error(); msg != m.getLastErr() {
			m.setLastErr(msg)
			m.log.Info("cluster inventory unavailable (fail-soft; needs the clusterinventories read RBAC)",
				"error", msg)
		}
		m.setState(nil, nil, false)
		return
	}
	m.setLastErr("")
	m.setState(specInventory(u), statusAvailableUpdates(u), true)
}

// HandleUpdates consumes a server VERDICT on availableUpdates — the caller
// (publisher.OnAvailableUpdates) invokes it ONLY when the heartbeat response
// carried the key at all. The key is tristate (kubehz-api f628c97): ABSENT
// (no inventory reported / addons index unreachable) never reaches here, so
// an index outage can never wipe the CR's last known status; a PRESENT
// verdict is written — plus lastReported — via the STATUS subresource, so
// `kubectl get clusterinventory cluster -o yaml` shows addon updates with no
// dashboard; and an EMPTY verdict ([] = "index consulted, nothing is newer")
// CLEARS status.availableUpdates — without that, a user who upgrades their
// addons would keep stale "update available" notices in the CR forever.
//
// A JSON merge patch touching ONLY status.availableUpdates+lastReported: the
// agent never writes spec (lo-owned) and never touches status.observedAddons
// (merge semantics leave sibling keys alone). Idempotent — the incoming list
// is compared against the CR's current status first (refreshed on every poll
// and every successful write), so a server repeating itself beat after beat —
// including an already-clear [] — costs zero writes. RBAC-denied warns once
// and the agent keeps beating.
func (m *Manager) HandleUpdates(ctx context.Context, updates []state.AvailableUpdate) {
	capped := capUpdates(updates)
	if len(capped) == 0 && len(updates) > 0 {
		// Every entry was invalid (nameless) — a buggy server, not a clear
		// verdict. Only an EXPLICIT [] may clear the user's status.
		return
	}
	updates = capped

	m.mu.Lock()
	if !m.present {
		m.mu.Unlock()
		m.log.Debug("server sent an availableUpdates verdict but no ClusterInventory CR exists; skipping status write")
		return
	}
	if equalUpdates(m.statusUpdates, updates) {
		m.mu.Unlock()
		return // unchanged (incl. already-clear) — skip the write (idempotence)
	}
	m.mu.Unlock()

	if updates == nil {
		// Marshal the clear verdict as an explicit [], NOT null: in a JSON
		// merge patch null DELETES the field, while [] keeps it present-and-
		// empty — "checked, nothing newer", distinguishable from never-checked.
		updates = []state.AvailableUpdate{}
	}
	patch, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"availableUpdates": updates,
			"lastReported":     m.now().UTC().Format(time.RFC3339),
		},
	})
	if err != nil { // unreachable for these types; fail-soft regardless
		return
	}

	pctx, cancel := context.WithTimeout(ctx, patchTimeout)
	_, err = m.dyn.Resource(GVR).Patch(pctx, CRName, types.MergePatchType, patch,
		metav1.PatchOptions{FieldManager: fieldManager}, "status")
	cancel()

	if err != nil {
		switch {
		case apierrors.IsForbidden(err):
			m.mu.Lock()
			warned := m.warnedRBAC
			m.warnedRBAC = true
			m.mu.Unlock()
			if !warned {
				// Warn ONCE: updates stay visible on the dashboard; only the
				// kubectl-visible mirror is missing. Beating continues.
				m.log.Warn("cannot write ClusterInventory status (clusterinventories/status patch RBAC missing) — apply the current deploy/base RBAC for kubectl-visible addon updates",
					"error", err.Error())
			}
		case apierrors.IsNotFound(err):
			// CR deleted between poll and beat; the next poll re-syncs.
			m.setState(nil, nil, false)
		default:
			if msg := err.Error(); msg != m.getLastPatchErr() {
				m.setLastPatchErr(msg)
				m.log.Warn("ClusterInventory status write failed; will retry on a later beat", "error", msg)
			}
		}
		return
	}

	m.setLastPatchErr("")
	m.mu.Lock()
	m.statusUpdates = updates
	m.mu.Unlock()
	m.log.Info("wrote availableUpdates to ClusterInventory status", "updates", len(updates))
}

// setState stores the latest view and signals the coalescer when the SPEC
// changed (status is not part of the payload, so it never triggers a beat).
func (m *Manager) setState(inv *state.Inventory, updates []state.AvailableUpdate, present bool) {
	m.mu.Lock()
	changed := !reflect.DeepEqual(m.inv, inv)
	m.inv = inv
	m.statusUpdates = updates
	m.present = present
	m.mu.Unlock()
	if changed && m.notify != nil {
		m.notify()
	}
}

// capUpdates clamps the server-supplied list to the ClusterInventory CRD's
// status.availableUpdates bounds (maxItems 256; name<=253, versions<=64;
// name required) so a buggy/hostile response can never produce a status
// write the apiserver would reject — or bloat the user's CR.
func capUpdates(updates []state.AvailableUpdate) []state.AvailableUpdate {
	var kept []state.AvailableUpdate
	for _, u := range updates {
		if u.Name == "" {
			continue
		}
		if len(kept) == state.MaxAvailableUpdates {
			break
		}
		u.Name = clampString(u.Name, state.MaxNameLen)
		u.Current = clampString(u.Current, state.MaxVersionLen)
		u.Latest = clampString(u.Latest, state.MaxVersionLen)
		kept = append(kept, u)
	}
	return kept
}

// equalUpdates compares two update lists entry by entry (order-sensitive: the
// server owns the ordering; a reorder is a legitimate change).
func equalUpdates(a, b []state.AvailableUpdate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// clampString truncates s to at most n runes (rune-safe).
func clampString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func (m *Manager) getLastErr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastErr
}

func (m *Manager) setLastErr(msg string) {
	m.mu.Lock()
	m.lastErr = msg
	m.mu.Unlock()
}

func (m *Manager) getLastPatchErr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastPatchErr
}

func (m *Manager) setLastPatchErr(msg string) {
	m.mu.Lock()
	m.lastPatchErr = msg
	m.mu.Unlock()
}
