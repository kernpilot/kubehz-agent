package inventory

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// getTimeout bounds each apiserver GET so a wedged connection can never stall
// the poll loop.
const getTimeout = 15 * time.Second

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
			m.setInventory(nil, false)
			return
		}
		if msg := err.Error(); msg != m.getLastErr() {
			m.setLastErr(msg)
			m.log.Info("cluster inventory unavailable (fail-soft; needs the clusterinventories read RBAC)",
				"error", msg)
		}
		m.setInventory(nil, false)
		return
	}
	m.setLastErr("")
	m.setInventory(specInventory(u), true)
}

// setInventory stores the latest view and signals the coalescer on change.
func (m *Manager) setInventory(inv *state.Inventory, present bool) {
	m.mu.Lock()
	changed := !reflect.DeepEqual(m.inv, inv)
	m.inv = inv
	m.present = present
	m.mu.Unlock()
	if changed && m.notify != nil {
		m.notify()
	}
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
