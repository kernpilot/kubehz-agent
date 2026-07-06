// Package actions keeps the agent's desired-state action reports (P3): a
// small, thread-safe, IN-MEMORY store the executor writes and every heartbeat
// reads. In-memory is deliberate (managed-platform-spec §3): a restart
// re-polls /desired and reconverges idempotently, so the store rebuilds
// itself — no PV, no CRD, no state the user has to clean up.
//
// Lifecycle contract with the server (kubehz-api dfa9b7a): the heartbeat's
// actions[] is persisted latest-wins and a beat WITHOUT actions clears them.
// The store therefore holds the actions of the most recently processed
// desired-state revision and every beat carries its snapshot verbatim —
// "current/recent actions keep riding every beat while relevant", and an
// emptied store makes the next beat clear them server-side.
package actions

import (
	"sync"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// Store is the actions holder. Zero value is not usable — construct with New.
type Store struct {
	mu       sync.Mutex
	revision int
	items    []state.Action // insertion-ordered; keyed by (Type, Target)
	notify   func()
}

// New builds a Store. notify (optional, may be nil) fires after any VISIBLE
// change — wire it to the agent's change channel so a done/failed action
// reaches the dashboard on the next debounced beat instead of waiting for the
// periodic one. It is called without the lock held and must be non-blocking
// (the agent wires the same non-blocking channel send the informers use).
func New(notify func()) *Store {
	if notify == nil {
		notify = func() {}
	}
	return &Store{notify: notify}
}

// Begin opens a reporting pass for revision. A NEW revision replaces the
// previous revision's actions (latest-wins, mirroring the server's
// persistence). Re-beginning the SAME revision keeps the existing items so a
// retry pass upserts over them without a visible clear-then-refill flap — and
// without spurious notifies when the retry reproduces identical outcomes.
func (s *Store) Begin(revision int) {
	s.mu.Lock()
	changed := false
	if revision != s.revision {
		changed = len(s.items) > 0
		s.items = nil
		s.revision = revision
	}
	s.mu.Unlock()
	if changed {
		s.notify()
	}
}

// Upsert records an action keyed by (Type, Target), replacing an existing
// entry for the same key. It notifies ONLY when the stored value actually
// changed, so an idempotent retry pass that reproduces the same outcomes
// causes no beat churn.
func (s *Store) Upsert(a state.Action) {
	s.mu.Lock()
	changed := true
	found := false
	for i := range s.items {
		if s.items[i].Type == a.Type && s.items[i].Target == a.Target {
			found = true
			if s.items[i] == a {
				changed = false
			} else {
				s.items[i] = a
			}
			break
		}
	}
	if !found {
		s.items = append(s.items, a)
	}
	s.mu.Unlock()
	if changed {
		s.notify()
	}
}

// Clear drops all actions (report-only posture: the next beat carries no
// actions[], which the server treats as "clear"). Notifies only if there was
// something to clear.
func (s *Store) Clear() {
	s.mu.Lock()
	had := len(s.items) > 0
	s.items = nil
	s.revision = 0
	s.mu.Unlock()
	if had {
		s.notify()
	}
}

// Snapshot returns a copy of the current actions for the payload builder.
// Never returns a shared slice — the sender marshals payloads concurrently
// with executor writes.
func (s *Store) Snapshot() []state.Action {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		return nil
	}
	out := make([]state.Action, len(s.items))
	copy(out, s.items)
	return out
}

// Revision reports the revision the store currently holds actions for
// (0 = none). For logging/tests.
func (s *Store) Revision() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}
