package machineissues

import (
	"sync"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// Store holds the current machineIssues snapshot the heartbeat threads into
// every beat — the same in-memory, latest-wins lifecycle as the actions store
// (kubehz-api persists machineIssues latest-wins; an ABSENT key clears them,
// so an empty store must yield a nil snapshot for omitempty to drop the key).
type Store struct {
	mu     sync.Mutex
	items  []state.MachineIssue
	notify func()
}

// NewStore builds a Store. notify (optional, may be nil) fires after any
// VISIBLE change — wire it to the agent's change channel so a fresh issue
// reaches the dashboard on the next debounced beat. Called without the lock
// held; must be non-blocking.
func NewStore(notify func()) *Store {
	if notify == nil {
		notify = func() {}
	}
	return &Store{notify: notify}
}

// Set replaces the snapshot. Notifies only when the content actually changed,
// so a steady-state pass (same issues, or none→none) causes no beat churn.
func (s *Store) Set(issues []state.MachineIssue) {
	s.mu.Lock()
	changed := !equal(s.items, issues)
	if changed {
		s.items = make([]state.MachineIssue, len(issues))
		copy(s.items, issues)
	}
	s.mu.Unlock()
	if changed {
		s.notify()
	}
}

// Snapshot returns a copy of the current issues (nil when empty — omitempty).
// Never returns a shared slice: the sender marshals payloads concurrently
// with collector writes.
func (s *Store) Snapshot() []state.MachineIssue {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) == 0 {
		return nil
	}
	out := make([]state.MachineIssue, len(s.items))
	copy(out, s.items)
	return out
}

func equal(a, b []state.MachineIssue) bool {
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
