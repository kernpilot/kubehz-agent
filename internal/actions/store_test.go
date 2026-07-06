package actions

import (
	"sync"
	"testing"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

func scaleAction(target, status, detail string, rev int) state.Action {
	return state.Action{
		Type: state.ActionScale, Target: target, Status: status, Detail: detail, Revision: rev,
	}
}

func TestStore_UpsertTransitionsAndSnapshot(t *testing.T) {
	var notifies int
	s := New(func() { notifies++ })

	s.Begin(3)
	s.Upsert(scaleAction("pool-a", state.ActionPending, "", 3))
	s.Upsert(scaleAction("pool-a", state.ActionInProgress, "", 3))
	s.Upsert(scaleAction("pool-a", state.ActionDone, "replicas 2 -> 3", 3))

	got := s.Snapshot()
	if len(got) != 1 {
		t.Fatalf("snapshot = %d actions, want 1 (upsert must replace by key)", len(got))
	}
	if got[0].Status != state.ActionDone || got[0].Detail != "replicas 2 -> 3" || got[0].Revision != 3 {
		t.Errorf("terminal action wrong: %+v", got[0])
	}
	if notifies != 3 {
		t.Errorf("notifies = %d, want 3 (one per transition)", notifies)
	}

	// A second pool appends rather than replaces.
	s.Upsert(scaleAction("pool-b", state.ActionFailed, "no MachineDeployment", 3))
	if got := s.Snapshot(); len(got) != 2 {
		t.Errorf("snapshot = %d actions, want 2", len(got))
	}
}

// An idempotent retry pass (same revision, same outcomes) must cause zero
// notifies — otherwise a permanently-refused pool would wake the coalescer
// every poll forever.
func TestStore_IdenticalUpsertIsSilent(t *testing.T) {
	var notifies int
	s := New(func() { notifies++ })

	s.Begin(5)
	a := scaleAction("pool-a", state.ActionFailed, "refused", 5)
	s.Upsert(a)
	before := notifies

	s.Begin(5) // retry pass, same revision: items kept
	s.Upsert(a)
	if notifies != before {
		t.Errorf("identical retry pass notified (%d -> %d)", before, notifies)
	}
	if got := s.Snapshot(); len(got) != 1 {
		t.Errorf("retry pass changed the snapshot: %+v", got)
	}
}

// A NEW revision replaces the previous revision's actions (latest-wins,
// mirroring the server); beginning it clears the old items.
func TestStore_NewRevisionClears(t *testing.T) {
	s := New(nil)
	s.Begin(1)
	s.Upsert(scaleAction("pool-a", state.ActionDone, "", 1))

	s.Begin(2)
	if got := s.Snapshot(); got != nil {
		t.Errorf("new revision did not clear old actions: %+v", got)
	}
	if s.Revision() != 2 {
		t.Errorf("revision = %d, want 2", s.Revision())
	}
}

func TestStore_ClearEmptiesAndNotifiesOnce(t *testing.T) {
	var notifies int
	s := New(func() { notifies++ })

	s.Begin(1)
	s.Upsert(scaleAction("pool-a", state.ActionDone, "", 1))
	n := notifies

	s.Clear()
	if got := s.Snapshot(); got != nil {
		t.Errorf("clear left actions: %+v", got)
	}
	if notifies != n+1 {
		t.Errorf("clear notified %d times, want 1", notifies-n)
	}

	// Clearing an already-empty store is silent (no beat churn).
	s.Clear()
	if notifies != n+1 {
		t.Errorf("empty clear notified")
	}
}

// Snapshot must return a copy: the sender marshals payloads concurrently with
// executor writes, so a shared slice would be a data race.
func TestStore_SnapshotIsACopy(t *testing.T) {
	s := New(nil)
	s.Begin(1)
	s.Upsert(scaleAction("pool-a", state.ActionPending, "", 1))

	snap := s.Snapshot()
	s.Upsert(scaleAction("pool-a", state.ActionDone, "", 1))
	if snap[0].Status != state.ActionPending {
		t.Errorf("snapshot mutated by a later upsert: %+v", snap[0])
	}
}

// The store is written by the executor goroutine and read by the coalescer
// flush; hammer it under -race.
func TestStore_ConcurrentAccess(t *testing.T) {
	s := New(func() {})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s.Begin(i)
			s.Upsert(scaleAction("pool-a", state.ActionDone, "", i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = s.Snapshot()
		}
	}()
	wg.Wait()
}

// Prune retires pending/in-progress/failed actions of one type whose targets
// are gone, keeps done outcomes and other types, and notifies only on change.
func TestStore_PruneKeepsDoneAndOtherTypes(t *testing.T) {
	notified := 0
	s := New(func() { notified++ })
	s.Begin(3)
	s.Upsert(state.Action{Type: state.ActionHeal, Target: "m-done", Status: state.ActionDone, Revision: 3})
	s.Upsert(state.Action{Type: state.ActionHeal, Target: "m-stale", Status: state.ActionPending, Revision: 3})
	s.Upsert(state.Action{Type: state.ActionHeal, Target: "m-live", Status: state.ActionFailed, Revision: 3})
	s.Upsert(state.Action{Type: state.ActionScale, Target: "m-stale", Status: state.ActionPending, Revision: 3})
	base := notified

	s.Prune(state.ActionHeal, map[string]bool{"m-live": true})

	snap := s.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot = %+v, want done+live heal and the scale action", snap)
	}
	for _, a := range snap {
		if a.Type == state.ActionHeal && a.Target == "m-stale" {
			t.Errorf("stale pending heal survived prune: %+v", a)
		}
	}
	if notified != base+1 {
		t.Errorf("notified = %d, want exactly one notify for the prune", notified-base)
	}

	// Idempotent: nothing left to prune → no notify.
	s.Prune(state.ActionHeal, map[string]bool{"m-live": true})
	if notified != base+1 {
		t.Errorf("no-op prune notified")
	}
}
