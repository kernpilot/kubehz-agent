package publisher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

// Sender delivers payloads with retry/backoff, decoupled from the Coalescer's
// timing. It holds a single "latest" slot: Enqueue overwrites it, so a payload
// superseded before it is sent is simply dropped in favour of the newer one
// (coalescing at the delivery layer too). This guarantees the informer/
// coalescer goroutine NEVER blocks on the network, and the freshest state is
// always what gets sent.
//
// A gate (SetGate) can hold the sends back: it is asked before EVERY send,
// a retry included, and while it is closed the pending payload is dropped,
// not kept for later. The Sender also tracks how long its sends have failed
// without a success in between (FailingFor).
type Sender struct {
	pub        *Publisher
	newBackoff func() *Backoff
	log        *slog.Logger
	afterFunc  func(time.Duration) <-chan time.Time // injectable timer for tests
	now        func() time.Time                     // injectable clock for tests
	gate       func() bool

	mu           sync.Mutex
	latest       *state.Payload
	wake         chan struct{}
	failingSince time.Time // first failure since the last success; zero = none
}

// NewSender builds a Sender. logger may be nil (discards). baseBackoff/maxBackoff
// bound the retry spacing.
func NewSender(pub *Publisher, baseBackoff, maxBackoff time.Duration, logger *slog.Logger) *Sender {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	return &Sender{
		pub:        pub,
		newBackoff: func() *Backoff { return NewBackoff(baseBackoff, maxBackoff) },
		log:        logger,
		afterFunc:  time.After,
		now:        time.Now,
		wake:       make(chan struct{}, 1),
	}
}

// SetGate installs the send gate. Call it before Run. While gate returns
// false nothing is sent: the pending payload is dropped, and the next
// Enqueue brings a fresh one. A dropped payload is neither a success nor a
// failure for FailingFor. nil removes the gate.
func (s *Sender) SetGate(gate func() bool) { s.gate = gate }

// FailingFor reports how long the sends have failed: the time since the
// first failed send after the last successful one, or zero when the last
// send went through (or none failed yet).
func (s *Sender) FailingFor() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failingSince.IsZero() {
		return 0
	}
	return s.now().Sub(s.failingSince)
}

func (s *Sender) failed() {
	s.mu.Lock()
	if s.failingSince.IsZero() {
		s.failingSince = s.now()
	}
	s.mu.Unlock()
}

func (s *Sender) succeeded() {
	s.mu.Lock()
	s.failingSince = time.Time{}
	s.mu.Unlock()
}

// Enqueue stores the payload as the latest to send and wakes the sender. It is
// non-blocking and safe to call from the Coalescer goroutine.
func (s *Sender) Enqueue(p *state.Payload) {
	s.mu.Lock()
	s.latest = p
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default: // a wake is already pending; the sender will pick up latest
	}
}

func (s *Sender) take() *state.Payload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

// clearIfSame nils the slot only if it still holds p (i.e. nothing newer
// arrived while we were sending), so a successful send doesn't drop a payload
// that superseded it.
func (s *Sender) clearIfSame(p *state.Payload) {
	s.mu.Lock()
	if s.latest == p {
		s.latest = nil
	}
	s.mu.Unlock()
}

// Run delivers enqueued payloads until ctx is cancelled. On failure it retries
// the LATEST payload with exponential backoff, waking early if a newer payload
// arrives.
func (s *Sender) Run(ctx context.Context) {
	backoff := s.newBackoff()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		}

		// Drain: keep sending until the slot is empty or ctx dies.
		for {
			p := s.take()
			if p == nil {
				break
			}
			// The gate is asked before every attempt, a retry too: a payload
			// queued while the gate was open must not go out after it closed.
			if s.gate != nil && !s.gate() {
				s.clearIfSame(p)
				continue
			}
			err := s.pub.Publish(ctx, p)
			if err == nil {
				s.clearIfSame(p)
				s.succeeded()
				backoff.Reset()
				continue
			}
			if ctx.Err() != nil {
				return
			}
			s.failed()

			var authErr *AuthError
			if errors.As(err, &authErr) && s.reloadToken(ctx) {
				// A re-enrollment rotates the Secret under the running pod
				// (the kubelet refreshes the projected file). Re-read the
				// token first: a CHANGED token is retried at once with a
				// fresh backoff, because the rejection was about the old one.
				backoff.Reset()
				continue
			}
			wait := backoff.Next()
			if authErr != nil {
				// Identity problem: surface loudly but keep retrying (recovery
				// is a redeploy/rotation, outside the agent's authority).
				s.log.Error("heartbeat auth rejected; will keep retrying",
					"error", authErr.Error(), "retryIn", wait.String(), "attempt", backoff.Attempt())
				// An auth failure is NOT cured by fresher data, so a newer
				// enqueue must not preempt the wait — otherwise a revoked token
				// would be retried at the enqueue cadence forever instead of
				// backing off to the cap. The retry still sends the LATEST
				// payload (take() below), so nothing stale is ever delivered.
				select {
				case <-ctx.Done():
					return
				case <-s.afterFunc(wait):
				}
				continue
			}
			s.log.Warn("heartbeat push failed; backing off",
				"error", err.Error(), "retryIn", wait.String(), "attempt", backoff.Attempt())

			select {
			case <-ctx.Done():
				return
			case <-s.wake:
				// A newer payload arrived; loop and send it immediately.
			case <-s.afterFunc(wait):
				// Backoff elapsed; retry the (possibly updated) latest.
			}
		}
	}
}

// reloadToken re-reads bearer A after a rejection and reports whether it
// changed. Errors are logged (never the token) and count as "unchanged".
func (s *Sender) reloadToken(ctx context.Context) bool {
	changed, err := s.pub.ReloadToken(ctx)
	if err != nil {
		s.log.Warn("could not re-read the agent token after a rejection", "error", err.Error())
		return false
	}
	if changed {
		s.log.Info("agent token changed at its source; retrying the heartbeat with the new token")
	}
	return changed
}

// discard is an io.Writer that drops everything (default no-op logger sink).
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
