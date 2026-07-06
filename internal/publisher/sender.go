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
type Sender struct {
	pub        *Publisher
	newBackoff func() *Backoff
	log        *slog.Logger
	afterFunc  func(time.Duration) <-chan time.Time // injectable timer for tests

	mu     sync.Mutex
	latest *state.Payload
	wake   chan struct{}
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
		wake:       make(chan struct{}, 1),
	}
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
			err := s.pub.Publish(ctx, p)
			if err == nil {
				s.clearIfSame(p)
				backoff.Reset()
				continue
			}
			if ctx.Err() != nil {
				return
			}

			wait := backoff.Next()
			var authErr *AuthError
			if errors.As(err, &authErr) {
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

// discard is an io.Writer that drops everything (default no-op logger sink).
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
