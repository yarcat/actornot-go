package actornot

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/lmittmann/tint"
)

// MailboxPoster implements an actor-like message processing system. It serializes
// messages for a specific destination (e.g. a Telegram chat), ensuring they are
// processed sequentially in a single go-routine within the whole cloud. This approach
// guarantees consistent state management even across distributed server instances
// or workers and is designed for resilience against crashes and restarts.
//
// The design is inspired by the Actor model (see https://en.wikipedia.org/wiki/Actor_model)
// and libraries such as protoactor-go. In this paradigm, a "process"
// is responsible for enqueueing an update to the user's mailbox and scheduling
// its processing if no other process is currently active for that user.
//
// The processing lifecycle involves the following key steps, emphasizing atomicity
// for critical state changes:
//
//  1. Enqueue Update: An incoming update is added to the mailbox, which is
//     typically an array within the user's document. This operation, along with
//     incrementing a counter, must be atomic.
//  2. Attempt to Acquire Processing Lock: The system tries to transition the user's
//     state to "running". This is an atomic operation that also updates a lease
//     time to ensure the system can self-heal from crashes or restarts. If
//     another process already holds the "running" state, this instance waits.
//  3. Process Queue: If the "running" state is successfully acquired, this
//     instance processes messages from the queue until it's empty. The lease
//     time is periodically updated during this phase.
//  4. Release Lock and Re-check: Once the queue is empty, the user's state is
//     atomically transitioned to "idle". The system then checks if new updates
//     arrived during the processing period. If so, it attempts to re-acquire the
//     "running" state (returning to step 2). It's important to note that
//     another worker might acquire the "running" state at this juncture.
//
// Operations that may take significant time, such as Optical Character Recognition (OCR)
// or toxicity analysis, can be performed concurrently (e.g., in separate goroutines).
// However, when these operations complete, they must communicate their results back
// by sending a new message via this mailbox. This ensures that all interactions
// with an actor's state are funneled through its serialized message queue,
// maintaining consistency even if the actor instance is restarted or migrates
// between workers.
type MailboxPoster[
	Env Envelope,
	LEnt any, // Locked entity type e.g. db.TgState.
	LRun LockedEntityRunner[LEnt],
	LQO LockableQueueOp[LEnt],
	LQ LockableQueue[Env, LEnt, LQO],
] struct {
	queue     LQ
	runner    LRun
	scheduler ScheduleWaiter
	log       *slog.Logger

	// stopped indicates whether the mailbox is stopped and should not attempt to process messages.
	stopped atomic.Bool
	// ongoingPosts ensures Close() waits for all scheduling that cannot be interrupted.
	ongoingPosts sync.WaitGroup
	// closeOnce ensures that the mailbox can only be closed once.
	closeOnce sync.Once
}

// NewMailboxPoster creates a new MailboxPoster instance with the provided scheduler.
// Please note that the mailbox takes ownership of the scheduler, meaning it will call Wait()
// on it when Close() is called.
func NewMailboxPoster[
	Env Envelope,
	LEnt any,
	LRun LockedEntityRunner[LEnt],
	LQO LockableQueueOp[LEnt],
	LQ LockableQueue[Env, LEnt, LQO],
](
	queue LQ,
	run LRun,
	scheduler ScheduleWaiter,
	log *slog.Logger,
) *MailboxPoster[Env, LEnt, LRun, LQO, LQ] {
	return &MailboxPoster[Env, LEnt, LRun, LQO, LQ]{
		queue:     queue,
		runner:    run,
		scheduler: scheduler,
		log:       log,
	}
}

// PostMessage adds a message to the mailbox and schedules its processing.
// It returns an error if the operation fails, such as if the message cannot be enqueued.
func (m *MailboxPoster[Env, LEnt, LRun, LQO, LQ]) PostMessage(envelope Env) error {
	op, err := m.queue.Enqueue(envelope)
	if err != nil {
		return fmt.Errorf("enqueuing message: %w", err)
	}

	log := envelope.With(m.log)

	if m.stopped.Load() { // An early check to avoid unnecessary processing.
		log.Debug("mailbox is stopped, not trying to act")
		return nil
	}

	m.ongoingPosts.Add(1)
	defer m.ongoingPosts.Done()

	if m.stopped.Load() { // Check again to ensure the mailbox is still active.
		log.Debug("mailbox is stopped, not acting")
		return nil
	}

	lockedEntity, err := op.TryLock()
	if err != nil {
		log.Error("failed to acquire lock for sender", tint.Err(err))
		return nil // The event was enqueued, not reporting an error here.
	}

	if !m.runner.Runnable(lockedEntity) {
		log.Debug("locked entity is not runnable, skipping processing")
		return nil
	}

	log.Debug("acquired lock for sender, scheduling processing")
	m.scheduler.Schedule(func() {
		for {
			m.runner.RunLocked(lockedEntity)
			var err error
			if lockedEntity, err = op.UpdateLocked(lockedEntity); err != nil {
				log.Error("failed to update locked entity", tint.Err(err))
				return
			} else if !m.runner.Runnable(lockedEntity) {
				log.Debug("locked entity is no longer runnable, stopping processing")
				return
			}
		}
	})

	return nil
}

// Close stops the mailbox and waits for all ongoing posts to complete.
func (m *MailboxPoster[Env, LEnt, LRun, LQO, LQ]) Close() {
	m.closeOnce.Do(func() {
		m.stopped.Store(true)
		m.ongoingPosts.Wait()
		m.scheduler.Wait()
	})
}

// LockedEntityRunner is an interface that defines methods for running a locked entity.
type LockedEntityRunner[LEnt any] interface {
	// RunLocked processes the locked entity.
	RunLocked(LEnt)
	// Runnable checks if the locked entity can be processed. It returns true if
	// the entity is in a state that allows processing, otherwise false.
	Runnable(LEnt) bool
}

// Envelope is an interface that defines a message envelope containing a sender and a message.
// It is used to encapsulate messages sent to the mailbox, allowing the system
// to identify the sender and the content of the message.
type Envelope interface {
	// With returns a logger with additional context for this envelope.
	With(*slog.Logger) *slog.Logger
}

// LockableQueue is an interface for a queue that can be used in a mailbox system.
type LockableQueue[
	Env Envelope,
	LEnt any, // Locked entity type e.g. db.TgState.
	LQO LockableQueueOp[LEnt],
] interface {
	// Enqueue adds a message to the queue. It returns an error if the operation fails.
	Enqueue(Env) (LQO, error)
}

// LockableQueueOp is an interface that defines the operations that can be performed on a lockable queue.
// Instances of this interface are returned by the LockableQueue.Enqueue method and operate in the context
// of a specific unique event ID.
type LockableQueueOp[LEnt any] interface {
	// TryLock attempts to acquire a lock for the current unique event ID.
	TryLock() (LEnt, error)
	// UpdateLocked updates the locked entity associated with the given unique event ID.
	UpdateLocked(LEnt) (LEnt, error)
}
