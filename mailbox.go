package actornot

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/lmittmann/tint"
)

// MailboxPoster implements a distributed actor-like message processing system that
// provides serialized, single-threaded message processing for specific destinations
// (e.g., Telegram chats, user sessions) across a distributed cloud environment.
// It ensures sequential message processing and consistent state management even
// when deployed across multiple server instances or workers.
//
// Design Philosophy:
//
// This implementation is conceptually inspired by the Actor model
// (https://en.wikipedia.org/wiki/Actor_model) and draws practical implementation
// patterns from protoactor-go. Like protoactor-go, it uses atomic operations and
// distributed locking to coordinate message processing across multiple instances.
// However, unlike protoactor-go's in-memory approach, this system is designed to
// work with heavyweight, persistent storage systems (e.g., databases) for both
// message queuing and distributed coordination.
//
// The key insight is that while traditional actor systems rely on in-process
// coordination, distributed systems require external coordination mechanisms.
// This implementation abstracts away the specific coordination mechanism through
// the LockableQueue interface, allowing it to work with various backends (MongoDB,
// PostgreSQL, Redis, etc.) while maintaining actor-like semantics.
//
// Processing Lifecycle:
//
// The system implements a lock-based processing loop that ensures only one instance
// processes messages for a given entity at any time:
//
//  1. Message Enqueuing: PostMessage() adds messages to the distributed queue via
//     LockableQueue.Enqueue(), which returns a LockableQueueOp for further operations.
//
//  2. Lock Acquisition: The system attempts to acquire an exclusive lock through
//     LockableQueueOp.TryLock(). This is typically implemented as an atomic
//     database operation (e.g., conditional updates with timestamps for lease-based
//     locking). If the lock is already held by another instance, this attempt
//     gracefully fails without blocking.
//
//  3. Processing Loop: Once locked, the system schedules a goroutine that repeatedly:
//     - Processes the locked entity via LockedEntityRunner.RunLocked()
//     - Updates the lock state via LockableQueueOp.UpdateLocked()
//     - Checks if more processing is needed via LockedEntityRunner.Runnable()
//     - Continues until the queue is empty or the entity becomes non-runnable
//
//  4. Automatic Lock Release: The processing loop terminates when no more work
//     is available, automatically releasing the lock through the UpdateLocked()
//     mechanism. Other instances can then acquire the lock if new messages arrive.
//
// Fault Tolerance:
//
// The system is designed for resilience against crashes and network partitions:
// - Locks are typically implemented with lease timeouts to handle crashed instances
// - Message enqueuing is separate from lock acquisition, ensuring no message loss
// - The UpdateLocked() mechanism allows for lease renewal during long processing
// - Graceful shutdown via Close() ensures clean termination and resource cleanup
//
// Concurrency Model:
//
// While message processing for each entity is serialized, the system supports
// high concurrency through:
//   - Multiple entities can be processed simultaneously by different instances
//   - Long-running operations (OCR, AI analysis) can spawn separate goroutines
//     and communicate results back through new messages to maintain ordering
//   - The scheduler abstraction allows for both concurrent and serial execution modes
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
