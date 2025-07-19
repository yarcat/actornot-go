package main

// Counter Example - MongoDB Actor Pattern Implementation
//
// This example demonstrates distributed actor-like message processing using MongoDB
// for coordination. The key insight is using MongoDB's atomic operations to implement
// distributed locking and event queuing across multiple server instances.
//
// CORE CONCEPTS:
//
// 1. DISTRIBUTED LOCKING:
//    - Each user state has a "lock" field with expiration time and owner ID.
//    - Only one server instance can process events for a user at any time.
//    - Locks are lease-based: they automatically expire if a server crashes.
//    - Uses MongoDB's atomic FindOneAndUpdate to prevent race conditions.
//
// 2. EVENT QUEUE:
//    - Events are stored in an array field within each user's document.
//    - New events are added atomically using $push operations.
//    - Events are cleared atomically when processing begins.
//    - Sequential processing ensures state consistency.
//
// 3. PROCESSING FLOW:
//    a) Webhook receives event → Enqueue() adds event to MongoDB.
//    b) MailboxPoster calls TryLock() → attempts to acquire processing lock.
//    c) If lock acquired → RunLocked() processes all events in batch.
//    d) UpdateLocked() saves state and checks for new events.
//    e) If new events exist → continue processing, else release lock.
//
// 4. FAULT TOLERANCE:
//    - Lock expiration handles crashed servers.
//    - Atomic operations prevent lost or duplicate processing.
//    - Multiple instances can run safely - MongoDB coordinates them.
//
// This pattern is ideal for systems that need:
// - Sequential processing per entity (user/session/chat).
// - Distributed deployment across multiple servers.
// - Fault tolerance and automatic recovery.
// - High concurrency across different entities.

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/yarcat/actornot-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// State represents a simple state document in MongoDB with a counter.
type State struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	UserID    string        `bson:"user_id"`
	Counter   int64         `bson:"counter"`
	CreatedAt time.Time     `bson:"created_at,omitempty"`
	UpdatedAt time.Time     `bson:"updated_at,omitempty"`

	// Lock mechanism - implements distributed locking across multiple server instances
	// This ensures only ONE instance can process events for a user at any given time.
	// The lock uses lease-based expiration for fault tolerance - if a server crashes,
	// the lock will automatically expire and another instance can take over.
	Lock struct {
		Until time.Time `bson:"until,omitempty"` // Lock expiration time (lease-based).
		Owner string    `bson:"owner,omitempty"` // Unique identifier of the lock owner.
	} `bson:"lock,omitempty"`

	// Event queue - stores pending events to be processed sequentially.
	//
	// Keeping events as an array inside the main document is an optimization for atomicity and performance:
	// it allows us to atomically enqueue, dequeue, and clear events together with state updates in a single
	// MongoDB operation. However, it is also possible to use a separate collection for events, which may be
	// preferable for very large or long-lived event streams.
	//
	// When working with the events array, it is important to know whether the array comes from the 'before'
	// or 'after' version of the document returned by MongoDB's FindOneAndUpdate. The 'before' version contains
	// the events to process, while the 'after' version may have already cleared them. Careful handling is required
	// to avoid missing or double-processing events.
	//
	// IMPORTANT: The events array is the only field in the document that is allowed to be mutated (appended to or cleared)
	// while the document is locked for processing. All other fields must only be changed by the processor holding the lock.
	// This ensures that event enqueuing is always safe and concurrent, while state changes remain strictly sequential.
	//
	// By design, this implementation consumes (processes and clears) all events at once in a batch for efficiency.
	// However, it is absolutely possible to process events one by one, or to remove only the events that have been
	// handled so far when updating the locked state, depending on the application's requirements.
	Events []Event `bson:"events,omitempty"`
}

// Event represents a counter increment event.
//
// In this example, the only event type is a counter increment (Delta). If you need to support
// multiple event types, you would typically define a wrapper struct (e.g., with a Type field and
// a pointer to the actual event payload), and use pointers in the Events array. Then, in RunLocked,
// you would dispatch handling based on the event type.
type Event struct {
	CreatedAt time.Time `bson:"created_at"`
	Delta     int64     `bson:"delta"` // Amount to increment counter by.
}

// CounterEnvelope wraps an event with user information.
type CounterEnvelope struct {
	UserID   string
	LockName string
	Event    Event
}

// With implements actornot.Envelope.
func (e *CounterEnvelope) With(log *slog.Logger) *slog.Logger {
	return log.With("user_id", e.UserID, "lock", e.LockName)
}

// MongoDB helpers.
func A(items ...any) bson.A           { return bson.A(items) }
func D(items ...bson.E) bson.D        { return bson.D(items) }
func E(key string, value any) bson.E  { return bson.E{Key: key, Value: value} }
func DE(key string, value any) bson.D { return D(E(key, value)) }

// CounterQueue implements actornot.LockableQueue for MongoDB.
type CounterQueue struct {
	collection   *mongo.Collection
	lockDuration time.Duration
}

func NewCounterQueue(collection *mongo.Collection) *CounterQueue {
	return &CounterQueue{
		collection:   collection,
		lockDuration: time.Minute,
	}
}

func (q *CounterQueue) Enqueue(env *CounterEnvelope) (*CounterQueueOp, error) {
	// CRITICAL: Add event to the queue atomically using MongoDB's upsert operation
	// This is the entry point for all events - webhook handlers call this to add events.
	// The operation is atomic, meaning either the event is added successfully or
	// the operation fails entirely (no partial state).

	now := time.Now()
	filter := DE("user_id", env.UserID)
	update := D(
		// Push the new event to the events array.
		E("$push", DE("events", env.Event)),

		// If document doesn't exist, create it with these initial values.
		// This handles the "first event for a new user" case.
		E("$setOnInsert", D(
			E("user_id", env.UserID),
			E("counter", 0),
			E("created_at", now),
		)),

		// Always update the timestamp regardless of insert/update.
		E("$set", DE("updated_at", now)),
	)
	updateOpts := options.UpdateOne().SetUpsert(true)
	_, err := q.collection.UpdateOne(context.Background(), filter, update, updateOpts)
	if err != nil {
		return nil, err
	}

	return &CounterQueueOp{
		queue:    q,
		userID:   env.UserID,
		lockName: env.LockName,
	}, nil
}

// CounterQueueOp implements actornot.LockableQueueOp.
type CounterQueueOp struct {
	queue    *CounterQueue
	userID   string
	lockName string
}

func (op *CounterQueueOp) TryLock() (*State, error) {
	return op.queue.tryLock(op.userID, op.lockName)
}

func (op *CounterQueueOp) UpdateLocked(state *State) (*State, error) {
	return op.queue.updateLocked(state, op.lockName)
}

// tryLock implements the distributed locking mechanism.
// This is the HEART of the actor pattern - it ensures only one instance processes
// events for a given user across the entire distributed system.
func (q *CounterQueue) tryLock(userID, lockOwner string) (*State, error) {
	now := time.Now()
	lockUntil := now.Add(q.lockDuration)

	// CRITICAL FILTER: This complex MongoDB filter is the key to distributed locking.
	// It uses atomic operations to ensure only one instance can acquire the lock.
	filter := D(
		E("user_id", userID), // Target the specific user.
		// Only proceed if there are events to process - no point locking an empty queue.
		E("events.0", DE("$exists", true)), // At least one event exists.
		// Lock acquisition logic: we can acquire the lock if ANY of these conditions are true:
		E("$or", A(
			// Case 1: No lock exists yet (first time processing for this user).
			DE("lock.until", DE("$exists", false)),
			// Case 2: Lock has expired (previous processor crashed or finished).
			DE("lock.until", DE("$lt", now)),
			// Case 3: We already own the lock (re-entrant processing).
			D(
				E("lock.until", DE("$gt", now)), // Lock is still valid.
				E("lock.owner", lockOwner),      // AND we own it.
			),
		)),
	)

	// ATOMIC UPDATE: If the filter matches, atomically set the lock and clear events
	// The use of FindOneAndUpdate ensures this is atomic - either we get the lock
	// and the events, or another instance does. No race conditions.
	//
	// We need to use a pipeline to conditionally set the lock based on whether events exist.
	update := A(DE("$set", D(
		// Set lock expiration using conditional logic:
		// - If events exist after clearing: extend the lock.
		// - If no events exist: clear the lock (set to zero time).
		E("lock.until", hasEventsSwitch(lockUntil, time.Time{})),
		E("lock.owner", hasEventsSwitch(lockOwner, "")),
		// This implementation consumes events right away, clearing them from the document.
		// It is ok to set an empty array here, since we will either consume the events
		// or they are already empty.
		E("events", A()),
	)))

	// CRITICAL: We must use options.Before here because the update clears the events array.
	// We need to process the events as they existed before the update; otherwise, we would
	// see an empty array and lose the events.
	updateOpts := options.FindOneAndUpdate().SetReturnDocument(options.Before)

	var state State
	err := q.collection.FindOneAndUpdate(context.Background(), filter, update, updateOpts).Decode(&state)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// This is NORMAL - it means another instance already has the lock
			// or there are no events to process.
			return nil, nil
		}
		return nil, err
	}

	// SUCCESS: We acquired the lock and got the events to process.
	// Update the returned state to reflect the new lock ownership.
	state.Lock.Until = lockUntil
	state.Lock.Owner = lockOwner

	return &state, nil
}

// hasEventsSwitch helper function that returns different values based on whether events exist.
// This implements conditional logic in MongoDB aggregation pipeline:
// - If events array has items: return trueValue (maintain lock).
// - If events array is empty: return falseValue (release lock).
// This is used to automatically release locks when no more work is available.
func hasEventsSwitch[T any](trueValue, falseValue T) bson.D {
	return DE("$cond", D(
		E("if", DE("$gt", A(DE("$size", "$events"), 0))), // if events.length > 0.
		E("then", trueValue),                             // keep the lock/owner.
		E("else", falseValue),                            // release the lock/owner.
	))
}

// updateLocked updates the locked state and checks for more events.
// This is called after processing events to:
// 1. Save the updated state (e.g., new counter value).
// 2. Check if new events arrived while we were processing.
// 3. Either continue processing or release the lock.
func (q *CounterQueue) updateLocked(state *State, lockOwner string) (*State, error) {
	now := time.Now()
	lockUntil := now.Add(q.lockDuration)

	// SECURITY: Ensure we still own the lock before updating.
	// This prevents race conditions where the lock expired or was taken by another instance.
	filter := D(
		E("_id", state.ID),              // Exact document.
		E("lock.owner", lockOwner),      // We still own it.
		E("lock.until", DE("$gt", now)), // Lock is still valid.
	)

	// Update the state and use conditional logic for lock management.
	// We must use pipeline updates to conditionally set the lock based on whether new events exist.
	update := A(DE("$set", D(
		E("counter", state.Counter), // Save the updated counter.
		E("updated_at", now),        // Update timestamp.

		// CONDITIONAL LOCK RENEWAL:
		// - If new events arrived: extend the lock and continue processing.
		// - If no new events: release the lock (set to zero time).
		E("lock.until", hasEventsSwitch(lockUntil, time.Time{})),
		E("lock.owner", hasEventsSwitch(lockOwner, "")),

		// Clear events array - we'll get new events in the return if they exist.
		E("events", A()),
	)))

	updateOpts := options.FindOneAndUpdate().SetReturnDocument(options.Before)

	var beforeState State
	err := q.collection.FindOneAndUpdate(context.Background(), filter, update, updateOpts).Decode(&beforeState)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// NORMAL: We lost the lock (expired, or taken by another instance).
			// This can happen if processing took too long.
			return nil, nil
		}
		return nil, err
	}

	// Check if new events arrived while we were processing.
	if len(beforeState.Events) > 0 {
		// MORE WORK: New events arrived, continue processing with renewed lock.
		result := *state
		result.Events = beforeState.Events // New events to process.
		result.Lock.Until = lockUntil      // Renewed lock.
		result.Lock.Owner = lockOwner
		return &result, nil
	}

	// NO MORE WORK: No new events, lock was automatically released.
	return nil, nil
}

// CounterRunner implements actornot.LockedEntityRunner.
type CounterRunner struct{ log *slog.Logger }

func NewCounterRunner(log *slog.Logger) *CounterRunner { return &CounterRunner{log: log} }

func (r *CounterRunner) Runnable(state *State) bool { return state != nil && len(state.Events) > 0 }

// RunLocked processes all events for a single entity (user) while holding the distributed lock.
//
// This method is the *only* place in the entire cloud of functions/servers where the state for this entity
// is actually mutated and business logic is applied. The distributed locking mechanism ensures that,
// at any given time, only one instance of RunLocked is executing for a particular user (state),
// even if there are many servers or goroutines running in parallel. This provides strong sequential
// processing guarantees and prevents race conditions or double-processing.
//
// In summary: RunLocked is the critical section for entity processing. All state changes and event
// handling for a user must happen here, and nowhere else.
func (r *CounterRunner) RunLocked(state *State) {
	// IMPORTANT: Clear processed events from memory.
	// The actual database events are cleared by the MongoDB update operations.
	// We do not need to do that if events are NOT stored in the state.
	events := state.Events
	state.Events = nil

	log := r.log.With(slog.String("user_id", state.UserID))

	log.Info("Processing events",
		slog.Int("event_count", len(events)),
		slog.Int64("counter", state.Counter),
	)

	// BATCH PROCESSING: Process all events in the current batch.
	// This is more efficient than processing one event at a time and
	// reduces the number of database round trips.
	totalDelta := int64(0)
	for _, event := range events {
		// We have only only type of event - incrementing the counter.
		// Otherwise we would dispatch different handlers based on event type.
		totalDelta += event.Delta
	}

	// Update the state in memory - this will be persisted by updateLocked().
	state.Counter += totalDelta

	log.Info("Updated counter",
		slog.Int64("delta", totalDelta),
		slog.Int64("counter", state.Counter),
	)
}

func must[T any](value T, err error) T { must0(err); return value }

func must0(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://localhost:27017"
	}

	flag.StringVar(&mongoURI, "mongo-uri", mongoURI, "MongoDB connection URI. MONGO_URI env var is used by default.")

	flag.Parse()

	log := slog.Default()

	client := must(mongo.Connect(options.Client().
		ApplyURI(mongoURI).
		SetBSONOptions(&options.BSONOptions{
			UseJSONStructTags: true,
			OmitZeroStruct:    true,
			NilSliceAsEmpty:   true,
			NilMapAsEmpty:     true,
		}),
	))
	defer client.Disconnect(context.Background())

	must0(client.Ping(context.Background(), nil))

	log.Info("Connected to MongoDB")

	db := client.Database("actornot_example")
	collection := db.Collection("states")

	// Create indexes for better performance.
	must(collection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{Keys: DE("user_id", 1)},
		{Keys: DE("lock.until", 1)},
	}))

	queue := NewCounterQueue(collection)
	runner := NewCounterRunner(log)
	scheduler := actornot.NewConcurrentScheduler()

	mailbox := actornot.NewMailboxPoster(queue, runner, scheduler, log)
	defer mailbox.Close()

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		envelope := &CounterEnvelope{
			UserID:   r.URL.Query().Get("user_id"),
			LockName: "webhook-" + uuid.NewString(),
			Event: Event{
				CreatedAt: time.Now(),
				Delta:     must(strconv.ParseInt(r.URL.Query().Get("delta"), 10, 64)),
			},
		}
		must0(mailbox.PostMessage(envelope))
		fmt.Fprintf(w, "Posted increment of %d for user %s\n", envelope.Event.Delta, envelope.UserID)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			cursor := must(collection.Find(r.Context(), bson.D{}))
			var states []State
			must0(cursor.All(r.Context(), &states))
			must0(json.NewEncoder(w).Encode(states))
			return
		}

		var state State
		must0(collection.FindOne(r.Context(), DE("user_id", userID)).Decode(&state))
		must0(json.NewEncoder(w).Encode(state))
	})

	log.Info("Starting HTTP server on :8080")
	log.Info("Try: curl 'http://localhost:8080/webhook?user_id=alice&delta=5'")
	log.Info("Status: curl 'http://localhost:8080/status?user_id=alice'")
	log.Info("All users: curl 'http://localhost:8080/status'")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Error("HTTP server failed", "error", err)
		os.Exit(1)
	}
}
