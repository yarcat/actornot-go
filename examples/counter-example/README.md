# Counter Example - MongoDB Actor Pattern

This tutorial demonstrates a simple, robust way to process per-user events using MongoDB and the actor pattern in Go. You’ll learn how to:

- Model per-user state and event queues.
- Use MongoDB for distributed locking.
- Process events sequentially per user, safely and scalably.

## Overview

We implement a counter system where:
- Each user has a counter.
- Increments are processed as events.
- MongoDB ensures only one instance processes a user’s events at a time.

## Key Concepts & Implementation

### 1. Envelope: Representing Events

Each event (e.g., increment) is wrapped in an envelope.

```go
type CounterEnvelope struct {
    UserID string // User identifier.
    Delta  int64  // Increment amount.
}
```

### 2. State: Per-User Data

```go
type State struct {
    ID      bson.ObjectID // MongoDB document ID.
    UserID  string        // User identifier.
    Counter int64         // The counter value.
    Events  []Event       // Queue of pending increment events.
    Lock    StateLock     // Distributed lock info.
}
```

The `Events` field is an optimization. Instead of updating the counter immediately for every request, events are queued and processed in batches. This reduces write contention and allows efficient, atomic updates. By processing multiple events at once, the system minimizes the number of database writes and lock operations.

### 3. Runner: Processing Events

The runner acquires a lock, processes all pending events for a user, updates the counter, and releases the lock. Only one runner per user can hold the lock at a time. **You only implement the logic for processing events and acquiring the lock; the mailbox calls `UpdateLocked` for you automatically after each processing step.**

```go
func processEvents(state *State) {
    for len(state.Events) > 0 {
        event := state.Events[0]
        state.Counter += event.Delta
        state.Events = state.Events[1:]
    }
    // Save state and release lock.
}
```

The logic for processing events is typically implemented in a `RunLocked` or similar function. Dropping events from the queue after processing is an important optimization. It ensures that processed events do not accumulate, which keeps the event queue small and efficient. However, there are options for how and when to remove events. For example, you might choose to remove events only after confirming they were actually executed and persisted. This approach is safer, as it avoids losing events in case of a crash during processing, but may require more complex logic and potentially slower performance. The choice depends on your consistency and reliability requirements.

### 4. Locking: Distributed Safety

Locks are managed in MongoDB using atomic updates and expiration times. This ensures only one process handles a user’s events, even across multiple servers. The locking mechanism uses MongoDB's `$cond` operator within update pipelines to atomically acquire or release locks based on conditions. This approach prevents race conditions and ensures that lock acquisition and event queue updates are performed in a single, atomic operation.

## Running the Example

### Prerequisites
- MongoDB running locally (default port 27017) or set `MONGO_URI`.
- Go 1.22+.

### Start the server

```bash
cd examples/counter-example
go run main.go
```

### Try it out with curl

#### Increment a counter
```bash
curl 'http://localhost:8080/webhook?user_id=alice&delta=5'
curl 'http://localhost:8080/webhook?user_id=bob&delta=3'
```

#### Check status
```bash
curl 'http://localhost:8080/status?user_id=alice'
curl 'http://localhost:8080/status'
```

## How It Works

1. **Event Enqueueing**: HTTP requests create `CounterEnvelope` messages, enqueued for the user. Events are appended to the `Events` array in the user's state document.
2. **Atomic Operations**: MongoDB ensures events are added and locks are acquired atomically. Update pipelines with `$cond` allow conditional logic directly in the database, reducing the risk of race conditions.
3. **Processing Loop**: The runner processes all pending events for a user, updates the counter, and checks for new events. By processing events in batches, the system reduces the number of lock acquisitions and database writes. Dropping processed events is an optimization, but you can also implement safer strategies to remove events only after successful execution.
4. **Distributed Safety**: Multiple server instances can run. MongoDB ensures only one processes events for each user at a time by using distributed locks with expiration. If a process crashes, the lock expires and another process can take over.

## Benefits
- **Consistency**: Events for each user are processed sequentially.
- **Scalability**: Different users can be processed in parallel.
- **Fault Tolerance**: Locks expire automatically if a process crashes.
- **Efficiency**: Batching events and using atomic update pipelines reduces database load and increases throughput.

This pattern is ideal for systems needing per-entity consistency and horizontal scalability. All optimizations and tricks, such as event batching, dropping processed events, and atomic conditional updates with MongoDB pipelines, are designed to maximize safety and performance.
