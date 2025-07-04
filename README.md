# actornot-go

> Definitely not an actor. Just a very enthusiastic mailbox with commitment issues.

## What is this? (And why is it called "actornot")

**actornot-go** (from the Russian "недоактёр" — literally, "not quite an actor") is a Go library for building actor-like, distributed, and *serverless-friendly* systems. It's for when you want the coolness of the Actor Model, but your cloud is stateless, your database is your only friend, and your serverless functions keep ghosting you.

**TL;DR:**
- You want per-entity (user/session/chat) sequential processing.
- You want to scale horizontally, crash safely, and never lose a message.
- You want to do this in a stateless, serverless, or distributed environment where in-memory actors are a no-go.

## Why should you care?

Traditional actor systems (like Akka, Erlang, protoactor-go) are awesome... until you deploy them to serverless or distributed cloud, and suddenly your actors forget who they are every time the pod restarts. *Persistence is hard!* This library gives you actor-like behavior — but with all state, locks, and queues living in your database (MongoDB, Postgres, Redis, whatever). No more memory loss!

- **Serverless/Cloud Native**: Survives pod evictions, cold starts, and cloud drama.
- **Distributed**: Multiple instances? No problem. Only one processes a given entity at a time.
- **Crash-safe**: Locks expire, so if your function dies, another picks up the slack.
- **No message left behind**: Events are queued and processed in order, even if your infra is chaos.

## How does it work?

- **MailboxPoster**: The main star. Posts messages to a distributed queue, grabs a lock, and processes them one at a time — *per entity*. It also calls `UpdateLocked` for you automagically after each processing step, so you don't have to worry about updating or releasing locks yourself.
- **LockableQueue**: Abstracts your DB-backed queue and distributed lock. Plug in MongoDB, Postgres, Redis, etc.
- **Envelope**: Wraps your message with context (like user ID).
- **Runner**: You implement the logic for processing a locked entity (`RunLocked`), and how to acquire the lock, but the mailbox wires everything up and keeps the loop running for you.

All the hard stuff (locking, batching, retries, crash recovery) is handled for you. You just write your business logic.

## Example: Counter with MongoDB

Here's a taste from the [counter example](examples/counter-example):

```go
// Envelope for a counter increment event
 type CounterEnvelope struct {
     UserID string
     Delta  int64
 }

// State for each user
 type State struct {
     UserID  string
     Counter int64
     Events  []Event // Event queue
     Lock    struct {
         Until time.Time // Lock expiration
         Owner string    // Lock owner
     }
 }

// Runner: process all events for a user
 func processEvents(state *State) {
     for len(state.Events) > 0 {
         event := state.Events[0]
         state.Counter += event.Delta
         state.Events = state.Events[1:]
     }
     // Save state and release lock
 }
```

## Quickstart

1. **Run the example:**

```bash
cd examples/counter-example
MONGO_URI='mongodb://localhost:27017' go run main.go
```

2. **Try it out:**

```bash
curl 'http://localhost:8080/webhook?user_id=alice&delta=5'
curl 'http://localhost:8080/status?user_id=alice'
```

## Features
- ⚡ **Serverless-friendly**: Survives cold starts, pod evictions, and cloud chaos.
- 🔒 **Distributed locks**: Only one instance processes an entity at a time.
- 📨 **Event queue**: No message left behind. Events are processed in order.
- 🧑‍💻 **Pluggable backend**: Use MongoDB, Postgres, Redis, or anything you can lock.
- 💥 **Crash-safe**: Locks expire, so work always resumes.
- 🦾 **Horizontal scaling**: Add more instances, process more entities in parallel.
- 😎 **Modern Go**: Generics, interfaces, and a sprinkle of existential dread.

## The Name: "недоактёр" (actornot)

In Russian, "недоактёр" means "not quite an actor" — and that's exactly what this is. It's not a full-blown actor system, but it's got enough actor energy to get you through the distributed trenches. Like an actor who never made it to Hollywood, but still gets the job done in your serverless backend.

## Contributing

PRs, issues, memes, and existential questions welcome. If you make this library do something wild, let us know!

---

*actornot-go: For when you want actor vibes, but your cloud is commitment-phobic.*
