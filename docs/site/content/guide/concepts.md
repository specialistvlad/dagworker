---
title: "Concepts: nodes, scopes, status, leases"
description: The vocabulary the rest of the docs assume — nodes, scopes, the four-value status, and the lease protocol that makes concurrent workers safe.
---

Everything else in these docs assumes this vocabulary. The lease protocol in
particular is worth reading slowly: it is the mechanism that makes it safe to
run more than one worker against the same graph, and it is the one part of
the design where getting the mental model wrong leads to real data
corruption rather than a compile error.

## Nodes and scopes

A [`Node`](/dagworker/reference/contract/) is one unit of work, identified by
a `(Scope, NodeID)` pair. `NodeID` is supplied by you, never generated —
which matters the moment a caller wants to retry adding a node after a
timeout: the caller expresses "the same node" by reusing the ID, rather than
accidentally creating a second one. A node carries an opaque `Payload`
(`[]byte`, capped at 256 KiB by default), a `Kind` string that partitions the
ready set so a worker can claim only the kinds it can handle, a `Priority`
that orders the ready set, and a `Trigger` rule that decides when it becomes
claimable given its predecessors' outcomes.

A [`Scope`](/dagworker/reference/contract/) is a namespace — the unit of
isolation, configuration, completion, and retention, all at once. Scopes are
created implicitly on first write; there is no `CreateScope` call. **An edge
never crosses a scope boundary.** That restriction is what keeps cycle
checking, completion detection, and every complexity bound in this library
scope-local: the moment an edge could cross scopes, "is this graph done"
becomes a distributed termination-detection problem instead of a counter
you can read in O(1). If you need one causal chain across two logical
namespaces, either put both halves in one scope and use `Labels` to
distinguish them, or bridge the two scopes explicitly in your own code — a
terminal node's `Ack` in scope A triggers your program to call `AddNode` in
scope B.

## The four-value status, and why it is four

```go
StatusNew         // exists, has not completed an attempt successfully
StatusInProgress  // a worker holds a valid lease
StatusSuccess     // terminal, succeeded
StatusError       // terminal, did not succeed
```

That is the entire public vocabulary, and it does not grow. Every question
you might want a fifth status to answer — *why* did it fail, was it a
timeout, was it cancelled, was it skipped — is answered instead by a
separate, closed `Reason`:

```go
ReasonNone            // no significant outcome yet
ReasonWorkerError     // the worker acked failure (Nack)
ReasonTimeout         // the lease deadline elapsed with no ack
ReasonUpstreamFailed  // a predecessor failed and the trigger rule can no longer be satisfied
ReasonSkipped         // the trigger rule is provably unsatisfiable for a non-failure reason
ReasonCancelled       // Cancel or CancelScope
ReasonRemoved         // a predecessor was removed under CascadeFail
```

The split exists because `Status` and `Reason` answer different questions
that different code cares about. A subscriber deciding whether to run a
downstream step only ever needs `Status.Terminal()`. An operator paging
through a dashboard needs `Reason` to tell "the worker's code threw" apart
from "nobody heard from the worker in time" — and every production system
surveyed while designing this library converged on treating a timeout as an
error with a reason, never as its own status.

`Reason` is populated according to a fixed table — worth knowing because it
answers "what does `Reason` mean on a node that's still running":

| Status | Attempt | `Reason` means |
|---|---|---|
| `New` | 0 | `ReasonNone` |
| `New` | > 0 | why the **most recent attempt** failed (the node is awaiting retry) |
| `InProgress` | ≥ 1 | why the **previous** attempt failed, or `ReasonNone` on the first attempt |
| `Success` | ≥ 1 | `ReasonNone` |
| `Error` | ≥ 0 | why the node is **terminally** failed |

## The internal phase: why "ready" isn't a status

`StatusNew` covers two situations that feel very different from where you're
standing: a node still waiting on a predecessor, and a node sitting in the
ready set right now, available for any worker to claim. Internally the
scheduler tracks five phases —

```go
PhaseBlocked    // ≥1 unsatisfied predecessor            → Status New
PhaseScheduled  // deps satisfied, retry backoff pending  → Status New
PhaseReady      // claimable now                          → Status New
PhaseClaimed    // a worker holds a valid lease           → Status InProgress
PhaseDone       // terminal                                → Status Success | Error
```

— but `Phase` is deliberately not part of `Status`. The reason is concrete:
adding an edge can flip a node between `Blocked` and `Ready` with **no
worker and no caller action involved**. If that flip were visible as a
`Status` change, a subscriber watching the event stream would see a node go
`New → New` for no reason it can explain, which looks exactly like a bug in
either the library or its own code. `Phase` carries no stability promise
across versions, never appears on the event stream or in a wire format, and
is reachable only through [`Manager.Inspect`](/dagworker/guide/operations/)
— which exists specifically for the moment you need to answer "why is this
node stuck."

## Leases and the fencing epoch

This is the part of the design that makes concurrent workers safe, so it is
worth understanding precisely rather than by analogy.

**The problem a lease alone cannot solve.** Say a worker claims a node,
stalls — a GC pause, a suspended VM, a frozen container — for longer than
its lease's deadline. The library, correctly, reclaims the node and hands it
to a second worker. Now the first worker wakes up, oblivious to any of this,
finishes its (redundant) work, and calls `Ack`. If nothing stops that write,
it can land *after* the second worker's own `Ack` and silently overwrite the
node's true final state. This is not a hypothetical: Kleppmann's critique of
distributed locking names exactly this scenario, and the conclusion every
system that has to be correct about it reaches independently — Chubby,
ZooKeeper, Kafka's idempotent producer, Temporal's `RangeID` — is the same:
**a lease is not safe unless every write it authorizes is checked against a
monotonic token at the moment of the write**, not merely at the moment the
lease was granted.

**How dagworker does it.** Every node carries a `uint64` lease epoch. Claim
is one atomic storage operation that:

1. selects an eligible node from the ready set, honoring priority then FIFO;
2. sets `Status = InProgress`;
3. **increments the epoch** and sets `Attempt` to the new epoch value — they
   are the same integer, which is why a retry count and a fencing token
   never disagree;
4. sets the deadline to the storage backend's own clock plus the lease
   timeout;
5. indexes that deadline so a sweep can find it later without scanning.

The worker gets that epoch back as part of the `Lease` it holds. Every
subsequent write against the node — `Ack`, `Nack`, `Extend` — must present
that exact epoch, and the backend performs the compare **inside the same
atomic operation as the mutation**, never as a preceding check a pause could
race. If the presented epoch does not match the node's current epoch, the
write is rejected with `ErrLeaseMismatch`, full stop. Zero rows affected on
Postgres, a failed re-check inside the Lua script on Redis, a failed
compare-under-mutex in memory — all three backends implement the identical
contract, verified by the same conformance suite.

```go
// Adapted from Example_leaseTimeout in the repository's own test suite.
abandoned, _ := m.TryClaim(ctx, "jobs")       // worker one
// ... worker one stalls past its lease deadline ...
recovered, _ := m.TryClaim(ctx, "jobs")       // worker two picks up the same node

// Worker one comes back and tries to report success. It is refused: its
// lease was superseded, and accepting it would silently overwrite worker
// two's result.
err := m.Ack(ctx, abandoned, nil)
errors.Is(err, dagworker.ErrLeaseMismatch) // true
```

`ErrLeaseMismatch` is never retryable, and the library never tells a caller
otherwise: by the time you see it, the work this lease represented may
already have been redone by whoever holds the epoch now. There is a second,
narrower error, `ErrLeaseExpired`, for the case where the epoch still
matches but the deadline itself has passed — distinct because the epoch
comparison and the deadline comparison answer different questions.

**`Extend` is a separate operation from `Ack`/`Nack`, on purpose.**
Conflating "I am still here" with "I am finished" is exactly the bug class
that bit early Kafka consumer groups, where a slow message handler starved
the heartbeat and triggered a spurious eviction. `Extend(ctx, lease, d)`
moves the deadline to `now + d`, fenced on the same epoch, and changes
nothing else — not `Status`, not `Attempt`, not the event sequence. See
[Writing workers](/dagworker/guide/workers/) for the heartbeat pattern that
uses it.

**The trust model this all rests on: workers are cooperative.** The fencing
token is a plain, unsigned integer, not a cryptographically signed one. A
worker that wanted to could forge a higher epoch and steal a node it never
legitimately held, or replay an old ack (which the same fencing check simply
rejects, buying the attacker nothing). What a malicious or buggy worker
*cannot* do is corrupt the graph's structure, cross a scope boundary, or
exceed the payload cap — none of that is derived from anything a worker
presents. This is a deliberate choice, not an oversight: it is the identical
trust boundary every job-queue system in this space assumes (Faktory, Asynq,
River, Sidekiq, and Temporal's own `RangeID`), and it holds as long as your
workers are operated by the people who operate your `Manager` instances. If
that stops being true — third-party or otherwise adversarial workers — this
is the first assumption to revisit before deploying.

## Events versus claiming

[`Manager.Subscribe`](/dagworker/reference/contract/) hands you a stream of
three kinds of notification:

- **`EventCreated`** — a node came into existence, always as its first
  event.
- **`EventTransition`** — a node's public `Status` changed. This is the
  observation feed.
- **`EventReady`** — a node became claimable. This is a doorbell, not a
  delivery.

The distinction that matters: **claiming never trusts the event stream.**
`EventReady` is coalescing, best-effort, and may be dropped entirely without
affecting correctness, because a claim always re-derives eligibility by
asking storage, never by trusting an accumulated set of "I heard this was
ready" events. If you find yourself keeping a set of node IDs learned from
`EventReady` and treating it as authoritative, that reintroduces exactly the
failure mode this split is designed to avoid. `EventTransition`, by
contrast, is your at-least-once, resumable observation feed on a backend
that supports the durable tier (`Durable: true` in `SubscribeOptions`,
resumed by `Cursor`) — but even it is deliberately not on the critical path
for correctness: a subscriber that misses a transition pays a latency cost
in noticing, never a correctness cost in what the graph actually does.

The fan-out point never blocks on a subscriber. A slow consumer either has
the oldest buffered event dropped for it (`OverflowDropOldest`, the default,
which sets `Event.Gap` on the next delivery so it knows truthfully that it
missed something) or has its subscription terminated
(`OverflowCloseSlow`) — there is deliberately no blocking policy, because
blocking would let one slow observer stall the scheduler for everyone else.

## Where this leads

Concepts in hand, the natural next reads are [Trigger
rules](/dagworker/guide/trigger-rules/) for how a node decides it's ready
given more than one predecessor, and [Dynamic
graphs](/dagworker/guide/dynamic-graphs/) for what "the graph is running
while you change it" actually means for cycle safety and completion
detection.
