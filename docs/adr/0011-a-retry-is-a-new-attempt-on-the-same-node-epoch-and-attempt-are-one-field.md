# ADR-0011: A retry is a new attempt on the same node; epoch and attempt are one field

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §3.4; docs/research/03-leases-heartbeats-timeouts.md §3.4 (Kleppmann)

## Context

River's own documented failure mode names the exact race this ADR closes: a `running` job whose
engine cannot update its state "will be left as `running` and will require a pass by the job rescuer
service" (12 §3.4). Generalized, the danger case is a worker from **attempt N** finally responding
with a very slow `Ack` **after** the sweeper has already declared attempt N timed out and dispatched
attempt N+1 to a different worker. Without a fencing check keyed on which attempt is being
acknowledged, that late `Ack` "could incorrectly mark the node `Success` after a second worker is
already redoing the work (or worse, both attempts write conflicting results)" (12 §3.4) — this is
Kleppmann's GC-pause scenario (03 §3.4), restated in this project's own vocabulary. No system surveyed
in this project's research — Temporal, Argo, River, or Step Functions — allows a completed unit of
work to un-complete; "retry" always means a **new attempt** on a node that is not yet terminal, never
a mutation of an already-terminal one (12 §3.4).

## Decision

`attempt`/`LeaseEpoch` (ADR-0006) is **one field doing double duty**: it is both (a) the retry-count
the backoff/max-attempts policy reads, and (b) the fencing token the `Complete`/`Extend`/`Sweep` CAS
keys on (ADR-0007). No second lease-ID or independent attempt counter is introduced anywhere in the
storage layer.

- The public `Outcome.Attempt uint32` is 1-based and equals the epoch the worker was handed as
  `Claim.LeaseEpoch` at the moment it took that attempt.
- Every retry (transition T9: `Nack` and `attempt < maxAttempts`) re-enters the ready-set as
  `New/Ready` — delayed by backoff (ADR-0012) — on the **same `NodeID`**. It is never assigned a new
  node identity, and the retry itself touches neither the node's payload nor its edges.
- The fenced CAS compares the presented epoch against the node's **current** epoch, not merely
  whether `status == in_progress`:

  ```go
  // Storage-layer shape (ADR-0007). A 0-row result means either the node
  // already moved on (attempt N+1 already dispatched) or this call is a
  // duplicate — both collapse to the identical caller-visible error.
  //   UPDATE nodes SET status = $new, outcome = $outcome
  //   WHERE id = $1 AND status = 'in_progress' AND lease_epoch = $2
  ```

  A worker is deliberately given no way to distinguish "I was too slow" from "someone else already
  handled this" — per 12 §3.4, that is "the correct level of ignorance for a worker to have," since
  either case leads to the identical correct action (stop) and telling them apart would invite a
  worker to build retry logic on a distinction that carries no operational meaning.
- `maxAttempts` is a per-node, per-claim-time setting carried alongside the backoff parameters
  (ADR-0012). Exhausting it terminates the node `Error` with an `Outcome.Reason` naming the cause
  (`ReasonWorkerError` or `ReasonTimeout`) — never a silent drop.

## Consequences

### Positive

- No separate lease-UUID scheme is needed anywhere in the storage layer: attempt numbers are already
  monotonic per node by construction (bumped on every claim, ADR-0006) and already durable, since
  they are part of the very row a retry mutates.
- A dashboard's "this node has failed N times" figure and the fencing epoch are the same number — no
  join or derived computation needed to display retry history.
- The fenced CAS predicate is a single, uniform shape (`status` + one epoch compare) reused
  identically by `Complete`, `Extend`, and `Sweep` — one thing to get right instead of two.

### Negative

- A node's max-attempts ceiling and its fencing-epoch space share one namespace. This is not a
  practical concern at any realistic `maxAttempts` value (the field is a `uint64`, ADR-0006), but it
  is worth documenting explicitly so a future contributor is never tempted to "reset" the epoch on
  retry to keep attempt numbers small — doing so would silently reopen the exact stale-write race this
  ADR exists to close.
- Retry history beyond the current attempt number (e.g., "what were the prior attempts' error
  messages") is not carried by this field alone; it must come from the event stream (ADR-0019) or a
  host-side audit log, not from the node's current `Outcome`.

### Neutral

- This ADR fixes only *that* a retry is a new attempt on the same node and that attempt equals epoch
  — it does not itself specify the backoff delay before the retried node re-enters `Ready` (ADR-0012)
  or the fail-fast-vs-continue cascade policy for downstream nodes (a separate decision).

## Alternatives considered

**Separate `attempt` (business-facing retry count) and `LeaseEpoch` (fencing token) as two
independent fields.** Rejected: 12 §3.4 notes this buys nothing, since attempt numbers are already
monotonic per node by construction — two fields is two things to keep synchronized and two chances to
key a CAS predicate on the wrong one.

**Generate a new `NodeID` per retry attempt** (the shape Cloud Tasks' attempt-telemetry model gestures
toward, tracking attempts without a status enum at all). Rejected: this breaks the DAG's edge
structure, since successors depend on the **original** node ID, not a per-attempt one, and it makes a
caller's own idempotent-retry-after-timeout call impossible to express correctly — the same
node-identity argument ADR-0025 makes for why `NodeID` must be caller-supplied and stable.

**Distinguish "I was too slow" from "someone else already handled this" in the error a stale worker
receives.** Rejected: 12 §3.4 is explicit this is "the correct level of ignorance for a worker to
have" — in either case the worker's only correct action is to stop, and surfacing the distinction
would invite building retry or alerting logic on a difference that carries no actionable meaning.

## References

- docs/research/12-dag-semantics-and-state-machine.md §3.4
- docs/research/03-leases-heartbeats-timeouts.md §3.4
- River `rivertype` package (job rescuer / stuck-running failure mode) — https://pkg.go.dev/github.com/riverqueue/river/rivertype
- docs/research/00-synthesis.md ADR-11 seed
