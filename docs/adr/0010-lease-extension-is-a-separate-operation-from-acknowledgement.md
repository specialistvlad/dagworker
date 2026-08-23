# ADR-0010: Lease extension is a separate operation from acknowledgement

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §2.2-2.3, §5c

## Context

Kafka's consumer-group liveness model is a cautionary case study this design must not repeat. Before
client version 0.10.2, heartbeats were piggybacked on `poll()` calls, so a slow message handler
silently starved the heartbeat thread and triggered `session.timeout.ms`-based eviction even though
the process was alive and (slowly) working. Kafka's fix was to split the concerns into two separate
timers: `session.timeout.ms` (a dedicated heartbeat thread proving the process is alive) and
`max.poll.interval.ms` (a separate, much longer timer proving the process is still making progress
through its work loop) (03 §2.3). The lesson generalizes directly: "is the worker alive" and "is this
specific node's work progressing" must never be conflated into one signal or one RPC shape, because
conflating them is exactly what produced Kafka's original bug class.

Google Cloud Pub/Sub supplies the positive precedent for the API shape. Its high-level client
libraries implement **automatic lease management**: rather than requiring the application to
hand-roll a heartbeat loop, the library itself tracks outstanding unacked messages and periodically
extends their deadline on the caller's behalf, sized from the 99th percentile of observed ack latency
rather than one fixed guess (03 §2.2). Both SQS's `ChangeMessageVisibility` and Pub/Sub's
`ModifyAckDeadline` compute the new deadline **from the moment the extension call is received**, not
additively from whatever deadline was previously set — "the 10 seconds begin to count from the time
that you make the [call]." SQS's own documented gotcha is that teams assume an extension "sticks" as
the new default for that message and are surprised when a later redelivery reverts to the queue's
original short timeout — an ambiguity this project should not import (03 §2.1). SQS additionally
caps total extension at a **hard 12-hour ceiling from first receipt**, "not extendable... regardless
of how many `ChangeMessageVisibility` calls are made" — a deliberate bound on how long one stuck
worker can wall off a unit of work from the rest of the system (03 §2.1).

## Decision

1. `Extend(ctx context.Context, token ClaimToken, newDuration time.Duration) (deadline time.Time, err
   error)` is a call distinct from `Complete` (which serves both `Ack` and `Nack`, ADR-0007) — never
   the same endpoint, never a sentinel result on the completion call.
2. `Extend`'s new deadline is `server_now() + newDuration` — an **absolute reset**, computed from the
   storage backend's own clock (ADR-0008) at the moment the call is processed, never additive to
   whatever deadline was previously set.
3. `Extend` is fenced identically to `Complete` (ADR-0006): a stale presented epoch is rejected
   outright with an unambiguous "you no longer own this node" error — never a silent no-op, and never
   a false "success" against a lease that has already moved on to a new epoch.
4. The worker-facing SDK owns the renewal loop **by default**: a claimed-node handle runs a background
   goroutine that calls `Extend` at a safe fraction of the remaining lease, stopping only when the
   host calls `Ack`/`Nack` or the process is shutting down (tracked under the `Manager.Close`
   goroutine-accounting contract, ADR-0027). Hand-written host-side heartbeat timers remain possible
   but are never required.
5. A configurable **`MaxLeaseLifetime`**, settable per scope (ADR-0023), bounds total lease lifetime
   independent of how many `Extend` calls succeed — mirroring SQS's 12-hour absolute ceiling — so one
   pathologically long-lived or silently wedged-but-still-heartbeating worker cannot hold a node
   forever.
6. `Extend` and `Complete` stay two distinct call shapes even though the library tracks only one timer
   per node today (there is no separate worker-process-liveness concept). Keeping them separate now
   avoids ever needing to overload one endpoint if a future version adds process-level liveness on top
   of node-level leasing.

## Consequences

### Positive

- Closes the Kafka liveness-vs-progress conflation bug class before it can occur, by construction of
  the API rather than by convention.
- An SDK-owned heartbeat loop removes an entire category of "the host forgot to heartbeat and its
  honestly-working node got timed out from under it" bug reports before they can be filed.
- Reset-not-additive semantics keep the mental model simple across repeated extends and across
  redeliveries after a retry (ADR-0011) — there is never a "does my extension stick for next time"
  question to answer.

### Negative

- The SDK carries a background goroutine per outstanding claim that must be tracked and shut down
  cleanly; this is real complexity the `Manager.Close` contract (ADR-0027) must account for.
- `MaxLeaseLifetime` as an independent hard ceiling means a legitimately long-running worker cannot be
  extended past it without a scope-level configuration change — a deliberate trade-off, not an
  oversight.

### Neutral

- `Extend`'s low-stakes, idempotent-ish nature (a redundant or slightly late call is harmless) is a
  deliberate asymmetry with `Complete`'s high-stakes, exactly-fenced-once nature (ADR-0009) — the two
  calls are meant to feel different to implement and to call, not accidentally similar.

## Alternatives considered

**Overload `Ack`/`Nack` with an "in-progress, more time please" sentinel result.** Rejected: this is
precisely Kafka's pre-0.10.2 mistake generalized (03 §2.3) — conflating a liveness/scheduling signal
with a terminal, fenced, high-stakes one makes the two calls' retry and idempotency semantics
impossible to reason about independently.

**Additive extension (`deadline += newDuration`) instead of an absolute reset.** Rejected: SQS's own
documentation flags the additive interpretation as a source of real confusion, since an extension
does not "stick" across a later redelivery (03 §2.1) — an absolute reset from call-time is unambiguous
regardless of how many times `Extend` is called or how a node's history unfolds.

**Host-managed heartbeat only, with no SDK-owned loop.** Rejected as the default: Pub/Sub's own
automatic lease management is the better-designed precedent (03 §2.2); requiring every host program to
hand-roll a correctly-timed extension loop is an ergonomic tax and an easy-to-forget footgun this
library can eliminate entirely by owning the loop in the SDK.

## References

- Amazon SQS visibility timeout docs (`ChangeMessageVisibility`, 12-hour ceiling) — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Google Cloud Pub/Sub lease management — https://docs.cloud.google.com/pubsub/docs/lease-management
- `modifyAckDeadline` reference — https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/modifyAckDeadline
- Confluent, Kafka rebalancing explainer (`session.timeout.ms` vs `max.poll.interval.ms`) — https://www.confluent.io/learn/kafka-rebalancing/
- Kafka consumer configs — https://kafka.apache.org/41/configuration/consumer-configs/
- docs/research/03-leases-heartbeats-timeouts.md §2.2-2.3, §5c
- docs/research/00-synthesis.md ADR-10 seed
