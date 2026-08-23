# ADR-0009: Delivery guarantee is at-least-once delivery with at-most-once accepted effect

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §5e; docs/research/10-event-bus-and-delivery-semantics.md §2.1-2.3

## Context

Two independent impossibility results govern this decision, not engineering taste. Akkoyunlu,
Ekanadham, and Huber proved in 1975 that two parties communicating over a channel that can lose
messages can never both become *certain* the other received a given message — Jim Gray named this the
**Two Generals Problem** in 1978: "every ack needs an ack, forever" (10 §2.2). Fischer, Lynch, and
Paterson separately showed no deterministic algorithm can guarantee consensus in a fully asynchronous
system with even one crash-prone process. Neither result says reliable systems are impossible; both
say a *sender* cannot unilaterally *know* delivery happened exactly once by watching the wire alone —
the fix is to move the guarantee off the wire and onto idempotent state at the receiver (10 §2.2).

Every production message/queue system surveyed makes the identical choice. SQS is explicit that "the
at-least-once delivery model" means it "doesn't guarantee that a message won't be delivered more than
once within the visibility timeout period," stated as a property of the system, not a bug (03 §5e,
10 §2.1). Even Kafka's own celebrated "exactly-once semantics" is scoped precisely: the idempotent
producer deduplicates on `(PID, epoch, sequence number)` *inside Kafka's own log*, and transactions
make a consume-transform-produce loop atomic *within Kafka* — none of it extends to an arbitrary
external side effect a consumer performs after reading a record, "unless that side effect is itself
transactional against the same offset-commit" (10 §2.3). This matters more acutely for dag-worker-go
than for a typical message queue: SQS is usually the last hop before a genuinely idempotent, dedup-
capable downstream consumer, whereas dag-worker-go's storage **is** the authoritative state of the
DAG — a duplicated accepted write here is not a redundant side effect, it is a corrupted node status.

Concretely, in this design a `Claim` response can be lost on its way back to the worker (network
partition, worker restart between receiving and processing), and the library cannot distinguish "the
worker never received it" from "the worker received it and is quietly working." Favoring liveness, a
timed-out node is reissued to a second worker (ADR-0006/ADR-0007); if the first worker was in fact
alive and eventually tries to report a result, that report must be safely rejected, not silently
accepted and not silently dropped without a trace.

## Decision

Document and implement, precisely and only, this guarantee: **at-least-once delivery of "take this
node" to some worker eventually; at-most-once acceptance of a terminal outcome per node per lease
epoch.** The word "exactly-once," unqualified, must never appear in the library's documentation or
Go doc comments for `Claim`/`Ack`/`Nack`/`Complete`.

1. The library may hand the same node to more than one worker over its lifetime, via retry after
   timeout (ADR-0011). This is expected behavior, not a bug, and must be described that way in the
   public docs before an operator files the inevitable "why did my worker run twice" issue.
2. Exactly one `Complete` call per lease epoch can ever succeed (ADR-0006/ADR-0007's fenced CAS).
   Every other attempt against that epoch — a duplicate report from the same worker, or a stale
   worker's late report after a reclaim — is rejected, never silently accepted and never silently
   dropped: it must be surfaced as a distinct `LateAckRejected` event on the observation stream
   (ADR-0019), separate from ordinary `Error`/`Timeout` transitions, so operators can tell "the
   worker's task genuinely failed" apart from "the worker's environment paused or migrated past its
   lease" (03 §5f).
3. At-most-once delivery — zero retries after any timeout, treating an ambiguous failure as terminal
   — is available only as an explicit, opt-in, per-scope-or-per-node-kind policy. It is never the
   library default: silently dropping a DAG node on an ambiguous failure is a worse default for a
   workflow engine than occasionally handing the same node to two workers and relying on the fencing
   token (ADR-0006) to keep that safe.
4. Public API documentation for `Claim`, `Ack`, `Nack`, and `Extend` states this guarantee in these
   exact terms and explains that the fencing token, not the transport, is the mechanism that makes
   duplication safe.

## Consequences

### Positive

- Sets correct caller expectations *before* the first bug report, with documentation that matches
  every mature system's own (SQS, Pub/Sub, Kafka, Redis Streams) — operators arriving from any of
  those systems bring already-correct intuition.
- The guarantee is provable from ADR-0006/ADR-0007's mechanism, not an unenforceable promise resting
  on hope: any violation would be a bug in the fenced-CAS implementation, testable by the conformance
  suite (ADR-0018).
- A distinct `LateAckRejected` event gives operators actionable signal (e.g., "tune lease durations
  upward for infrastructure known to migrate VMs frequently") at zero correctness cost.

### Negative

- Hosts must write idempotent worker logic when a node's side effect is not naturally idempotent —
  a real integration burden the library cannot remove, only make safe to reason about.
- `LateAckRejected` is one more event kind every consumer of the observation stream must know to
  handle or explicitly ignore.

### Neutral

- This ADR is a documentation and behavioral contract, not a new mechanism in itself — it names and
  constrains behavior that ADR-0006 and ADR-0007 already implement.

## Alternatives considered

**Claim exactly-once semantics via a Kafka-transaction-style idempotency layer.** Rejected: Kafka's
own EOS only holds *inside its log* (10 §2.3); dag-worker-go must guarantee an effect on an arbitrary
external worker process, which is precisely the case the Two Generals and FLP results rule out
achieving unilaterally — no amount of internal transactionality closes a gap that is fundamentally
about an external, independently-crashable party.

**Silently drop a stale ack with no distinct event, folding it into ordinary logging.** Rejected: 03
§5f recommends surfacing `LateAckRejected` distinctly as valuable, actionable operational signal at
zero correctness cost — silently absorbing it discards information operators would want.

**Default to at-most-once (no retry) to avoid ever double-delivering.** Rejected: 03 §5e is explicit
this must be an opt-in the host makes deliberately, never a library default — silently abandoning DAG
nodes on ambiguous failure is a worse failure mode for a workflow engine than a safely-fenced
duplicate delivery, which this library already makes safe by construction (ADR-0006).

## References

- Akkoyunlu, Ekanadham, Huber, "Some Constraints and Tradeoffs in the Design of Network
  Communications" (1975), summarized via Grokipedia — https://grokipedia.com/page/Two_Generals'_Problem
- Fischer, Lynch, Paterson impossibility summary — https://team.inria.fr/antique/the-impossibility-result-of-fischer-lynch-and-paterson-and-its-proof/
- Amazon SQS visibility timeout docs — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Conduktor, "Exactly-once semantics in Kafka" — https://www.conduktor.io/glossary/exactly-once-semantics-in-kafka
- Strimzi, "Exactly-once semantics with Kafka transactions" — https://strimzi.io/blog/2023/05/03/kafka-transactions/
- Tyler Treat, "You Cannot Have Exactly-Once Delivery" — https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/
- docs/research/03-leases-heartbeats-timeouts.md §5e, §5f
- docs/research/10-event-bus-and-delivery-semantics.md §2.1-2.3
- docs/research/00-synthesis.md ADR-09 seed
