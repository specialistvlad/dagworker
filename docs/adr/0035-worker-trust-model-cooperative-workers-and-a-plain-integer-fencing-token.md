# ADR-0035: Worker trust model: cooperative workers and a plain integer fencing token

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md Open Questions §1; docs/research/06-memcached-and-storage-abstraction.md (Temporal `RangeID`, §B.2, Open Questions); docs/research/00-synthesis.md §11 Open Question 8; owner decision

## Context

ADR-0006/ADR-0007's fencing design makes no claim about defending against a worker that is
*malicious* rather than merely slow, paused, or crashed — those are different threat models with
different, sometimes conflicting, cost/benefit trade-offs, and the research explicitly leaves the
choice open rather than picking one. Dossier 03's own Open Questions name it directly: "if untrusted
or third-party-hosted workers are ever a target deployment shape, an opaque signed/HMAC'd token
(bundling node ID + epoch + scope) may be warranted so a worker cannot simply guess or forge a higher
epoch — this trades simplicity for a security property the current brief does not clearly require;
needs a decision on the library's trust model for workers." Dossier 06 raises the identical fork
independently from the storage-port side, asking whether `ReadyQueue.ClaimReady` needs "an explicit
`ClaimToken`-embedded fencing counter" hardened against forgery, or whether that can be deferred.
Both dossiers converge on the same unresolved question and both explicitly decline to answer it,
deferring to "a decision on the library's trust model for workers" that only the project owner can
make, since it depends entirely on the target deployment shape rather than on anything the research
itself can settle.

The owner now resolves this: **workers are cooperative, operated by the same team that operates the
library instances.** This is not a new invention for this project — it is the identical trust
boundary every job-queue system surveyed in this research assumes by default. Faktory, Asynq, River,
and Sidekiq none of them sign their task tokens against a hostile worker; a plain `FETCH`/`RPOPLPUSH`-
returned identifier is trusted at face value. It is also, explicitly, Temporal's own production
model: Temporal's history shards track an in-memory `RangeID`, "a monotonically increasing generation
number used for fencing... ensuring only one instance can write to a shard," checked on every
persistence call — durable, monotonic, and verified by comparison, but never cryptographically
signed, because Temporal's own shard-owning processes are mutually trusted infrastructure, not
adversarial parties (06, Temporal `RangeID`). dag-worker-go adopts that same shape deliberately rather
than leaving it implicit.

## Decision

The fencing token (`LeaseEpoch`, ADR-0006) is a plain, unsigned, monotonic `uint64` — no cryptographic
protection, no server-side signing secret, no HMAC — handed to the worker in the clear as part of the
`Claim` response. This is Temporal's own `RangeID` task-token model, adopted deliberately, not a gap
left unnoticed.

**Threat model, stated plainly.** A malicious or buggy worker that has already received at least one
legitimate claim in a scope:

- **CAN** forge or guess a *higher* epoch than the one it was actually issued, and have a
  `Complete`/`Extend` call carrying that forged epoch succeed *if it happens to match the node's true
  current epoch* — there is no signature to detect that the value was not derived from an actual claim
  response. In practice this means a worker able to observe or guess another worker's live epoch for a
  node it does not itself hold can steal that node's completion.
- **CAN** replay an old, previously valid `Ack`/`Nack` request — but this is caught and rejected by the
  *same* fencing CAS (ADR-0006) as any other stale write, since a replayed request carries the
  original epoch, which is by definition no longer current once any reclaim has occurred; replay buys
  the attacker nothing beyond what an honest stale ack already gets, which is rejection.

A malicious or buggy worker **CANNOT**, under this model:

- **Corrupt the DAG's graph structure.** Edges, predecessor counts, and scope membership are never
  derived from anything a worker presents; a forged epoch can only ever affect the single node it
  targets, in the two ways named above, and its blast radius is fully contained by the fencing CAS —
  it either matches the node's true current epoch (indistinguishable from a legitimate claimant's own
  action) or it does not (rejected identically to any other stale write).
- **Cross a scope boundary.** `ClaimToken`s are scoped, and the storage port's key-prefix/partition
  discipline (ADR-0023) means a token meaningful in one scope carries no syntactic or semantic weight
  in another — there is no forgery that grants cross-scope access.
- **Exceed the configured payload cap.** Payload size is enforced at write time by the library/backend
  independent of anything a worker's claim response contains.

**Why this is acceptable for the target deployment.** dag-worker-go is a library embedded in a host
process, and its workers are, by the project's own stated shape, processes the *same operator*
deploys and controls. Guarding against a worker actively trying to steal another worker's node
defends against an adversary this deployment shape does not have. Spending complexity on it now — a
server-side signing key, key rotation, HMAC verification on every hot-path write — buys no real
protection for the stated use case, and it is not free: the fencing CAS on ADR-0006/ADR-0007's hottest
path would grow a signature-verification step on every single `Claim`/`Complete`/`Extend` call.

**What would change if untrusted workers were later required.** `ClaimToken`'s wire representation
would carry an **HMAC over `(scope, NodeID, epoch, deadline)`**, computed with a server-side key held
only by the library instances and never given to a worker. A worker would present the full signed
token, and every `Complete`/`Extend`/`Sweep` write would verify the signature before trusting the
epoch it decodes — closing exactly the "forge a higher epoch" gap named above, since a worker without
the key cannot construct a valid token for an epoch it was never actually issued. Key rotation would
follow the standard dual-key-window pattern: accept signatures produced by the current *or*
immediately-prior key during a rotation window, sign only with the current key, so in-flight leases
survive a rotation without a coordinated flag-day.

**Why this stays a backend change, not a wire break, today.** `ClaimToken` is already defined as an
opaque `interface{ String() string }` type at the storage port (ADR-0016, the Vitess-`topo.Version`-
derived shape), never a concrete `uint64` in the public API. A backend is free to encode an
HMAC-wrapped string inside that opaque value instead of a bare integer string without changing
`dagstore.Store`'s method signatures, without changing `dagworker.Claim`'s public shape, and without
requiring a `/v2` module boundary (ADR-0031). Callers already treat the token as an opaque value to be
round-tripped, never parsed — the discipline this ADR relies on is enforced by ADR-0016's own type
choice, made originally for an unrelated reason (backend representation independence across
Postgres/Redis/in-memory) that happens to also buy this upgrade path for free.

## Consequences

### Positive

- Zero added latency or complexity on the hot path for the deployment shape this project actually
  targets; matches every job-queue precedent surveyed (Faktory, Asynq, River, Sidekiq, Temporal).
- The upgrade path to signed tokens is fully specified now, so it is a scheduled, well-understood
  backend change later rather than a surprise `/v2` migration if the deployment shape ever changes.
- Keeps this project's general bias against speculative complexity consistent (mirrored in ADR-0012's
  rejection of an unrequested retry budget): a security property genuinely not required by the stated
  brief is not built in advance of need.

### Negative

- The library ships with a documented security limitation that a security-conscious adopter must read
  and accept before deploying with third-party or otherwise adversarial workers.
- "Cooperative workers" is a deployment *assumption* the library cannot verify or enforce at runtime —
  a misconfigured deployment that does expose claims to untrusted parties gets no warning from the
  library itself; this must be prominent in the top-level package documentation, not buried.

### Neutral

- This decision is silent on transport-layer security (TLS between workers and any network adapter) —
  that is a separate, orthogonal concern belonging to the optional gRPC/HTTP adapters, not to the core
  fencing mechanism this ADR governs.

## Alternatives considered

**HMAC-signed token from day one.** Rejected for v1: dossier 03's own Open Question frames this as
trading simplicity for "a security property the current brief does not clearly require"; the owner's
cooperative-worker decision removes the requirement outright rather than leaving it speculative, and
building it unused would contradict this project's general posture toward unrequested complexity.

**Full mutual-TLS with per-worker identity and authorization layered on top of the fencing token.**
Rejected as out of scope for this ADR: this is an authentication/authorization concern belonging to
the optional network adapters (gRPC/HTTP), not to the core fencing mechanism — conflating the two
would make the core module's trust model depend on a network layer it must otherwise have zero import
edge to.

**Capability-scoped tokens** (a worker can only ever claim nodes of a `Kind` it was pre-authorized
for). Rejected: this is an authorization feature orthogonal to fencing — it constrains *what* a worker
may claim, not whether a claimed node's completion is safe from a stale or forged write — and is not
required by the stated cooperative-worker trust model. Nothing in this ADR forecloses adding it
independently later, since it does not touch `ClaimToken`'s shape at all.

## References

- Temporal history-service architecture (`RangeID` fencing) — https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md
- Temporal shard-ownership assertion issue #3135 — https://github.com/temporalio/temporal/issues/3135
- Vitess `topo.Version`/`topo.Conn` opaque-version pattern — https://github.com/vitessio/vitess/blob/main/go/vt/topo/conn.go
- docs/research/03-leases-heartbeats-timeouts.md Open Questions §1
- docs/research/06-memcached-and-storage-abstraction.md (Temporal `RangeID` section, Recommendation 8, Open Questions)
- docs/research/00-synthesis.md §11 Open Question 8
