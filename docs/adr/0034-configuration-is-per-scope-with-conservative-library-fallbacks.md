# ADR-0034: Configuration is per-scope with conservative library fallbacks

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §4.1, §5.3, §6.3;
  docs/research/07-work-distribution-across-instances.md §1.1, §7.1 rec 6; docs/research/03-leases-heartbeats-timeouts.md
  §5f; docs/research/12-dag-semantics-and-state-machine.md §3.3

## Context

AMD-6 is explicit: the project owner rejected opinionated global defaults for retention,
concurrency limits, and partition sizing. The synthesis's own numbered decisions (retention TTL,
`MaxConcurrentInProgress`, virtual partition count `P`) each cite a single library-wide number
lifted from one reference system — River's 24h retention, a `P = 32-64` recommendation sized for
"a handful to a few dozen processes." Those numbers are reasonable for a job-queue-shaped
deployment (many small, short-lived scopes, high scope churn, modest per-scope node counts) and
actively wrong for a pipeline-shaped one (a handful of huge, long-lived scopes where a run can span
days and a single scope's concurrency legitimately needs to be very high). Baking either shape's
assumptions into a single global constant privileges one caller's workload over the other's, which
is exactly what AMD-6 forbids.

The fix is not "pick a better global number" — no single number serves both shapes honestly. It is
to make every setting that is shape-sensitive a **first-class per-scope value**, stored where every
instance touching that scope reads the same authoritative record, with a library-wide fallback
chosen for **safety under the zero value**, not for being a good guess at either shape's ideal
operating point. A caller who never touches configuration must get behavior that does not silently
throttle a huge pipeline scope and does not silently retain terabytes of small job-queue scopes
forever — the fallback's job is "do not surprise anyone badly," not "be optimal for anyone."

Several of these fields interact with decisions made elsewhere in this ADR set and must not
contradict them: `PartitionCount` is the value ADR-0014's `Assigner` reads for `P`; `DefaultLeaseTimeout`
and the retry shape connect to the fencing/backoff design (ADR-0006/ADR-0012);
`PayloadCap` narrows (never widens) the library-wide cap ADR-0026 already establishes.

## Decision

**Every scope carries its own `ScopeConfig`, stored in the backend as part of the scope's record
(same key-prefix mechanism as ADR-0023), so every instance touching that scope — regardless of
which process created it — reads the same authoritative value, never a per-instance environment
variable or in-process default.**

```go
type ScopeConfig struct {
    RetentionTTL         time.Duration // terminal-node retention window
    MaxInFlight          int           // 0 = unbounded; hard cap on concurrently InProgress nodes
    DefaultLeaseTimeout  time.Duration // used when a Claim call doesn't override
    LeaseCeiling         time.Duration // hard max any Claim/Extend may request for this scope
    PartitionCount       int           // 0 => 1; fixed at scope creation, see below
    PayloadCap           int           // bytes; 0 => library default (262144)
    SubscriberLagCeiling time.Duration // forced low-water-mark advance threshold, ADR-0022/12 §6.2
    RetryPolicy          RetryPolicy
}

type RetryPolicy struct {
    Base        time.Duration // Full Jitter base, ADR-0012
    Cap         time.Duration // Full Jitter cap
    MaxAttempts uint32
}
```

**Storage.** `ScopeConfig` is read and written through the mandatory storage core (AMD-1), using
the same optimistic-concurrency `Version` type already defined for node CRUD:

```go
GetScopeConfig(ctx context.Context, scope string) (ScopeConfig, Version, error)
PutScopeConfig(ctx context.Context, scope string, cfg ScopeConfig, expected Version) (Version, error)
```

It is mandatory core, not an optional facet, because admission-time enforcement (`MaxInFlight`,
`LeaseCeiling`, `PartitionCount`) has to happen **inside the same atomic operation as the mutation
it gates** — a backend that could only offer eventually-consistent config reads could not honestly
enforce a hard cap. Concretely: `Claim` and `AddNode` read the live `ScopeConfig` row as part of
the same transaction/Lua script/mutex-held region that performs the claim or insert, never from a
client-side cache, so admission control is correct even the instant after a config change — no
instance can observe a stale cap and admit past it. A `Manager`-level cache of `ScopeConfig` is
permitted only for **non-safety-critical** client behavior (choosing a default lease duration to
request, client-side backoff pacing) — anything that actually gates admission always re-checks the
backend's live record.

**Setting and updating.** A scope's `ScopeConfig` is created implicitly, at the same moment the
scope itself is (ADR-0023) — the first `AddNode`/`AddNodes` call against a new scope writes the
library-wide fallback (below) as that scope's initial config. It is changed explicitly via:

```go
func (m *Manager) SetScopeConfig(ctx context.Context, scope Scope, cfg ScopeConfig, expected Version) (Version, error)
func (m *Manager) UpdateScopeConfig(ctx context.Context, scope Scope, mutate func(*ScopeConfig)) error // read-CAS-retry helper
```

Any field may be updated at any time **except `PartitionCount`**, which is fixed at scope-creation
time and rejected with a typed `ErrScopeConfigImmutable` on any later `SetScopeConfig`/`UpdateScopeConfig`
call — see "in-flight semantics" below for why.

**In-flight semantics — what happens to work already underway when a field changes:**

- **`DefaultLeaseTimeout` / `LeaseCeiling`:** affect only claims issued *after* the change.
  Already-granted leases keep the deadline they were issued under — a config change never
  retroactively shortens or lengthens a lease a worker is actively holding, which would violate
  the same "never yank a deadline out from under a fenced holder" discipline the clock-authority
  decision (ADR-0008) already establishes.
- **`MaxInFlight` lowered below the current in-flight count:** existing in-flight claims are never
  force-revoked — there is no safe way to un-claim an active lease without breaking the fencing
  contract. Instead, admission simply refuses new claims (`TryClaim` returns `ErrScopeQuotaExceeded`;
  blocking `Claim` waits) until natural completions bring the in-flight count back under the new
  cap. This mirrors Kubernetes' own `ResourceQuota`: admission-time enforcement, never retroactive
  termination of already-admitted work.
- **`PartitionCount`:** immutable after scope creation, full stop. ADR-0014's node→partition
  hashing is a pure function of `(nodeID, P)`; changing `P` on a scope that already has nodes
  reshuffles nearly every node's partition assignment — a "rehash the world" event with no defined
  migration in this design. A scope that needs a different `P` must be recreated under a new scope
  ID; a future dedicated migration tool is explicitly out of scope for v1.
- **`PayloadCap` lowered:** applies only to future `AddNode` calls. Existing nodes whose payload
  already exceeds the new, lower cap are left exactly as they are — no retroactive validation, no
  deletion, no truncation.
- **`RetentionTTL` / `SubscriberLagCeiling`:** apply to the *next* GC sweep pass going forward, not
  synchronously. Lowering `RetentionTTL` does not force-delete already-past-the-old-TTL nodes
  instantly; they become eligible on the sweep's next scheduled pass, bounded by the sweep
  interval, exactly like every other GC decision in this design (12 §6.3).
- **`RetryPolicy`:** applies to the next `Nack`/timeout decision for each node independently —
  a node already mid-backoff keeps the delay it was scheduled under; only nodes that reach a retry
  decision after the change see the new policy.

**Conservative fallback values, and why each specific number:**

| Field | Fallback | Reasoning |
|---|---|---|
| `RetentionTTL` | `168h` (7 days) | Deletion is irreversible; a fallback that privileged the job-queue shape (River's 24h) would silently GC a multi-day pipeline run's results before an operator who forgot to configure it ever looked. 7 days gives a real chance to notice and configure, without being unbounded. A high-volume job-queue tenant who cares about storage cost sets this explicitly, low — that is the knob this ADR exists to hand them, not a default this ADR guesses on their behalf. |
| `MaxInFlight` | `0` (unbounded) | A hidden concurrency ceiling would silently throttle a huge pipeline scope's legitimate fan-out — the caller never asked for a cap, so the library must not impose one invisibly. The many-small-scopes shape that *wants* a per-tenant cap treats this as the load-bearing knob for its own shape and sets it explicitly; leaving it unbounded by default never surprises the shape that didn't ask for a limit. |
| `DefaultLeaseTimeout` | `30s` | Direct convergence across three independent sources: SQS's default visibility timeout (30s), Asynq's default `LeaseDuration` (30s), and the sample Postgres schema's own `default_timeout_ms` (30000) — "low tens of seconds" is dossier 03's own explicit recommendation (§5f), reasoned from cloud container-freeze/VM-migration pause durations, not from either workload shape. |
| `LeaseCeiling` | `12h` | Mirrors SQS's own hard ceiling on `ChangeMessageVisibility` — long enough that genuinely long-running pipeline work has real headroom via `Extend`, while still being a real, finite bound the sweeper's own assumptions can rely on; an unbounded ceiling would defeat the point of having one at all. |
| `PartitionCount` | `0` → treated as `1` | Reduces exactly to ADR-0013's pure pull-competition — the simplest correct behavior, and the cheapest possible per-scope bookkeeping overhead, which matters most for the many-small-scopes shape where this field's cost is paid once per scope, potentially millions of times. A pipeline-shaped scope that needs concurrency headroom within one scope sets `PartitionCount` to the `32-64` range explicitly at creation time, per dossier 07's own sizing recommendation — that shape's need is exactly the case this field exists to serve on request, not by default. |
| `PayloadCap` | `262144` bytes (256 KiB) | Matches the library-wide cap ADR-0026 already establishes and the traditionally-cited SQS message-size figure (12 §5.3) — a well-tested "small enough to force large blobs out-of-band, large enough for the overwhelming majority of real work-item payloads" number, independent of either workload shape. |
| `SubscriberLagCeiling` | `72h` | Long enough that an operator offline over a long weekend is not punished by having their subscription forcibly resynced; short enough that a genuinely abandoned subscriber does not pin storage indefinitely (12 §6.3) — reasoned independently of `RetentionTTL`, since it protects against a different failure (a wedged subscriber), not against unbounded data growth. |
| `RetryPolicy` | `{Base: 1s, Cap: 30s, MaxAttempts: 3}` | Full Jitter shape per the library-wide backoff decision (ADR-0012); `MaxAttempts` is kept deliberately small so a systemically broken worker or misconfiguration does not quietly consume ready-set capacity retrying forever under either workload shape — a scope that needs deep resilience against transient failures raises this explicitly. |

## Consequences

### Positive
- No single number in this design privileges either target workload shape — every field a shape
  cares about differently is a per-scope knob, and the fallback is chosen for safety, not for
  fitness to one shape.
- Admission enforcement (`MaxInFlight`, `LeaseCeiling`, `PartitionCount`) is correct immediately
  after a config change on every instance, because it is re-checked inside the same atomic
  operation as the mutation it gates — no propagation-delay window where one instance enforces an
  old cap and another enforces a new one.
- The zero-value `ScopeConfig{}` is safe to leave untouched indefinitely for either shape — nothing
  silently throttles, nothing silently deletes too early, nothing silently over-partitions.

### Negative
- Every `Claim`/`AddNode` call now reads a config row as part of its atomic operation — a small
  but real added cost (one extra field read/lock scope) on every mandatory backend's hot path,
  compared to a design with compiled-in constants.
- `PartitionCount`'s immutability means a scope that outgrows its initial sizing guess has no
  in-place fix — the caller must create a new scope, which is a real migration cost for a long-
  lived pipeline scope that grew larger than expected.
- Eight independently-reasoned fallback numbers is more surface for an implementer to get wrong or
  for documentation to drift from than one blessed "just use River's numbers" answer would have
  been.

### Neutral
- `ScopeConfig` reuses the same `Version`-based optimistic-concurrency shape as node CRUD rather
  than inventing a separate config-versioning mechanism — this is a consistency choice, not a
  performance-driven one.

## Alternatives considered

- **One library-wide default set, override only via `Option` at `Manager` construction**: rejected
  — a process-level default cannot serve two scopes of different shapes running against the same
  `Manager` simultaneously, which is the exact multi-tenant situation a shared library instance is
  expected to handle; AMD-6 rules this out directly.
- **Per-scope config held in the host program's own memory/env vars, not the backend**: rejected —
  "every instance agrees" fails immediately the moment a second process touches the same scope with
  a different in-memory value; the whole point of a shared storage backend (ADR-0016, numbered set)
  is that scope-level facts live where every instance can read the same one.
- **Derive `SubscriberLagCeiling` arithmetically from `RetentionTTL` (e.g., always 3×)**: considered
  and rejected as a hard coupling — the two protect against different failures (unbounded storage
  growth vs. a wedged subscriber) and an operator may legitimately want to tune them independently;
  each gets its own reasoned fallback instead.
- **Allow `PartitionCount` to change in place with a background rehash**: rejected for v1 — no
  dossier in this series designs a safe node-reassignment migration for an in-use scope, and
  building one speculatively risks shipping an unvalidated correctness-critical mechanism; recreate-
  under-a-new-scope-ID is the documented v1 answer, with an explicit migration tool left as future
  work.

## References

- [River — job cleaner service default retention](https://pkg.go.dev/github.com/riverqueue/river/rivertype)
- [Kubernetes — ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)
- [AWS SQS — visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- docs/research/12-dag-semantics-and-state-machine.md §4.1, §5.3, §6.2-§6.3
- docs/research/07-work-distribution-across-instances.md §7.1 recommendation 6
- docs/research/03-leases-heartbeats-timeouts.md §5f
- ADR-0013/ADR-0014 (`PartitionCount` feeds the `Assigner`); ADR-0023 (scope creation moment and
  key-prefixed storage location); ADR-0026 (numbered set, library-wide payload cap this ADR
  narrows); ADR-0032 (`Priority`, deliberately not a `ScopeConfig` field — it is per-node, not
  per-scope)
