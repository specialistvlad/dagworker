# ADR-0012: Retry backoff is full jitter

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/12-dag-semantics-and-state-machine.md §3.3

## Context

AWS's own Architecture Blog runs a head-to-head test of three candidate backoff formulas —
`FullJitter = random_between(0, min(cap, base·2^attempt))`, `EqualJitter` (a deterministic half plus a
bounded random half), and `DecorrelatedJitter` (bounded by the previous sleep) — and concludes Full
Jitter is the recommendation: it "uses less work" (fewer total retry attempts consumed fleet-wide
under contention) than Equal Jitter, and performs comparably to Decorrelated Jitter while being
simpler to reason about and implement, because "adding jitter addresses the clustering problem...
without jitter, exponential backoff alone creates gaps and clusters of calls, whereas jittered
approaches achieve an approximately constant rate of calls" (12 §3.3). AWS's own product team later
shipped exactly this recommendation as a literal `JitterStrategy: FULL | NONE` enum value in Step
Functions' `Retry` block, years after publishing the blog post — "about as strong an internal-
consistency endorsement as a design decision gets" (12 §3.3).

Two countervailing data points in the survey are both explicitly weaker precedents, not
counter-arguments: Cloud Tasks' `minBackoff`/`maxBackoff`/`maxDoublings` scheme has **no jitter term
at all** — a real, shipped, but knowingly inferior choice that predates or ignores the jitter
literature. Sidekiq's `count**4 + 15 + rand(10*(count+1))` is closer to Equal Jitter in shape (a
deterministic floor plus a bounded random addition), tuned for a human-facing ~20-day dead-letter
timeline rather than a machine-facing thundering-herd concern, and its polynomial (not exponential)
growth does not transfer to a library whose retry timeline must be configurable per node rather than
baked into one global curve (12 §3.3).

## Decision

```go
// Full Jitter, per AWS's own recommendation, as the library-wide default.
func fullJitterBackoff(attempt uint32, base, cap time.Duration) time.Duration {
    exp := base << attempt // base * 2^attempt, saturating on overflow
    if exp > cap || exp < base {
        exp = cap
    }
    return time.Duration(rand.Int63n(int64(exp) + 1))
}
```

- `random(0, min(cap, base·2^attempt))` is the library-wide default backoff computed before a retried
  node (transition T9, ADR-0011) re-enters `Ready`.
- `base`, `cap`, and `maxAttempts` are settable **per node, at `Claim` time**, via the same
  `ClaimOption`/retry-policy struct that carries the ack timeout (ADR-0007, ADR-0010) — timeout and
  retry policy are both answers to "how should the engine treat this node if the worker doesn't come
  back cleanly," and belong in one options struct, not two.
- `attempt` in the formula is the **same field** as `LeaseEpoch` (ADR-0011) — no separate retry-count
  variable is threaded through the backoff computation.
- A cross-node "retry budget" (a shared cap on total retries-in-flight across a scope, meant to stop a
  systemic outage from retry-storming a downstream dependency) is **explicitly out of scope** for the
  DAG engine itself: no system surveyed (Temporal, Airflow, Argo, Kubernetes, River, Cloud Tasks,
  Faktory, Sidekiq) implements this as an engine feature — it appears one layer down, as a property of
  a worker's own HTTP/RPC client. A host program that wants this can build it by watching the
  status-transition event stream (ADR-0019) for `ReasonWorkerError`/`ReasonTimeout` density and calling
  `CancelScope` if a threshold trips: exposing the primitive is sufficient; building the policy in is
  scope creep the design does not need.

## Consequences

### Positive

- A single, well-tested formula with a strong industry precedent (AWS's own head-to-head data, later
  reaffirmed by Step Functions shipping it as a literal enum) covers the default case without this
  project needing to run its own contention experiments first.
- Per-node overrides mean job-queue-shaped and pipeline-shaped scopes (see the per-scope configuration
  decision, ADR-0023) are never forced to share one global retry curve.

### Negative

- Full Jitter's uniform-random floor of zero means a retry can, rarely, fire almost immediately after
  a failure — an accepted trade per AWS's own data (it reduces aggregate contention despite this), but
  worth documenting so operators are not surprised by an occasional near-zero-delay retry.
- No cross-node retry budget ships with the engine; a host that needs one must build it on the event
  stream itself rather than configure a built-in knob.

### Neutral

- This ADR fixes only the backoff *formula*; it does not decide the fail-fast-vs-continue cascade
  policy for a node's downstream successors when retries are exhausted, which is a separate decision.

## Alternatives considered

**Equal Jitter.** Rejected: AWS's own testing shows it "uses more work" — more total retry attempts
consumed under contention — than Full Jitter for comparable protection against request clustering.

**Decorrelated Jitter.** Rejected as the default: performs comparably to Full Jitter per AWS's data but
is more complex to reason about and implement, since it depends on the previous sleep value rather
than just `attempt` — for no measured benefit at this project's scale. Nothing rules out exposing it
later as an alternative `BackoffStrategy` for a caller who wants it.

**No jitter — deterministic exponential backoff**, Cloud Tasks' shape. Rejected: a real, shipped
precedent, but explicitly the weaker one in the survey; deterministic backoff reintroduces exactly the
clustering-and-thundering-herd problem jitter exists to solve.

**A shared cross-scope retry budget as a first-class engine feature.** Rejected for v1: no system
surveyed implements this at the DAG-engine layer (12 §3.3); exposing the status-transition event
stream is sufficient for a host to build the policy itself, and keeps the engine's own responsibility
narrow.

## References

- AWS Architecture Blog, "Exponential Backoff and Jitter" — https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
- AWS Step Functions error handling (`JitterStrategy: FULL | NONE`) — https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html
- Sidekiq Error Handling wiki — https://github.com/sidekiq/sidekiq/wiki/Error-Handling
- docs/research/12-dag-semantics-and-state-machine.md §3.3
- docs/research/00-synthesis.md ADR-12 seed
