# ADR-0008: The storage backend owns the clock for every deadline comparison

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §3.5, §4, §5a, §5f

## Context

Spanner's TrueTime API is the canonical demonstration that "read the wall clock" is not a single
well-defined operation in a distributed system — `TT.now()` returns an interval, not a point, with a
half-width ε that Google's own reported deployment shows forming a sawtooth **between roughly 1ms and
7ms**, even with a dedicated fleet of GPS receivers and atomic-clock "Armageddon masters" cross-
checked against each other (03 §3.5). Spanner's answer, commit-wait, deliberately spends real
wall-clock time (up to 2ε) converting timestamp *uncertainty* into added *latency* — the only way to
make one clock reading provably precede another. dag-worker-go, embedded in arbitrary host processes
on arbitrary infrastructure — laptops, cheap cloud VMs, wildly inconsistent NTP hygiene across
instances — has no comparable guarantee to bound and cannot assume one.

Dossier 03's clock-pathology table (§4) catalogs what actually happens in production, concretely: NTP
step correction can jump a local clock backward or forward, making an expired lease look valid again
or a fresh one look instantly expired; VM live migration measurably lags the guest clock behind real
elapsed time (VMware's own documentation warns of exactly this); leap-second smearing can disagree
between smeared and non-smeared hosts in the same fleet. The sharpest case is the Linux freezer
cgroup: unlike `SIGSTOP`, which a process can at least architecturally intercept, the freezer is
"a *transparent*, non-catchable suspend" — a frozen worker resumes with **no signal handler run, no
exception raised, nothing** — it has categorically no way to detect that any time passed at all. The
table's unifying conclusion: there is no pause, skew, or clock event a worker can experience that a
storage-side deadline check plus a fencing token (ADR-0006) cannot render safe, and no amount of
client-side cleverness makes a client's own clock trustworthy enough to be the sole safety mechanism.

## Decision

**The storage backend's own clock is the sole authority for every deadline decision.** No library
instance's local clock — monotonic or wall — is ever consulted to decide whether a lease has expired.

- **Redis**: every "now" read used in a deadline comparison happens *inside* the Lua Function via
  `redis.call('TIME')`, "the current server time as a two-item list" — never passed in as an `ARGV`
  computed by the calling library instance.
- **Postgres**: every deadline write and comparison uses **`clock_timestamp()`**, never
  `now()`/`CURRENT_TIMESTAMP`/`transaction_timestamp()`. `now()` is frozen at transaction start; a
  long-running sweeper transaction that used it would compare against a timestamp stale by however
  long that transaction has been open — exactly the class of bug this ADR exists to prevent. This is
  a **lint-checkable rule**: grep-ban `now(` in every `.sql` file under the Postgres backend package
  and fail CI on a match, since reaching for the wrong Postgres time function is an easy, silent
  mistake for a future contributor to make.
- **In-memory**: exactly one process's clock is in scope, by this project's own design (the store is
  shared in-process across every worker in that process) — read it through the `Clock` interface
  (ADR-0027), preferring the monotonic reading Go attaches to `time.Now()` for elapsed-time and
  deadline-comparison math, since only the monotonic component is immune to NTP step correction on
  that single host.
- A per-node timeout is specified by the host as a `time.Duration`. The library converts it to an
  absolute deadline by adding it to a server-side "now" read **at the moment of the `Claim`** (inside
  the same atomic write, ADR-0007) — never by asking the calling process what time it thinks it is and
  shipping an absolute value to storage.
- A worker's own clock governs **scheduling only** — when it chooses to call `Extend` (ADR-0010). It
  is never consulted to decide whether the worker's own lease is still valid; that question is
  answered exclusively by the storage backend's response to the next operation the worker attempts
  (the fenced CAS, ADR-0006/ADR-0009).
- No clock-skew compensation (a fudge-factor grace window around the deadline) is built into the
  correctness path. Per §5f, any such margin is itself just an unmeasured epsilon; the one place a
  deliberate, documented margin belongs is heartbeat/extend *scheduling* (renew comfortably before the
  deadline, ADR-0010) — a robustness tuning knob, never a correctness-load-bearing tolerance window.

## Consequences

### Positive

- Every clock pathology in §4's table (NTP step, VM migration, freezer suspend, leap-second smear)
  becomes a non-issue by construction: no anomaly needs to be detected anywhere in the protocol, only
  the fencing check (ADR-0006) needs to fire correctly on the next write, and it already does
  regardless of what caused the anomaly.
- Cross-instance clock skew across N concurrent library processes is eliminated as a source of
  false-expiry/missed-expiry bugs, because exactly one clock — the storage backend's — ever
  adjudicates any deadline.
- The rule is simple enough to be mechanically enforced (a `.sql` lint ban), not just documented
  convention that erodes over contributors and time.

### Negative

- Every backend's Lua/SQL surface must be audited so no accidental client-supplied timestamp sneaks
  into a deadline write — a lint rule is required, not merely a code-review habit, precisely because
  the mistake is easy and silent.
- The in-memory backend's "authoritative clock" is only as trustworthy as the single host it runs on;
  this ADR does not and cannot protect against that host's own clock being wrong in an absolute sense
  — it only removes *cross-instance* disagreement as a variable, which is the risk actually in scope
  for a library instance's decision-making.

### Neutral

- This ADR deliberately does not attempt to model or bound clock uncertainty (no TrueTime-style
  interval, no commit-wait) — see Alternatives. The library ships a conservative low-tens-of-seconds
  default lease timeout (03 §5f) as an ergonomic starting point, not as a safety mechanism in itself;
  the fencing token is what makes any chosen value safe to get "wrong."

## Alternatives considered

**Client-computed wall-clock deadlines** (`deadline = time.Now().Add(timeout)` in the calling library
instance, shipped to storage as an absolute value). Rejected: this is exactly the design §3.5 and §4
show is unsafe — a stepped or skewed local clock produces a deadline meaning something different to
its writer than to whoever later reads it, and nothing requires multiple library instances' clocks to
agree at all.

**A TrueTime-style bounded-uncertainty interval with commit-wait.** Rejected as disproportionate:
Spanner can afford this because Google operates its own GPS/atomic-clock time-master infrastructure
and can measure and bound ε to single-digit milliseconds (03 §3.5); dag-worker-go runs on arbitrary
infrastructure with no comparable guarantee to bound, and the fencing token (ADR-0006) already makes
a plain point-in-time deadline safe without needing to reason about an uncertainty interval at all.

**Accept an ack "close to" the deadline via an explicit grace-window fudge factor.** Rejected: §5f
names this precisely as trading one undocumented epsilon for another; any deliberate margin belongs
in *scheduling* (when a worker chooses to renew, ADR-0010), never folded into the correctness check
itself, which must remain a hard boundary the storage clock alone decides.

## References

- Corbett et al., "Spanner: Google's Globally-Distributed Database" (OSDI 2012) — https://static.googleusercontent.com/media/research.google.com/en//archive/spanner-osdi2012.pdf
- sookocheff, TrueTime summary with sourced figures — https://sookocheff.com/post/time/truetime/
- muratbuffalo, "Use of Time in Distributed Databases" — http://muratbuffalo.blogspot.com/2025/01/use-of-time-in-distributed-databases.html
- Redis `TIME` command docs — https://redis.io/docs/latest/commands/time/
- PostgreSQL Date/Time Functions and Operators (`clock_timestamp()` vs `now()`) — https://www.postgresql.org/docs/current/functions-datetime.html
- Linux kernel freezer-subsystem docs — https://docs.kernel.org/admin-guide/cgroup-v1/freezer-subsystem.txt
- docs/research/03-leases-heartbeats-timeouts.md §3.5, §4, §5a, §5f
- docs/research/00-synthesis.md §2.2 step 3, ADR-08 seed
