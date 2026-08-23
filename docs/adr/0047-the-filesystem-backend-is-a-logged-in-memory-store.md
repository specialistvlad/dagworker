# ADR-0047: The filesystem backend is a logged in-memory store

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0031 (adds a backend to the core module rather than as an eighth module)
- **Backing research:** Redis AOF+RDB; issue #31

## Context

The requirement is stated, not hypothetical: **files only, no database of any
kind, state must survive a restart.** A migration to a real database comes
later.

No current backend serves it. In-memory loses everything; Redis and PostgreSQL
are servers. Issue #31 proposed four designs and recommended a periodic
snapshot (A) as the first milestone of a log-plus-snapshot design (B).

Re-estimating against the requirement changed the plan in three places.

## Decision 1: Option A cannot ship, and the reason is this repository's own bar

Not because a snapshot is inelegant. Because **it cannot honestly set
`CapDurableStorage`**, which is the only capability the requirement asks for.

The precedent is already set, and it is strict. Redis **declines**
`CapDurableStorage`:

> CapDurableStorage is not (Redis's default async replication can lose up to
> ~1s of writes on an unclean primary failover)

A ~1s window under a failover disqualifies Redis. A periodic snapshot's window
is the whole snapshot interval, and it opens on *every* unclean exit — a
SIGKILL, an OOM, a power loss — not only on a failover. It is strictly larger
and strictly more frequent than the window this project already judged
disqualifying.

So Option A is a backend that survives a restart, cannot say so in
`Capabilities()`, and exists to answer a requirement about surviving restarts.
ADR-0016's rule that a backend must not claim what it cannot deliver settles it.
**A is not a milestone on the way to B; it is a different, weaker product.**

## Decision 2: it lives in the core module, not an eighth one

ADR-0031's test for module placement is dependency edges, not importance. It
put `dagstoretest` in core on exactly that reasoning: it "adds **zero** new
module edges".

A filesystem backend needs `os`, `encoding/binary`, `hash/crc32` — **nothing
outside the standard library.** It passes the same test `dagstoretest` passed,
and it is the same shape as `storage/memory`, which is in core for the same
reason: a zero-dependency backend behind a module boundary costs a second
`go get`, a `go.work` entry, a `MODULES` entry, a Dependabot entry and a release
to coordinate, and buys no isolation, because there is nothing to isolate.

## Decision 3: the log records commands **plus the nondeterminism**, not outcomes

Issue #31 names the real design problem and is right about it: a naive
command log does not work, because `Claim` stamps a deadline from the store's
own clock (ADR-0008), so replaying the command produces a different deadline
than the original. It concluded that the log must therefore record outcomes,
which needs access to the in-memory backend's internals.

There is a third possibility it did not consider, and it is available because
of what `storage/memory` already exposes:

```go
memory.WithClock(c dw.Clock)
memory.WithJitter(fn func(n int64) int64)
```

**Every source of nondeterminism in the in-memory backend is already
injectable**, and it is only those two: the clock, and the retry-backoff
jitter. Everything else is deterministic — the ready set and the successor and
predecessor lists are slices, not maps, so no iteration order varies between
runs, and `AddNodes` settles in caller order through `orderedSet`.

So a log record is the command **and the readings it consumed**:

```
record := { command, clockReadings []int64, jitterValues []int64, crc32 }
```

Replay feeds a clock and a jitter function that pop from the recorded
sequences. The result is not approximately the original state, it is
**exactly** the original state, and it is guaranteed by construction rather
than by a serialiser being kept in step with the backend it serialises.

Recording every reading rather than one per operation matters: `Claim` reads
the clock four times, and a single timestamp replayed for all four would drift
from what actually happened.

What this buys over the outcome log: no new public API on `storage/memory` for
the log's sake, no fork of the reference implementation, and — the real prize —
**one implementation of the semantics.** The file backend cannot disagree with
the in-memory backend about what a claim does, because it *is* the in-memory
backend with a log around it.

### What it does not solve

Compaction. The log must be truncated or replay grows without bound, and a
snapshot needs the state serialised, which the trick above does not provide.

Filtering the log ("keep records that mention a live node") is tempting and
wrong: completing a node releases its successors, so dropping a removed node's
records can leave a surviving successor un-readied on replay. The dependency
structure is exactly what makes records non-self-contained.

So a snapshot is needed, and `storage/memory` gains `Snapshot`/`Restore` after
all — issue #31's option 1, which it called "cleanest" and "useful on its own".
The re-estimation does not avoid it; it confines it to the snapshot path and
keeps it off the log path, where it would otherwise have had to encode the
outcome of every mutation.

## Consequences

- **`storage/file` sets `CapDurableStorage` and not `CapCrossProcess`**, both
  truthfully. One process at a time; the log is not a coordination protocol.
- **The durability disclosure gains a row**, and it says "no loss on an unclean
  exit, at one `fsync` per mutation", with group commit as a documented opt-in
  that trades a stated window for throughput — never a default, because a
  default that quietly reintroduces Option A's window would defeat the point.
- **`dagstoretest.RunConformance` passes unmodified**, which is what makes the
  later database migration the one-line change ADR-0016 promises.
- **The graph stays resident in memory**, the same assumption `storage/memory`
  already makes. A backend that outgrows memory is a different product, and
  bbolt (#31's option D) is what it would be built on — excluded here because
  "no database of any kind" excludes an embedded database, and because it would
  be the core module's first third-party dependency.
- **The migration path is unchanged and already works**: `filestore.New(path)`
  → `postgres.Open(dsn)`. Moving the *data* remains unsolved and is issue #32.
