---
title: Performance and the complexity guarantees
description: The measured numbers at a million nodes on every backend, and — more importantly — the ratio methodology that makes those numbers a tested contract rather than a claim.
---

The number that matters on this page is not any single latency figure. It's
that none of them change with the size of the graph. This page gives you
both: the measured numbers, and the methodology that actually enforces the
guarantee behind them — because the methodology is what makes the numbers
trustworthy rather than a snapshot that quietly goes stale.

## The measured numbers

At **1,000,000 nodes**, on every backend, Apple M-series, Go 1.26, databases
in containers on the same laptop:

| | in-memory | Redis | PostgreSQL |
|---|---|---|---|
| `ScopeStats` | 31 ns | 177 µs | 167 µs |
| `GetNode` | 443 ns | 196 µs | 185 µs |
| `Claim` + `Complete` | 1.7 µs | 797 µs | 3.6 ms |
| seed 1,000,000 nodes | 0.9 s | 34 s | 21 min |

One round trip to a container here is roughly 185 µs, which is why the two
networked backends bottom out where they do — nothing single-shot beats a
round trip, no matter how the query is written. PostgreSQL's seeding cost
works out to roughly six un-pipelined round trips per node inserted; that's
a constant factor, not growth with graph size, and pipelining those inserts
with `pgx.Batch` is the known, not-yet-taken optimization. See [Choosing a
backend](/dagworker/guide/backends/) for the same shape of comparison at a
more modest 30,000 nodes, and for what each backend actually durability-wise
promises in exchange for its cost.

## Why a ratio, and not a number

An absolute nanosecond threshold is a promise about hardware. On a shared CI
runner, that promise gets broken by a noisy neighbor long before it gets
broken by an actual regression — which means a threshold either has to be
so loose it catches nothing, or so tight it's a source of flaky failures
unrelated to the code under test. Neither is a contract worth having.

So the complexity guards assert something else: **the ratio of
per-operation cost between the smallest and largest graph size in the same
sweep, measured in the same process, in the same run.** The sweep spans
three orders of magnitude — 1,000, 10,000, 100,000, and 1,000,000 nodes —
which is wide enough that a hidden linear term cannot hide inside
measurement noise:

```
O(1) or O(log n)   ratio stays in single digits, dominated by cache misses
O(sqrt n)          ratio around 31x
O(n)               ratio around 1000x
```

A bound of 20x is loose enough to absorb the cache-locality penalty every
operation pays once its data no longer fits in L2, and tight enough that
anything worse than logarithmic fails decisively rather than marginally. A
networked backend gets a wider 30x bound, because a round trip dominates the
measurement and widens the spread — the guarantee isn't weaker there, the
noise floor is just higher.

This is what CI actually asserts on every run, not a number transcribed by
hand from a one-off benchmark:

```
Claim              1.12x
Claim+Complete     0.77x
AddNode (causal)   0.46x
ScopeStats         0.73x
GetNode            3.51x   (cache misses, not complexity)
```

A ratio below 1.0 is real, not a typo — it means the operation measured
*faster* at a million nodes than at a thousand, most likely because a larger
run amortizes warm-up cost differently. The guard only fires in one
direction: cost growing with graph size, which is the actual defect class
this exists to catch — a linear scan hiding where the design calls for O(1)
or O(log n).

## What each guard is actually checking

The guards aren't a single "is everything fast" check — each one targets
the specific operation whose complexity bound the design depends on, and
each is checked against the real, not synthetic, data structures a scope
uses:

- **`Claim` must cost the same at any ready-set size** — it's a pop from an
  ordered structure (a heap in memory, a `ZPOPMIN` on Redis, an indexed
  `SKIP LOCKED` scan on Postgres), never a search.
- **`GetNode` must not depend on how many other nodes exist** — the
  benchmark deliberately strides through the whole keyspace rather than
  re-reading one hot node, so the measurement pays the same cache-miss cost
  a real, spread-out workload would.
- **`AddNode` with one dependency on an existing node** — the common,
  causally-ordered insert — must stay O(1): the topological rank invariant
  already holds, so no cycle-check search runs at all (see [Dynamic
  graphs](/dagworker/guide/dynamic-graphs/) for what happens on the
  uncommon, out-of-order path instead).
- **Claiming and completing a node together must not depend on how many
  other nodes are in the scope** — the design's stated bound is that
  completion cost tracks a node's own out-degree, never total graph size,
  and the guard sweeps the same claim-then-complete operation from a
  thousand nodes up to a million to confirm the combined cost stays flat.

Every one of these runs against the in-memory backend on a plain `go test
./...`, and against Redis and PostgreSQL as well when
`DAGWORKER_INTEGRATION` is set, so the same guarantee is checked against
real network round trips, not only against the backend with no I/O to hide
behind.

## Reading the numbers as a user

The practical takeaway is simple: if your graph grows from a thousand nodes
to a million, your per-operation latency budget does not need to grow with
it. What *does* change is which constant dominates — in-memory operations
stay bound by cache locality, while Redis and PostgreSQL stay bound by
network round-trip time regardless of graph size. Pick a backend on that
basis (see [Choosing a backend](/dagworker/guide/backends/)), not on a fear
that a bigger graph will make every operation slower — by design, and by a
guard that runs on every commit, it won't.
