# ADR-0028: In-memory backend internals: struct-of-arrays, dense handles, sharding

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/09-performance-engineering-1m-nodes.md Part 1 (§1.1–§1.7), Part 2 (§2.1–§2.4)

## Context

The 1,000,000-node benchmark target (docs/research/00 §1 exec summary) is a data-layout discipline
for the in-memory backend, not a tuning pass applied afterward. Go's garbage collector's own guide
states its cost model plainly: "GC CPU time for cycle N = fixed cost + cost per byte × live heap
memory found," and pointer-free spans are classified `noscan` and skipped in O(1) — "the GC will
stop scanning values at the last pointer in the value"
([A Guide to the Go Garbage Collector](https://tip.golang.org/doc/gc-guide)). The cost is linear in
*pointer-bearing* live heap, never in object count.

**The arithmetic at target scale.** A "natural" Go graph representation —
`Node{ID string; Status uint8; Children, Parents []*Node}` — at 1,000,000 nodes and 3,000,000
edges (out-degree 3) costs roughly 137 MiB of heap and forces the GC to trace roughly 9,000,000
pointers **every mark cycle**, in random-access order, because heap-allocated `Node`s and their
backing arrays are not laid out contiguously. A struct-of-arrays representation over dense `int32`
handles — `status []uint8`, CSR-style `childOff/childIdx []int32`, `gen []uint32` — holds the
identical graph in roughly 20 MiB, and the GC's marginal cost for that data is a small constant
(just the handful of slice headers on the top-level struct) independent of N. That is a 6.8×
heap difference and, more importantly, the difference between "trace 9,000,000 pointers per cycle"
and "trace about a dozen words per cycle."

This is not a theoretical concern: Discord's own production account of its "Read States" service
— an in-process cache with tens of millions of pointer-bearing entries — is the canonical case
study. "The garbage collector had to scan the entire LRU cache in order to determine if the memory
was truly free from references… Go will force a garbage collection run every 2 minutes at
minimum" regardless of allocation rate
([Why Discord is switching from Go to Rust](https://discord.com/blog/why-discord-is-switching-from-go-to-rust)).
Both of Discord's own remediation attempts — tuning `GOGC`, shrinking the cache — failed for
structural reasons: the periodic forced GC cycle still has to walk whatever is live, and shrinking
the cache traded pause duration for a worse cache-miss rate. `allegro/bigcache`, built explicitly
around pointer-free values, measured a 6.2× GC-pause difference (1.5 ms vs. 9.3 ms) at 20,000,000
entries purely from pointer density in the payload ([allegro/bigcache](https://github.com/allegro/bigcache)) —
the closest empirical analog at a smaller scale to the arithmetic above.

**Lookup structure.** Go 1.24 rewrote the built-in map to a Swiss Table design and measured "up to
60% faster [map operations] in microbenchmarks" but only "a geometric mean CPU time improvement of
around 1.5%" across full application benchmarks
([Faster Go maps with Swiss Tables](https://go.dev/blog/swisstable);
[Go 1.24 release notes](https://go.dev/doc/go1.24)). The gap between those two numbers is the
central argument for this ADR's approach: the map itself was never the real-world bottleneck to
chase, because allocation, GC, and pointer-chasing elsewhere dominate. Since dagworker mints its
own node handles, it can skip the map runtime in the hot path entirely — `[]T` indexed directly by
a dense handle costs one bounds check and one array index, no hashing, whether the runtime is on
Go 1.23's bucket map or Go 1.24's Swiss Table.

**Concurrency.** A single contended atomic or mutex does not degrade gracefully with more
goroutines — it collapses, because a contended atomic serializes through the cache-coherence
protocol: "~100 ns per op [contended] … not slow because the instruction itself is slow… they
serialise across cores" ([Travis Downs, A Concurrency Cost Hierarchy](https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html);
corroborated by [ETH Zürich's atomic-operations benchmark](https://htor.inf.ethz.ch/publications/img/atomic-bench.pdf)).
A real Go measurement on an 8-core slice of a 20-core part found a 256-way sharded map at 40.0
Mops/s write-heavy versus 4.81 Mops/s for a single `sync.Mutex` (8.3×) and 13.7 Mops/s for
`sync.Map` — worse than sharding because `sync.Map` optimizes for read-mostly, append-only key
sets, not this workload's read/write mix
([Shard your locks](https://strebkov.dev/posts/shard-your-locks/)).

## Decision

The in-memory backend's internal representation — never crossing the public `dagstore.Store`
interface (ADR-0016), which stays string/`[]byte`-shaped — is fixed as follows:

**1. Dense, generation-counted `int32` handles**, minted by a slab allocator, never a bare
`map[string]*Node`:

```go
type Handle struct{ Index, Gen uint32 } // packs into one uint64; noscan, CAS-friendly

type Slab[T any] struct {
	items []T
	gens  []uint32
	free  []uint32 // LIFO stack of reclaimed indices
}

func (s *Slab[T]) Free(h Handle) bool {
	if int(h.Index) >= len(s.items) || s.gens[h.Index] != h.Gen {
		return false // stale handle — no-op, not corruption
	}
	s.gens[h.Index]++ // invalidates every outstanding copy of h
	var zero T
	s.items[h.Index] = zero
	s.free = append(s.free, h.Index)
	return true
}
```

**2. One string→`int32` intern table at the boundary**, never a second string-keyed map anywhere
downstream:

```go
type Interner struct {
	mu    sync.RWMutex
	toID  map[string]int32
	toStr []string // reverse lookup, index == handle
}

func (in *Interner) Intern(b []byte) int32 {
	in.mu.RLock()
	if id, ok := in.toID[string(b)]; ok { // compiler avoids allocating here
		in.mu.RUnlock()
		return id
	}
	in.mu.RUnlock()
	in.mu.Lock()
	defer in.mu.Unlock()
	if id, ok := in.toID[string(b)]; ok {
		return id
	}
	s := string(b)
	id := int32(len(in.toStr))
	in.toID[s] = id
	in.toStr = append(in.toStr, s)
	return id
}
```

Every internal comparison, hash, and GC scan operates on the `int32` handle past this one
boundary — never the string.

**3. CSR-style adjacency**, never `[]*Node`/`[]Handle`-per-node slices: `childOff []int32` (length
N+1) and `childIdx []int32` (length = total edges), giving the ~20 MiB / handful-of-pointers
footprint quantified in Context, versus the ~137 MiB / 9,000,000-traced-pointers cost of the
pointer-linked design.

**4. Bitsets for ready/blocked membership**, one `[]uint64` per `(scope, kind)` partition:

```go
type Bitset []uint64

func (b Bitset) Set(h int32)       { b[h>>6] |= 1 << uint(h&63) }
func (b Bitset) Clear(h int32)     { b[h>>6] &^= 1 << uint(h&63) }
func (b Bitset) Test(h int32) bool { return b[h>>6]&(1<<uint(h&63)) != 0 }
```

Popcount-based cardinality (`math/bits.OnesCount64` per word) is **O(N/64), not O(1)** — a 64×
constant-factor win, never described as constant-time in benchmarks or documentation. Escalate to
a compressed bitmap (RoaringBitmap-shaped) only if measurement shows the ready set is sparse
relative to N and iteration/cardinality cost actually matters.

**5. Sharded, cache-line-padded `RWMutex` striping** over the node-state table — never a single
global mutex, never bare `sync.Map`:

```go
var stripe [8]struct {
	sync.RWMutex
	_ [7]uint64 // pad to a full 64-byte cache line, prevent false sharing
}

shards := nextPow2(8 * runtime.GOMAXPROCS(0))
if shards > 256 {
	shards = 256 // measured knee (09 §2.2); re-validate on target hardware
}
shardIdx := handle & int32(shards-1)
```

**6. Ready queues and aggregate counters sharded identically** to the state table (same
handle-derived shard index), so a status transition and its resulting ready-notification stay
local to one shard's lock. Any per-completion aggregate (total ready, total in-flight) is a
striped, per-shard cell summed only on a rare `Stats()` call — never a single shared
`atomic.Int64` incremented on every completion, which is exactly the "one hot atomic" the
contention-cliff measurements above show collapsing throughput 20–100× under load.

## Consequences

### Positive
- Steady-state footprint at 1,000,000 nodes / 3,000,000 edges drops from ~137 MiB to ~20 MiB, and
  the GC's marginal per-cycle cost for that data drops from tracing ~9,000,000 pointers to tracing
  a small constant — the concrete mechanism that makes the project's own mandated 1M-node
  benchmark achievable on commodity hardware without GC-pause-driven tail latency.
- Write throughput scales to the measured shard knee (~256, 8×`GOMAXPROCS`) instead of collapsing
  at a handful of concurrent goroutines, which the single-mutex/`sync.Map` alternatives both do.

### Negative
- Materially more implementation complexity than the "obvious" pointer-linked Go design: a
  generation-counted slab allocator, a CSR adjacency structure that supports edge insertion and
  deletion (harder to get right than `append`-to-a-`[]*Node`), and a hand-rolled interner. This
  complexity is paid once, inside `storage/memory` only — it never crosses the public `Store`
  interface (ADR-0016), and no other backend or the public API pays any part of this cost.

### Neutral
- The 256-shard / `8×GOMAXPROCS` constant is explicitly a starting point measured on one 8-to-20-
  core desktop part (09 §2.2's own caveat), not a proven-optimal universal constant. It must be
  re-validated by dagworker's own benchmark suite on its target hardware; this ADR freezes the
  struct-of-arrays/handle/CSR/sharding *shape*, not this specific numeric constant.

## Alternatives considered

**Array-of-structs with pointer-linked children/parents** (`Node{ID string; Children []*Node; ...}`)
— the "obvious" Go design. Rejected on the arithmetic in Context: 6.8× more heap and 9,000,000
traced pointers per mark cycle at target scale, the exact shape Discord's production retrospective
and BigCache's measured 6.2× GC-pause ratio both warn against.

**`map[int32]Node` instead of `[]Node` indexed by handle.** Rejected: pays hashing and probe
indirection on every lookup for no benefit, since dagworker controls handle allocation and can
guarantee density; Go 1.24's own Swiss Table gains (1.5–3% real-world despite 60% microbenchmark)
prove the map was never the actual bottleneck worth chasing here.

**`unique.Handle[string]` (Go 1.23 stdlib interning) instead of a hand-rolled `int32` interner.**
Rejected for this specific role: it gives O(1) *equality* on an opaque, GC-managed handle backed
by weak references, not a dense small integer usable as a direct slice index — the actual property
this design needs.

**`sync.Map` for the sharded node-state table.** Rejected on the measured numbers: 13.7 Mops/s
versus 40.0 Mops/s for 256-way sharding under this workload's write-heavy claim/complete pattern,
because `sync.Map`'s optimization target — read-mostly, append-only key sets — does not match this
access pattern.

**A single global mutex or single global ready-queue.** Rejected outright per the cache-line-
contention-cliff measurements (≥8.3× throughput loss at only 8 cores, worse as core count grows),
independent of any other design choice in this ADR.

## References

- A Guide to the Go Garbage Collector — https://tip.golang.org/doc/gc-guide
- Why Discord is switching from Go to Rust — https://discord.com/blog/why-discord-is-switching-from-go-to-rust
- allegro/bigcache — https://github.com/allegro/bigcache
- Faster Go maps with Swiss Tables — https://go.dev/blog/swisstable; Go 1.24 release notes — https://go.dev/doc/go1.24
- Interning strings in Go — https://commaok.xyz/post/intern-strings/; `unique` package — https://pkg.go.dev/unique
- Generational indices (RustConf 2018) — https://kyren.github.io/2018/09/14/rustconf-talk.html
- RoaringBitmap — https://github.com/RoaringBitmap/roaring
- A Concurrency Cost Hierarchy — https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html
- Evaluating the Cost of Atomic Operations (ETH Zürich) — https://htor.inf.ethz.ch/publications/img/atomic-bench.pdf
- Shard your locks — https://strebkov.dev/posts/shard-your-locks/; go-perfbook — https://github.com/dgryski/go-perfbook/blob/master/performance.md
- puzpuzpuz/xsync (striped `Counter`, `LongAdder` pattern) — https://github.com/puzpuzpuz/xsync
- Sibling ADRs: ADR-0016 (storage port shape — this internal layout never crosses the public
  `Store` interface), ADR-0029 (minimum Go version — Go ≥1.25 tooling this design assumes)
