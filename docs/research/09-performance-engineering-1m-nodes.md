# Performance Engineering and Benchmark Methodology at 1,000,000 Nodes

This dossier covers four things needed to make the "O(1)/O(log n), benchmarked at 1M nodes"
requirement real rather than aspirational: (1) how to lay out a million-node dynamic graph in
Go memory so the garbage collector and the allocator are not the bottleneck, (2) how to make
concurrent access to that graph scale across cores instead of falling off a contention cliff,
(3) how to write Go benchmarks that actually *prove* a complexity bound instead of asserting it
in a docstring, and (4) what throughput ceiling is physically achievable per storage backend, so
the CI assertions are honest about what "fast" means for in-memory vs. Redis vs. Postgres.

---

## Part 1 — In-memory data layout for 1,000,000 nodes

### 1.1 The core mechanism: Go's GC skips memory it can prove is pointer-free

Go's garbage collector is a concurrent, non-generational, tri-color mark-sweep collector. Its
CPU cost is dominated by **marking**: starting from roots, it walks every reachable pointer and
must visit each one to determine liveness. The official GC guide states the mechanism dag-worker-go
needs to exploit directly:

> "Pointer-free values are segregated from other values… it may be advantageous to eliminate
> pointers from data structures that do not strictly need them, as this reduces the [scanning]
> pressure the GC exerts on the program." — [A Guide to the Go Garbage Collector](https://tip.golang.org/doc/gc-guide)

The guide also documents a second, cheaper trick: "The GC will stop scanning values at the last
pointer in the value," so *"it may be advantageous to group pointer fields in struct-typed values
at the beginning of the value."* Concretely, Go's allocator classifies every size class as either
"scan" (contains pointers, must be walked every GC cycle) or "noscan" (pointer-free, the GC marks
the whole span live in O(1) by looking at one bit and never enters it). A slice of `int32`,
`uint8`, or a fixed-size struct of only those types is *noscan*; a slice of `*Node` or of structs
containing a `string`/slice/map/interface field is *scan*, and every element in it is walked on
every mark cycle that includes it.

The GC guide's own cost model is a straight line in the *pointer-bearing* live-heap size, not
in the object count:

> "GC CPU time for cycle N = Fixed CPU time cost per cycle + average CPU time cost per byte ×
> live heap memory found in cycle N" — [gc-guide](https://tip.golang.org/doc/gc-guide)

and separately, for a program running in steady state:

> "Total GC CPU cost = (Allocation rate) / (GOGC / 100) × (Cost per byte) × T" — same source,
> with the explicit trade-off that **"doubling GOGC will double heap memory overheads and roughly
> halve GC CPU cost."**

The load-bearing implication for a 1,000,000-node dynamic DAG: if the graph is represented as
1,000,000 heap-allocated `*Node` objects wired together with pointer slices, the GC's marginal
per-cycle cost scales with the *pointer count* in that live graph, and every full mark cycle
re-walks it — this is recurring, not one-time, cost, and it recurs at a cadence Go fixes
independent of allocation behavior (see below). If the graph is represented as parallel
`int32`/`uint8` arrays indexed by a dense handle, the GC's marginal cost for that data is **zero**
regardless of whether N is 1,000 or 100,000,000, because the spans are noscan.

### 1.2 The Discord case study: this is not a micro-benchmark curiosity, it is a production failure mode

Discord's "Read States" service is the canonical, widely cited proof that this is a real
production concern, not a benchmarking artifact. The service tracked one entry per
user-per-channel in an in-process LRU cache with tens of millions of entries per node, "hundreds
of thousands of cache updates and tens of thousands of database writes per second." Their own
account:

> "The garbage collector had to scan the entire LRU cache in order to determine if the memory was
> truly free from references." … "Go will force a garbage collection run every 2 minutes at
> minimum" — regardless of allocation rate, causing periodic latency spikes even though the code
> had "very few allocations." — [Why Discord is switching from Go to Rust](https://discord.com/blog/why-discord-is-switching-from-go-to-rust)

Two tuning knobs were tried and both failed for structural reasons directly relevant to
dag-worker-go:

- **`GOGC` tuning** did nothing, because "allocation rates were insufficient to trigger more
  frequent collections" — the problem wasn't allocation *rate*, it was that Go's runtime forces a
  GC cycle at least every 2 minutes no matter what, and *that* cycle still has to walk however
  many pointers are live, however large the cache has grown since the last cycle.
- **Shrinking the cache** reduced pause duration but pushed p99 latency up (more cache misses),
  a real trade-off, not a free win.

The fix was rewriting the cache in Rust, which has no periodic full-heap trace at all. The
directly transferable lesson for a Go implementation of dag-worker-go is *not* "rewrite it in
Rust" — it's "don't build the one-object-per-node, pointer-linked cache Discord built." Their own
retrospective and independent Go caches converge on the same countermeasure: store the payload in
pointer-free byte arrays and keep only a handful of pointers (or none) in the hot path. BigCache,
a widely used Go LRU cache built explicitly around this constraint, documents the mechanism and
gives real numbers:

> "If map without pointers in keys and values is used then GC will omit its content." … "Byte
> slices size can grow to gigabytes without impact on performance because GC will only see a
> single pointer to it." At 20,000,000 entries: **GC pause 1.5ms for BigCache vs. 9.3ms for a
> standard `map[string][]byte]`.** — [allegro/bigcache](https://github.com/allegro/bigcache)

That 6.2× GC-pause ratio at 20M entries, from a library whose only difference from a plain
Go map is pointer density in the payload, is the empirical proof that the guide's cost model is
not theoretical.

### 1.3 `map[int32]T` vs. slice-indexed-by-handle vs. open addressing

Three structural choices exist for "look up a node's data given its handle," in increasing order
of applicability to *dense, library-allocated* integer handles (which is what dag-worker-go
controls, since it mints the handles):

| Structure | Lookup cost | Memory / entry overhead | GC cost | When to use |
|---|---|---|---|---|
| `map[int32]T` (Go 1.24 Swiss table) | O(1) amortized, ~1 SIMD group probe | Table kept below ~87% load factor, so real occupancy is N/0.87; group metadata is 1 control byte/slot + 8-slot groups | Scans every group's slots if `T` contains pointers; noscan if `T` is pure scalars, but you still pay hashing + probe indirection | Sparse or foreign key space (e.g. externally supplied node IDs before interning) |
| `[]T` indexed directly by handle | O(1), one bounds check + one array index, **no hashing** | Zero overhead beyond `sizeof(T)` × capacity; no load-factor slack | Same noscan/scan rule as any slice of `T` | **Default for dag-worker-go**: handles are dense, monotonically-issued `int32`s from your own slab allocator |
| Manual open addressing (Robin Hood / linear probing over a flat array) | O(1) amortized, cache-friendlier than chaining, comparable to Swiss table for numeric keys | Similar load-factor slack to Swiss tables (typically 70–90%) | Same rule | Only worth hand-rolling if you need a probing strategy Go's map doesn't offer (e.g. SIMD-free embedded targets, or custom eviction) — for dag-worker-go, don't; use the slice-by-handle path instead |

Go 1.24 rewrote the built-in map from a bucket-with-8-entries-and-an-overflow-pointer design to a
**Swiss Table**: groups of 8 slots with a 64-bit control word holding one metadata byte per slot
(empty / deleted / 7 bits of hash for occupied), enabling SIMD-style parallel comparison across
the group in one instruction:

> "In microbenchmarks, map operations are up to 60% faster than in Go 1.23... in full application
> benchmarks, we found a geometric mean CPU time improvement of around 1.5%." — [Faster Go maps
> with Swiss Tables](https://go.dev/blog/swisstable)

> "The new builtin map implementation based on Swiss Tables... lowered CPU overhead by about 2 to
> 3 percent on average across a representative benchmark suite." — [Go 1.24 release
> notes](https://go.dev/doc/go1.24)

The gap between "60% faster in microbenchmarks" and "1.5–3% faster in real applications" is the
single most important number in this section: **it tells you the map itself was never your
bottleneck in a real workload** — allocation, GC, and pointer chasing elsewhere in the program
dominate. This is the argument for skipping `map[int32]*Node` entirely in favor of `[]Node`
indexed by handle: you get the O(1) lookup *without paying for a hash table at all*, and you get
it whether you're on Go 1.23's bucket map or Go 1.24's Swiss table, because you never call the map
runtime in the hot path.

### 1.4 Interning string node IDs to `int32` handles

Host programs will supply human-meaningful node identifiers (UUIDs, `"job:42:step:3"`, etc.).
Those strings must never be the primary key inside the DAG engine — every comparison, every hash,
every GC scan through a `string` header costs more than a 4-byte integer compare. The
library-internal contract should be: **string in at the boundary, `int32` handle everywhere
inside.** The pattern (originally documented by a Google Go engineer) is a single intern map plus
a compiler trick that avoids allocating a throwaway string just to probe the map:

```go
type Interner struct {
    mu      sync.RWMutex
    toID    map[string]int32
    toStr   []string // reverse lookup, index == handle
}

// Intern returns the int32 handle for name, allocating a new one if unseen.
// b may be a []byte view into a request buffer; Go's compiler special-cases
// map[string(b)] lookups to avoid allocating a string just to probe.
func (in *Interner) Intern(b []byte) int32 {
    in.mu.RLock()
    if id, ok := in.toID[string(b)]; ok { // no allocation on this line
        in.mu.RUnlock()
        return id
    }
    in.mu.RUnlock()

    in.mu.Lock()
    defer in.mu.Unlock()
    if id, ok := in.toID[string(b)]; ok { // re-check after acquiring write lock
        return id
    }
    s := string(b) // one real allocation, owned by the interner forever
    id := int32(len(in.toStr))
    in.toID[s] = id
    in.toStr = append(in.toStr, s)
    return id
}
```

> "Map operations whose key is a converted byte slice don't actually generate a new string to use
> during the lookup." — [Josh Bleecher Snyder, Interning strings in Go](https://commaok.xyz/post/intern-strings/)

Go 1.23 shipped this pattern as a standard-library primitive, `unique.Handle[T]`, for the general
case of interning any comparable value:

> "Handles are equal if and only if the values used to produce them are equal." … "The comparison
> of two handles is trivial and typically much more efficient than comparing the values used to
> create them." — [`unique` package docs](https://pkg.go.dev/unique)

`unique.Handle[string]` is a good fit if dag-worker-go wants pointer-equality-fast comparisons
*and* is fine with the handle being an opaque, non-numeric, GC-managed value (it's backed by weak
references and a runtime cleanup queue, not by an `int32`). For dag-worker-go's stated
requirement of `int32`-handle-driven O(1) array indexing, roll the interner above rather than use
`unique` directly — `unique.Handle` gives you O(1) *equality*, not a dense small integer you can
index a slice with.

**One intern table only.** The requirement text ("interning string IDs to int32 handles with one
hash table") matters because every additional map you keep keyed by the *string* (e.g. a second
map for node metadata, a third for lock ownership) re-pays the string-hash cost and re-introduces
a pointer-bearing key into GC-scanned memory. Everything downstream of interning should be keyed
by the `int32` handle into flat slices.

### 1.5 Bitsets for ready/blocked membership

A node's "is it ready" and "is it blocked" status is a single bit per node, and testing/setting/
clearing that bit for one handle is genuinely O(1):

```go
type Bitset []uint64

func (b Bitset) Set(h int32)   { b[h>>6] |= 1 << uint(h&63) }
func (b Bitset) Clear(h int32) { b[h>>6] &^= 1 << uint(h&63) }
func (b Bitset) Test(h int32) bool { return b[h>>6]&(1<<uint(h&63)) != 0 }
```

At 1,000,000 nodes this is `1,000,000 / 8 = 125,000` bytes (≈122 KiB) per bitset, entirely
pointer-free — a `[]uint64` is noscan. **Be precise about what stays O(1) and what doesn't**:
counting how many nodes are currently ready (`popcount` over the whole array via
`math/bits.OnesCount64` per word) is O(N/64), not O(1) — a 64× constant-factor win over a naive
per-node scan, but still linear in N, and a benchmark that reports this operation must not claim
O(1) for it. If the ready set is small relative to N (e.g., a few thousand ready nodes out of a
million total, which is the common case for a wide-but-shallow DAG), a compressed bitmap changes
the asymptotics of iteration and cardinality to be proportional to the *population*, not to N.
[RoaringBitmap/roaring](https://github.com/RoaringBitmap/roaring) is the standard Go
implementation (used in production by InfluxDB, Bleve, and DataDog): it splits the key space into
2^16-element chunks and picks, per chunk, an array container (sorted `uint16`s) below roughly
4,096 set bits, or a plain 8 KiB bitmap container above that threshold, with a run-length container
for contiguous ranges — giving near-array-cost iteration when the set is sparse and near-bitmap
cost only when it's actually dense. The concurrency caveat is explicit and matters for a
DAG engine with concurrent readers: **"Bitmaps are left unsynchronized for performance"** — wrap
per-shard roaring bitmaps in the same mutex/shard scheme used for the rest of that shard's state,
never share one unsynchronized instance across goroutines.

### 1.6 Free-list slab allocation with generation counters

Dynamic add/remove means handles get reused, and a reused handle must never be mistaken for the
node that previously occupied that slot — classically solved with a **generational index**
(popularized for exactly this problem in game-engine entity systems):

> "This pattern... is widely known in the gamedev [world]" — pairing an `index` into a dense
> array with a `generation` counter that increments every time that slot is freed and reused; a
> stale handle's generation no longer matches the slot's current generation, so an access through
> it fails cleanly instead of silently returning the wrong node. — summarized from Catherine
> West's RustConf 2018 keynote, via [kyren's writeup](https://kyren.github.io/2018/09/14/rustconf-talk.html)

```go
type Handle struct {
    Index uint32
    Gen   uint32
}

type Slab[T any] struct {
    items []T
    gens  []uint32
    free  []uint32 // LIFO stack of reclaimed indices — reuse is cache-hot
}

func (s *Slab[T]) Alloc(v T) Handle {
    if n := len(s.free); n > 0 {
        idx := s.free[n-1]
        s.free = s.free[:n-1]
        s.items[idx] = v
        return Handle{Index: idx, Gen: s.gens[idx]}
    }
    idx := uint32(len(s.items))
    s.items = append(s.items, v)
    s.gens = append(s.gens, 0)
    return Handle{Index: idx, Gen: 0}
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

func (s *Slab[T]) Get(h Handle) (T, bool) {
    if int(h.Index) >= len(s.items) || s.gens[h.Index] != h.Gen {
        var zero T
        return zero, false
    }
    return s.items[h.Index], true
}
```

Packing `Handle` into a single `uint64` (`gen<<32 | index`) makes it CAS-friendly for
lock-free publication of newly allocated handles, and keeps every public handle dag-worker-go
hands to a host program an opaque, copyable, comparable 8-byte value with no pointer inside it —
itself a noscan type, so a channel or queue full of pending handles costs the GC nothing to scan.

### 1.7 The byte-level budget for 1,000,000 nodes, average out-degree 3

Assume 1,000,000 nodes and 3,000,000 edges (out-degree 3 on average; if reverse/parent edges are
also tracked, that's 3,000,000 more, unless you store the graph as a single directed edge list and
compute in-edges by index at ingestion). Two designs, same graph:

**Design A — pointer-heavy array-of-structs** (the "obvious" Go design):

```go
type Node struct {
    ID       string   // 16B header {data*, len}
    Status   uint8    // 1B (+7B padding to keep 8B alignment)
    Children []*Node  // 24B header {data*, len, cap}
    Parents  []*Node  // 24B header
}
```

| Component | Per-node cost | At N = 1,000,000 |
|---|---|---|
| `Node` struct header (16+8+24+24, rounds to Go size class 80B) | 80 B | 80 MB |
| `Children` backing array (avg 3 × 8B pointer, size class 32B) | 32 B | 32 MB |
| `Parents` backing array (avg 3 × 8B pointer, size class 32B) | 32 B | 32 MB |
| **Total heap footprint** | **144 B** | **≈137 MiB** |
| **Pointer words the GC must trace per mark cycle** (ID data-ptr + Children data-ptr + Parents data-ptr on the struct itself, plus 3 slots each inside Children/Parents backing arrays) | 3 + 3 + 3 = 9 words | **9,000,000 pointers (≈72 MB of pure pointer data), every mark cycle, in random-access order** |

**Design B — struct-of-arrays with `int32` handles, CSR-style edge storage:**

```go
type Graph struct {
    status    []uint8  // 1 node status byte
    childOff  []int32  // CSR offsets, len N+1
    childIdx  []int32  // flattened edge targets, len = total edges
    gen       []uint32 // generation counters for the slab allocator (§1.7)
    // string IDs, if retained, live only in the Interner (§1.5), not here
}
```

| Component | Size at N = 1,000,000, E = 3,000,000 |
|---|---|
| `status` (`[]uint8`) | 1,000,000 B ≈ 0.95 MiB |
| `childOff` (`[]int32`, N+1) | 4,000,004 B ≈ 3.81 MiB |
| `childIdx` (`[]int32`, E) | 12,000,000 B ≈ 11.44 MiB |
| `gen` (`[]uint32`, N) | 4,000,000 B ≈ 3.81 MiB |
| **Total heap footprint** | **≈21,000,004 B ≈ 20 MiB** |
| **Pointer words the GC must trace per mark cycle** | **A handful — just the slice headers of `Graph` itself (4 slices × 3 words). Zero inside the payload arrays; they are noscan.** |

The two designs hold the same graph. Design A costs **6.8× more heap** and forces the GC to
chase **9,000,000 pointers per mark cycle** (each a potential cache miss, since heap-allocated
`Node`s and their backing arrays are not laid out contiguously — this is exactly the "linked
lists and trees are... more difficult for the GC to walk in parallel" cost the GC guide names).
Design B's mark-cycle cost for this data is a constant independent of N. This is the concrete,
arithmetic version of the BigCache 1.5ms-vs-9.3ms measurement in §1.2, scaled to the specific
shape (1M nodes, out-degree 3) dag-worker-go must benchmark against.

---

## Part 2 — Concurrency at scale

### 2.1 The cache-line contention cliff, with numbers

A single atomic counter or mutex shared by every goroutine works fine until enough goroutines
touch it concurrently, at which point throughput doesn't degrade gracefully — it falls off a
cliff, because contended atomics don't get slower per-instruction, they **serialize** through the
cache-coherence protocol:

> "A contended atomic on a hot line costs ~100 ns per op; an uncontended one costs ~1–5 ns...
> Atomic operations are not slow because the instruction itself is slow. They are slow because,
> under contention, they serialise across cores through the coherence protocol." — [Travis Downs,
> A Concurrency Cost Hierarchy](https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html)

> "All the considered architectures have significantly lower bandwidth in a contended execution
> of atomic operations than in a non-contended case." — [Evaluating the Cost of Atomic Operations
> on Modern Architectures](https://htor.inf.ethz.ch/publications/img/atomic-bench.pdf) (ETH Zürich)

1–5 ns per uncontended op is 200M–1B raw ops/sec per core at the hardware level; 100 ns under
contention is a hard ceiling of **~10M ops/sec in aggregate across every core touching that one
cache line, no matter how many cores you add** — a 20–100× drop, and the drop is invisible in
single-threaded profiling because each individual instruction still "completes," it just makes
every other core wait for the cache-line bounce. This is the mechanism behind Amdahl's-law-style
collapse in any design with one hot atomic (a single global sequence counter, a single global
ready-queue length, a single global "nodes completed" tally).

A Go-level illustration, on real hardware, of the same cliff for a *map* rather than a bare
atomic:

> Testing shard counts from 1 to 4,096 on an 8-core slice of a 20-core i7-14700K, write-heavy
> workload: **plain `sync.Mutex`: 4.81 Mops/s. 256-way sharded map: 40.0 Mops/s (8.3× faster).
> `sync.Map`: 13.7 Mops/s** (worse than sharding because `sync.Map` is optimized for read-mostly,
> append-only key sets, not general read/write). — [Shard your locks: benchmarking 6 Go cache
> designs](https://strebkov.dev/posts/shard-your-locks/)

| Shards | Read-only (Mops/s) | Write-heavy (Mops/s) |
|---|---|---|
| 1 (plain mutex) | 5.95 | 4.81 |
| 256 | 47.6 | 40.0 |
| `sync.Map` | 33.3 | 13.7 |

The jump from 1 to 256 shards was 9× at 8 cores; the gain flattens well before 4,096 — "256... sits right in the knee of the performance curve," per the same source.

### 2.2 Lock striping: how many shards

There is no universal formula, but the converging practice across libraries is: **over-provision
shards well beyond `GOMAXPROCS`, as a power of two, and cap it** — because more shards cost a
fixed, tiny amount of extra memory (one mutex + queue header per shard) but keep paying off in
reduced collision probability until the shard count is large enough that two goroutines rarely
land on the same shard even under bursty, non-uniform key distributions. `dgryski/go-perfbook`
gives the canonical padded-stripe pattern to prevent *false sharing* between adjacent shard locks
on the same cache line:

```go
var stripe [8]struct {
    sync.Mutex
    _ [7]uint64 // pad this shard's mutex out to a full 64B cache line
}
```

> "cache-line bouncing between processors" is exactly what this padding prevents — without it,
> two logically independent shards can still contend because their mutexes share a physical
> cache line. — [dgryski/go-perfbook](https://github.com/dgryski/go-perfbook/blob/master/performance.md)

For dag-worker-go, a pragmatic default is:

```go
shards := nextPow2(8 * runtime.GOMAXPROCS(0))
if shards > 256 {
    shards = 256
}
```

— i.e., 8×`GOMAXPROCS`, rounded up to a power of two (so a handle's shard index is a cheap
`handle & (shards-1)` mask instead of a modulo), capped at 256 based on the measured knee above.
This must be **re-validated by dag-worker-go's own benchmark suite on its own target hardware**
(§3) rather than trusted as a universal constant — the 256 number came from one 8-to-20-core Intel
desktop part, not a server-grade many-core box, and the right constant is workload- and
hardware-dependent.

### 2.3 Per-shard ready queues, not one global queue

If node handles are sharded (§2.2) for the state-transition table, the "this node must now be
taken by a worker" notification queue should be sharded **the same way**, keyed by the same
handle-derived shard index, so a status transition and its resulting ready-notification are both
local to one shard's lock — no cross-shard coordination on the hot path. A single global
subscriber-facing stream is then a fan-in across shard queues, which is O(shards), not O(N): each
shard queue is drained by one fan-in goroutine per shard (or a `select` over `shards` channels),
merged into the single public stream the host program subscribes to. This keeps the *write* side
(a worker goroutine marking a node ready) shard-local and cheap, while the *read* side (a
subscriber wanting one linear stream) pays a bounded, shard-count-sized fan-in cost, never an
N-sized one.

### 2.4 Sharded counters: the do-not-share-one-hot-atomic rule

Every aggregate counter dag-worker-go is tempted to expose (total nodes, total ready, total
in-flight) is a candidate for the same contention cliff as §2.1 if implemented as one
`atomic.Int64` incremented by every worker completion across every shard. The standard
countermeasure, ported from Java's `LongAdder` and Linux's per-CPU counters, is a striped counter:
one cell per shard (or per-P), each cell padded to its own cache line, summed only on read:

> "Modern concurrent systems frequently employ striped counters to mitigate the overhead of
> heavily contended atomic operations. In Java, this idea is used in `LongAdder`, which
> dynamically distributes updates across an array of independent memory structures... Cache line
> padding ensures each atomic value occupies its own cache line... so increments by different
> processes do not invalidate each other's cache entries." — summarized from public
> descriptions of striped-counter designs; Go's [`puzpuzpuz/xsync`](https://github.com/puzpuzpuz/xsync)
> ships a ready-made `Counter` type built on exactly this `LongAdder` pattern, alongside a `Map`
> whose "Get operations are obstruction-free and involve no writes to shared memory, hence no
> mutexes or any other sort of locks" for the read path.

The rule for dag-worker-go's implementation: **the write path for any statistic increments a
per-shard (or per-P) cell; only a `Stats()`/metrics-export call sums across cells, and that sum is
O(shards), paid rarely, not O(1)-but-contended, paid on every completion.** Never let a
per-node-completion code path touch a single shared `atomic.Int64` — that one line is exactly the
"one hot atomic" the ETH and Travis Downs measurements above show collapsing throughput by 20–
100× the moment more than a handful of cores contend on it concurrently.

---

## Part 3 — Proving the complexity claim empirically

A comment claiming "O(1)" is worth nothing without a benchmark that would *fail* if the claim
were false. The methodology below is designed around that falsifiability requirement.

### 3.1 Shape of a complexity-proving benchmark

Run the same operation at N = 10³, 10⁴, 10⁵, 10⁶ (and, budget permitting, 10⁷ to see whether the
curve is still flat past the point where the working set exceeds LLC), and compare **ratios of
per-operation cost across N**, not absolute numbers:

```go
func BenchmarkMarkReady(b *testing.B) {
    for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
        b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
            g := buildTestGraph(n, avgOutDegree3)
            handles := sampleHandles(g, 1024) // fixed sample, reused across iterations
            b.ReportAllocs()
            i := 0
            for b.Loop() {
                g.MarkReady(handles[i%len(handles)])
                i++
            }
        })
    }
}
```

A genuinely O(1) operation should show `ns/op` essentially flat across the four sizes, modulo
cache-hierarchy effects (see §3.4). A genuinely O(log n) operation (e.g. a per-shard balanced
structure, or the interner's map growth curve) should show `ns/op` growing roughly with `log N` —
from `N=1e3` to `N=1e6` that's a 3-decade increase in N, so `log₂(1e6/1e3) ≈ 10`, meaning an
O(log n) operation's cost should grow by a small constant multiple, not the ~1000× a linear
operation would show.

### 3.2 `b.Loop` (Go 1.24) vs. the old `b.N` loop, and why it matters here specifically

The classic `for i := 0; i < b.N; i++ { fn() }` pattern is unsound the moment `fn()`'s result is
unused: the compiler can prove the call has no observable effect and delete it, so the benchmark
silently measures nothing.

```go
func BenchmarkIsCondWrong(b *testing.B) {
  for range b.N {
    isCond(201) // return value discarded → compiler may eliminate the call entirely
  }
}
```

> "The compiler eliminates it as dead code" … Go 1.24's `testing.B.Loop` fixes this: "the Go
> compiler now detects loops where the condition is just a call to `testing.B.Loop` and prevents
> dead code elimination within the loop," implemented by "disallowing inlining into the body of
> such a loop." — [More predictable benchmarking with testing.B.Loop](https://go.dev/blog/testing-b-loop)

`b.Loop` also folds `ResetTimer`/`StopTimer` semantics into the loop boundary automatically, so
one-time setup before the loop (building the 1,000,000-node test graph) is never counted:

```go
func BenchmarkMarkReady(b *testing.B) {
    g := buildTestGraph(1_000_000, avgOutDegree3) // excluded from timing automatically
    for b.Loop() {
        g.MarkReady(someHandle)
    }
}
```

For a graph library specifically, this matters more than usual: graph-construction cost at
N=1,000,000 is itself substantial, and an accidental measurement of "build a million-node graph
once per `b.N` iteration" rather than "call the operation under test" is a realistic mistake that
`b.Loop`'s single-execution-of-setup semantics prevents by construction.

### 3.3 Statistical validity: benchstat and Georges et al.

A single run of a microbenchmark is a sample from a noisy distribution (thermal throttling,
scheduler jitter, background processes, JIT-equivalent Go inlining decisions that can vary by
build). The methodological baseline for "how many runs, and how do you compare them honestly" is:

> "Java performance is difficult to benchmark because it's affected by... non-determinism at
> run-time" from JIT compilation, thread scheduling, garbage collection, and system effects — the
> paper's core recommendation is to run **multiple independent invocations**, report **confidence
> intervals for the mean**, and explicitly reject **"best of N"** reporting as invalid because it
> discards exactly the variance information needed to know whether an observed difference is
> real. — Georges, Buytaert & Eeckhout, [Statistically Rigorous Java Performance
> Evaluation](https://dri.es/files/oopsla07-georges.pdf), OOPSLA 2007

Go's own tooling operationalizes this directly. `benchstat` computes non-parametric statistics
(median + confidence interval) across repeated `go test -bench` runs and uses a proper hypothesis
test rather than eyeballing:

> "benchstat uses non-parametric statistics: median for summaries, and the Mann-Whitney U-test for
> A/B comparisons." … "Each benchmark should be run at least 10 times... Pick a number of
> benchmark runs (at least 10, ideally 20) and stick to it." — [benchstat
> docs](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)

Concretely, for dag-worker-go's CI:

```bash
go test -run '^$' -bench BenchmarkMarkReady -benchtime 200x -count 20 ./... > new.txt
benchstat old.txt new.txt   # or: benchstat new.txt to just get medians+CIs for one run
```

`-benchtime 200x` (a fixed iteration count, not a fixed wall-clock duration) makes runs
comparable across machines of different speed — a time-based `-benchtime=1s` would run *fewer*
iterations on a slow CI runner, which changes the noise profile of the resulting sample.

### 3.4 CI noise: why absolute nanosecond thresholds lie, and the ratio guard

CI machines are shared, virtualized, and subject to CPU frequency scaling — an absolute assertion
like "`MarkReady` must take < 50ns" will flake on a throttled or noisy-neighbor runner regardless
of whether the algorithm is correct. Two standard countermeasures:

1. **Pin CPU frequency and serialize benchmark runs.** Austin Clements' `perflock` exists
   specifically for this:

   > "perflock is a simple locking wrapper for running benchmarks on shared hosts that acquires a
   > system-wide lock while running commands... [and] change[s] the CPU governing and frequency
   > scaling settings to try to make the machine behave more consistently." —
   > [aclements/perflock](https://github.com/aclements/perflock)

   Redis's own official benchmark methodology does the same thing at the OS level, and is worth
   quoting as a model of the discipline: "disabling Intel HT Technology, disabling CPU Frequency
   scaling, with all configurable BIOS and CPU system settings set to performance... pinned to
   specific physical cores." — [Redis benchmark
   docs](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/)

2. **Assert a ratio, not an absolute duration.** Since cache-hierarchy effects are real (a
   1,000,000-entry working set genuinely doesn't fit in L2/L3 the way a 1,000-entry one does), a
   *true* O(1) algorithm's `ns/op` can legitimately grow by a small, bounded factor (typically
   2–5×) purely from L1→LLC→DRAM latency differences as N grows, even though its instruction count
   per operation is constant. The CI guard should therefore assert the **ratio** stays under a
   generous bound that separates "flat, allowing for memory hierarchy" from "actually linear or
   worse":

```go
func TestComplexity_MarkReady_IsConstant(t *testing.T) {
    costAt := func(n int) float64 {
        g := buildTestGraph(n, avgOutDegree3)
        h := sampleHandles(g, 1)[0]
        res := testing.Benchmark(func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                g.MarkReady(h)
                g.ClearReady(h) // undo, so repeated iterations don't change graph state
            }
        })
        return float64(res.T.Nanoseconds()) / float64(res.N)
    }

    small := costAt(1_000)
    large := costAt(1_000_000)
    ratio := large / small

    // A flat O(1) op can legitimately cost more at 1M due to cache-hierarchy
    // effects (bigger working set, more TLB misses) — bound generously, not tightly.
    const maxAllowedRatio = 5.0
    if ratio > maxAllowedRatio {
        t.Fatalf("MarkReady cost ratio 1e6/1e3 = %.2fx, want <= %.1fx (got %.1fns -> %.1fns) — looks non-constant",
            ratio, maxAllowedRatio, small, large)
    }
}
```

This is deliberately a plain `go test` (not a benchmark harness invocation), runnable in normal
CI, self-contained, and immune to absolute-timing flakiness because it only compares two numbers
measured on the *same* machine in the *same* run. Use `testing.Benchmark` directly (as above) for
this self-checking form; use the full `b.Loop`/`benchstat` machinery from §3.2–3.3 for
human-reviewed performance-regression tracking across commits, which is a different question
("did this PR make it slower?") from complexity verification ("does this scale correctly?").

For O(log n) operations, replace the bound with the expected log-growth factor plus slack, e.g.
`maxAllowedRatio := 2*math.Log2(1e6/1e3) = ~20`, derived rather than guessed, so the test document
self-explains why that number was chosen.

### 3.5 pprof, `runtime/metrics`, and the execution tracer as corroborating evidence

A ratio guard proves the *time* complexity claim; three further tools prove the *mechanism*
claimed in Part 1 (that the SoA design is GC-free) is actually what's happening in the binary
under test, not an accident of a particular benchmark shape:

- **`go tool pprof`** on a CPU profile (`go test -bench . -cpuprofile cpu.out`) shows where time is
  spent; a healthy `MarkReady` profile should show no `runtime.scanobject`/`runtime.gcDrain`
  frames at all in the hot path, since the payload arrays are noscan.
- **`runtime/metrics`** gives programmatic, stable-named access to exactly the GC-scan bytes
  claimed to be flat in §1.7:

  ```go
  samples := []metrics.Sample{
      {Name: "/gc/scan/heap:bytes"},
      {Name: "/sched/pauses/total/gc:seconds"},
  }
  metrics.Read(samples)
  ```

  Snapshotting `/gc/scan/heap:bytes` before and after populating a 1,000,000-node graph is a
  direct, numeric confirmation of the Part 1 claim: for the SoA design this delta should be tiny
  (just the interner's string-keyed map, if any strings are retained); for the pointer-heavy AoS
  design it will be tens of megabytes, matching the §1.7 arithmetic. This package is the
  documented, stable replacement for `runtime.ReadMemStats` precisely because its metric set "can
  evolve across Go versions" without breaking callers — [`runtime/metrics`
  docs](https://pkg.go.dev/runtime/metrics).
- **`go tool trace`** (via `go test -trace trace.out`, or the `/debug/pprof/trace` HTTP handler)
  gives a scheduler-latency view — "time goroutines wait to be scheduled" — which is the right
  tool for validating Part 2's sharding claims: under a sharded design, worker goroutines should
  show short, uniform scheduling latency; under a single-mutex design, the trace view will show
  goroutines piling up waiting on the same lock, visible as a widening latency distribution as
  concurrent load increases.

---

## Part 4 — Realistic throughput ceilings per backend

The point of this section is to stop the benchmark suite from asserting numbers that are
physically impossible for a given backend, and to make pipelining/batching a *requirement*, not
an optimization, for any backend involving a network round trip.

### 4.1 In-memory (single process, default backend)

Bounded only by CPU, cache locality, and the contention behavior from Part 2. With the
sharded-noscan design from Parts 1–2, a single core sustains tens of millions of `MarkReady`/
`Claim` operations per second in isolation (consistent with the sharded-map numbers in §2.1: 40–
47 Mops/s aggregate across 8 cores for a full map-based workload, meaning a much simpler
handle-indexed slice op should be faster still). The benchmark suite should assert this backend's
`ns/op` **ratio** across N (§3), not a raw ops/sec floor, since raw ops/sec is CI-hardware-
dependent — but it can safely assert a *relative* floor like "in-memory `Claim` must be at least
100× faster than the Redis-backend `Claim` measured in the same CI run," which is portable across
machines because both sides scale with the same hardware.

### 4.2 Redis over loopback: the RTT tax and the pipelining win

**The arithmetic.** One non-pipelined round trip at RTT microseconds caps a single connection at
`1,000,000 / RTT` ops/sec. At the low end of realistic loopback RTT (Unix domain socket, ~30 µs
per Redis's own numbers below) that's ~33,000 ops/sec; at a more typical TCP-loopback effective
RTT (~100 µs once syscall/epoll/context-switch overhead is included) that's ~10,000 ops/sec per
connection — this is exactly the 10–20k ops/sec-per-connection ceiling the requirement names, and
it is a **hard physical floor for any unpipelined design**, not a tuning problem.

Real measured numbers from Redis's own benchmark documentation, all on loopback:

| Mode | Result |
|---|---|
| `SET`/`LPUSH`, no pipelining, `-t set,lpush -n 100000` | SET: 180,180 req/s, p50 = 0.143 ms; LPUSH: 188,324 req/s, p50 = 0.135 ms |
| `SET` against 100,000 random keys, 1,000,000 total ops, 50 clients, no pipelining | 72,144.87 req/s; 99.76% ≤ 1 ms |
| `SET`/`GET`, **pipeline depth 16** (`-P 16`) | **SET: 1,536,098.25 req/s, p50 = 0.479 ms; GET: 1,811,594.25 req/s, p50 = 0.391 ms** |

— [Redis benchmark docs](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/)

Pipelining 16 requests per round trip bought **8.5×–10× throughput** (180K → 1.5M for SET) at the
cost of *higher* per-request p50 latency (0.143 ms → 0.479 ms) — the server now waits to fill/
drain a batch before replying to any single request in it. This is the batch-size knee in
concrete numbers: **throughput scales with pipeline depth roughly up to the point where per-batch
latency starts to matter to the caller**, not indefinitely (very deep pipelines risk large
send/receive buffers and head-of-line blocking for latency-sensitive callers). Two more relevant
facts from the same source: Unix domain sockets get "around 50% more throughput than the TCP/IP
loopback on Linux," and that gap *shrinks* as pipeline depth grows, because a longer pipeline
amortizes the per-syscall cost that the socket-type difference is mostly measuring in the first
place.

**Recommendation for dag-worker-go's Redis backend**: never issue the per-node claim/ack as an
unpipelined round trip in a hot loop; batch a worker's "give me up to K ready nodes" as one
pipelined/scripted call. A `Claim` that must also enforce a per-node timeout atomically (set
status to in-progress *and* set an expiry, or fail if another instance already claimed it) is a
textbook use for a single Lua script executed via `EVAL`, which Redis guarantees runs atomically
and which costs exactly one round trip regardless of how many keys it touches:

```lua
-- KEYS[1] = ready-set key (sorted set or list of ready node handles, per shard)
-- ARGV[1] = worker id, ARGV[2] = now (unix ms), ARGV[3] = timeout ms, ARGV[4] = batch size
local claimed = {}
for i = 1, tonumber(ARGV[4]) do
    local handle = redis.call('LPOP', KEYS[1])
    if not handle then break end
    local statusKey = 'node:' .. handle .. ':status'
    redis.call('HSET', statusKey, 'state', 'in-progress', 'owner', ARGV[2], 'deadline', tonumber(ARGV[2]) + tonumber(ARGV[3]))
    table.insert(claimed, handle)
end
return claimed
```

This turns a batch of K claims into one round trip instead of K, moving the workload from the
10–20k ops/sec unpipelined ceiling straight into the >1M ops/sec pipelined regime measured above.

### 4.3 Postgres over loopback: row-at-a-time vs. batched/COPY

Local Unix-socket round-trip time to Postgres is on the order of 0.1 ms, and "a trivial `SELECT`
can take in the order of 0.1ms to execute server-side" as well, so an unbatched write path pays
roughly 0.2–0.5 ms per row purely in round-trip and parse/plan overhead before any actual I/O.
Measured throughput at various batch sizes, local Unix socket (`pgx`, 3-column table):

| Rows/batch | `CopyFrom` (rows/s) | `SendBatch` (rows/s) | Multi-row `INSERT` (rows/s) | One-by-one (latency) |
|---|---|---|---|---|
| 5 | 11,900 | 7,350 | 9,800 | 3.8 ms |
| 50 | 61,700 | 26,300 | 35,700 | 37 ms |
| 500 | 156,200 | 41,700 | 58,100 | 370 ms |
| 5,000 | 277,800 | 52,600 | 80,600 | 3.7 s |
| 50,000 | 357,100 | 56,800 | n/a (hits 65,535-param limit) | 37 s |

— [pgx Bulk Insert Showdown](https://goldlapel.com/grounds/go-postgres/pgx-bulk-insert-benchmarks)

Two things this table proves numerically:

1. **The batch-size knee.** Going 5→50 rows (10×) gets CopyFrom a 5.2× throughput gain; 50→500
   gets 2.5×; 500→5,000 gets 1.78×; 5,000→50,000 gets only 1.29×. Each further 10× in batch size
   buys a shrinking multiple — the round-trip cost that batching amortizes is being divided by an
   ever-larger denominator, so the marginal win falls off exactly as queueing-theory intuition
   predicts. The practical knee for this workload is around 500–5,000 rows: past that, `CopyFrom`
   is already within ~30% of its asymptote.
2. **Batching matters more, not less, as network distance grows.** At 67 ms simulated cross-region
   RTT, 500-row `CopyFrom` finished in 71 ms (dominated by the *one* round trip) versus 33.5
   *seconds* for one-by-one inserts of the same 500 rows — a 472× difference — because one-by-one
   pays the 67 ms round trip 500 times. Even purely on loopback (≈0.1 ms RTT), `CopyFrom` is still
   18× faster than one-by-one at 5,000 rows (18 ms vs. 3.7 s) purely from parse/plan/round-trip
   overhead, before counting any actual disk I/O difference between `COPY`'s dedicated ring-buffer
   path and row-by-row WAL writes.

**Recommendation**: dag-worker-go's Postgres backend should expose a batched claim primitive
analogous to the Redis Lua script, built on `SELECT ... FOR UPDATE SKIP LOCKED` plus a bulk
`UPDATE ... RETURNING`, so one instance's poll for "give me up to K ready nodes" is one round trip
regardless of K:

```sql
WITH claimed AS (
    SELECT handle
    FROM   nodes
    WHERE  scope = $1 AND status = 'ready'
    ORDER  BY priority, handle
    FOR UPDATE SKIP LOCKED
    LIMIT  $2                      -- batch size K
)
UPDATE nodes
SET    status = 'in-progress', owner = $3, deadline = now() + $4::interval
FROM   claimed
WHERE  nodes.handle = claimed.handle
RETURNING nodes.handle, nodes.payload;
```

`SKIP LOCKED` is what lets multiple concurrent library instances (the multi-instance requirement)
poll the same table without blocking on each other's in-flight claims — each instance's query
simply skips rows another instance already has row-locked, which is the standard Postgres
work-queue pattern and composes directly with the batching argument above: it's one round trip
that both claims K rows *and* resolves cross-instance contention in the same statement.

### 4.4 What the benchmark suite should actually assert

Tie Part 3's methodology and Part 4's physics together into one rule: **assert ratios, never
absolutes, and make every ratio dimensionless so it is portable across CI hardware.**

| Assertion | Why an absolute number would be wrong |
|---|---|
| `cost(N=1e6) / cost(N=1e3) <= 5` for any claimed O(1) op (§3.4) | Absolute ns/op depends on CI CPU speed, which varies run to run |
| Pipelined-Redis throughput ≥ 20× unpipelined-Redis throughput, measured in the same run | Absolute ops/sec depends on host RTT and CPU; the *ratio* between two modes on the same machine does not |
| Batched-Postgres (`CopyFrom`/bulk `UPDATE...RETURNING`) throughput ≥ 15× row-by-row, measured in the same run | Same reasoning; §4.3's own data shows 18×–472× depending on RTT, so 15× is a safe, conservative floor |
| In-memory backend ≥ 100× faster than Redis backend, ≥ 300× faster than Postgres backend, same run | Encodes the *ordering* of ceilings (no network < loopback network < loopback network + durability), which is architecturally guaranteed regardless of absolute CI speed |
| Redis/Postgres backend absolute ops/sec | **Do not assert** in CI as a hard gate — assert it in a nightly/perf-tracked job via `benchstat` trend comparison (§3.3) instead, where a human reviews drift rather than a flaky CI gate failing on noisy-neighbor hardware |

---

## Recommendations for dag-worker-go

1. **Never allocate one heap object per node.** Represent the graph as struct-of-arrays keyed by
   a library-minted, dense `int32` handle (slab-allocated with a generation counter, §1.7); this
   is the single highest-leverage decision in the whole design and the one the Discord case study
   (§1.2) and the BigCache numbers (§1.2) most directly validate.
2. **Intern every externally supplied string ID to an `int32` handle exactly once**, at the
   ingestion boundary, through one `Interner` (§1.5); never key a second internal map by the raw
   string.
3. **Use plain slices indexed by handle, not `map[int32]T`, for anything on the hot path.** Reach
   for a hash map only at the one place a string or foreign key genuinely needs to become a
   handle (the interner itself), where Go 1.24's Swiss table map is already the right tool with no
   extra code (§1.3–1.4).
4. **Shard everything that's mutated concurrently** — status table, ready/blocked bitsets, ready
   queues, and aggregate counters — using the same handle-derived shard index throughout, sized to
   `min(256, nextPow2(8×GOMAXPROCS))` as a starting point to be re-measured on target hardware
   (§2.2), with cache-line-padded shard structs to prevent false sharing (§2.1).
5. **Ban the single shared `atomic.Int64` for any per-completion counter.** Every hot-path counter
   must be per-shard/per-P and summed only on read (§2.4).
6. **Every complexity claim in the codebase gets a `TestComplexity_*` ratio-guard test** in the
   style of §3.4, run in normal CI, plus a `benchstat`-tracked nightly benchmark for absolute
   regression trend-watching — the two serve different purposes and neither substitutes for the
   other (§3.3–3.4).
7. **All benchmarks in this codebase use `b.Loop` (Go 1.24+), never bare `b.N` loops**, and every
   benchmark that measures an operation whose result is otherwise unused must either use `b.Loop`
   (which the compiler now protects) or explicitly sink the result to a package-level variable as
   defense in depth (§3.2).
8. **No backend's client code may issue an unpipelined/unbatched round trip in a loop.** The Redis
   backend's claim/ack path is a single Lua `EVAL` per batch (§4.2); the Postgres backend's claim
   path is a single `SKIP LOCKED` CTE + bulk `UPDATE...RETURNING` per batch (§4.3). This is a
   correctness requirement for meeting the stated 1M-node performance goal, not an optional
   optimization.
9. **CI gates on ratios, nightly jobs track absolutes.** Adopt the table in §4.4 verbatim as the
   shape of the CI assertion set; route any assertion that depends on absolute ops/sec or ns/op to
   a `benchstat`-compared nightly job instead of a per-PR CI gate.
10. **Reach for a compressed bitmap (Roaring) only if profiling shows the ready/blocked sets are
    sparse relative to N in real workloads** (§1.6) — for the common case of a dense, mostly-full
    bitset, the plain `[]uint64` is simpler, has no unsynchronized-access footgun, and is already
    O(N/64) for cardinality, which is good enough unless measurement says otherwise.

## Open questions

- **What is dag-worker-go's actual target hardware for the 1M-node benchmark?** The shard-count
  knee (256, §2.1–2.2) and the Redis/Postgres RTT numbers (§4.2–4.3) were measured on specific,
  named hardware (an 8-to-20-core Intel desktop; unspecified pgx/goldlapel and Redis-team
  hardware) — dag-worker-go's own CI runners and any target deployment host need their own
  baseline run before the constants in this document are trusted as more than starting points.
- **Are parent (reverse) edges required at all, or can they be computed on demand?** §1.7's byte
  budget assumed symmetric storage of child and parent edge lists; if the DAG only ever needs
  forward traversal (scheduling children when a node completes) and reverse edges are only needed
  rarely (e.g., "why is this node still blocked" diagnostics), storing only the CSR forward edges
  and computing in-degree once at ingestion (rather than maintaining a live reverse index) roughly
  halves the steady-state memory budget — this is a product decision, not a performance one, and
  needs an answer before Part 1's design is finalized.
- **How sparse is "ready" in practice?** §1.6's Roaring-bitmap recommendation is conditional on
  real DAG shapes having, say, <5% of nodes ready simultaneously; wide, shallow, highly-parallel
  DAGs (many independent roots) could easily have ready-set occupancy well above the ~12% array-
  container threshold where Roaring degrades to plain-bitmap performance, in which case the extra
  complexity of a compressed bitmap buys nothing and a plain `[]uint64` should simply be the only
  implementation.
- **Does the multi-instance work-distribution strategy (a separate open design question in the
  project brief) change the batching math in §4.2–4.3?** A pull-based competition model (many
  instances racing `SKIP LOCKED`/`EVAL` claims against one shared store) has different batch-size
  economics than a partition-per-scope model (each instance owns a disjoint shard and never
  contends) — the latter could let batch sizes grow much larger before hitting per-batch latency
  concerns, since there's no risk of over-claiming work another instance also wanted. This
  document assumes the former (contention-based) model when sizing the SQL/Lua batch primitives
  in §4.2–4.3 and should be revisited once that design question is settled.
- **What is Go's actual GC scan cost for a `[]uint32`/`[]int32` payload at 1M+ elements measured
  on Go 1.24's allocator, not just asserted from the guide's prose?** §3.5 names the exact
  `runtime/metrics` keys to measure this directly; that measurement should be run and its numbers
  captured in this repository's own benchmark output before Part 1's arithmetic is treated as
  validated rather than derived.
