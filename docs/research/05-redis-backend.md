# Redis as a Backend: DAG + Ready-Set + Lease Store at 1M+ Nodes

This dossier evaluates Redis (7.x/8.x semantics; OSS Redis, not Redis Enterprise) as a storage
backend for `dag-worker-go`'s three concurrently-live structures per scope:

1. **The DAG itself** — ~1,000,000 node records, each with a public status and internal
   metadata, connected by an average out-degree/in-degree of 3.
2. **The ready set** — the subset of nodes whose predecessors have all succeeded, waiting to be
   claimed by an external worker.
3. **The lease/timeout store** — nodes currently claimed by a worker, each with a deadline; a
   sweep must convert an unanswered claim into `error-with-timeout`.

Everything here assumes multiple independent `dag-worker-go` processes hit the **same** Redis
deployment concurrently (single node, replicated primary, or Cluster), so every data-structure
choice is evaluated for what happens when N unrelated clients race on it, not just for single-
writer throughput.

All key names below carry a `{scope}` hash tag prefix (e.g. `{acct42}:ready`) so that every key
belonging to one DAG scope lands in the same Redis Cluster hash slot — this is the load-bearing
design decision for Section 6 and is assumed throughout.

## 1. Data model at a glance

| Key | Type | Encoding at 1M nodes / 3 edges | Purpose |
|---|---|---|---|
| `{s}:n:<id>` | HASH | `hashtable` (own key) or bucketed `listpack` | node record: status, owner, token, deadline, timestamps |
| `{s}:succ:<id>` | SET | `intset` (numeric ids) | outgoing edges from `<id>` |
| `{s}:predp:<id>` | SET | `intset`, transient (deleted once empty) | not-yet-satisfied predecessors of `<id>` |
| `{s}:ready` | ZSET | `skiplist` (>128 members) | FIFO/priority queue of claimable nodes, score = sequence or priority |
| `{s}:leases` | ZSET | `skiplist` | in-flight claims, score = absolute deadline (ms) |
| `{s}:seq`, `{s}:fence` | STRING (int) | `int` | monotonic ready-sequence and fencing-token counters |
| `{s}:events` | STREAM | radix tree of listpacks | transition/must-be-taken event bus |

Sections 2–5 justify each row's type choice against the alternatives named in the brief; Section 7
does the byte-level arithmetic for the whole table at 1,000,000 nodes.

## 2. Node record: HASH vs. one serialized blob

Two designs compete for `{s}:n:<id>`:

**A — HASH per node**, fields `status`, `owner`, `token`, `deadline`, `pending`/predecessor
bookkeeping, `created`, `finished`, `errmsg`, plus whatever small metadata the host wants inline.

**B — one opaque string per node**, msgpack- or protobuf-encoded, holding the same fields as a
single blob (`SET {s}:n:<id> <bytes>`).

| Concern | HASH | Serialized blob |
|---|---|---|
| Atomic single-field mutation (`status`, `owner`, `token`) | `HSET`/`HGET`/`HINCRBY` — native, O(1), no deserialization | requires GET → decode → mutate → encode → SET, wrapped in a script/CAS |
| Cost of a status-only read | `HGET n:<id> status`, no touch of other fields | must transfer and decode the *whole* record even to read one byte |
| CPU inside a blocking Lua script | zero decode cost | needs a msgpack/protobuf decoder **written in Lua 5.1**, since Redis only bundles `cjson`/`cmsgpack` is present in some builds but not guaranteed across all target deployments — extra CPU burned inside a single-threaded, server-wide-blocking script, directly working against the "bound every script" rule (§8) |
| Per-field storage overhead | each field pays a small listpack entry header (backlen + encoding byte, ~2–11 bytes) *inside* the same hash allocation — cheap | one `robj`+SDS overhead total, no per-field tax |
| Memory once the hash exceeds `hash-max-listpack-entries`/`-value` | converts to a real `hashtable`, ~2–10× larger per docs ([Redis memory optimization](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)) | unaffected — a string never has an "encoding cliff" |
| Partial updates under Cluster/AOF/replication | `HSET` replicates as itself, minimal effects-replication cost | full blob is retransmitted/rewritten on every mutation |

**Position:** use the HASH. The library's entire concurrency story is built on cheap, atomic,
single-field Lua/Function mutations (claim, complete, sweep) touching `status`/`owner`/`token`/
`deadline` many times over a node's life while the descriptive payload (whatever opaque bytes the
host attaches) changes at most once. Splitting the record — **hot fields as individual HASH
fields, cold/large payload as one msgpack-encoded field in the same HASH** — gets the best of
both: `HSET n:<id> status 2 owner w7 token 481` never touches the payload bytes, and the payload
is still one field, not five. This is exactly the hybrid Redis's own memory-optimization guide
recommends for "objects with a few small fields plus one larger blob."

The `hash-max-listpack-value` default is **64 bytes** ([memory optimization doc](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)) — if the host's payload routinely exceeds that, the entire per-node hash silently
converts to `hashtable` encoding the moment the payload field is written, even though every other
field is 1–3 bytes. Keep the payload in a *separate* hash/key from the hot fields if payloads can
be large, so the frequently-mutated status/owner/token/deadline hash stays listpack-encoded
independent of payload size.

## 3. Ready-set: LIST vs. ZSET (priority) vs. SET vs. Stream

| | LIST | ZSET | SET | Stream |
|---|---|---|---|---|
| Atomic claim-1 | `LPOP`/`RPOPLPUSH` — O(1) | `ZPOPMIN` — O(log N) | `SPOP` — O(1) | `XREADGROUP ... COUNT 1 '>'` — O(1) amortized |
| Atomic claim-N in one round trip | `LPOP key N` (Redis ≥6.2) | `ZPOPMIN key N` — **one command, priority-ordered** | `SPOP key N` — no ordering | `XREADGROUP COUNT N` |
| Priority ordering | none (FIFO only) | **yes** — score = priority, or `priority*2^32 + seq` for tie-break | none | none (delivery order = append order only) |
| Built-in lease/PEL | none — must pair with a second structure | none — must pair with a second structure | none | **yes** — `XREADGROUP` auto-populates a per-consumer Pending Entries List |
| Per-item, per-claim settable deadline | needs an external store either way | needs an external store either way | needs an external store either way | PEL tracks *idle time since delivery*, not an absolute per-message deadline — a uniform `MIN-IDLE-TIME` threshold applies to whoever runs `XAUTOCLAIM`, so a genuinely per-node deadline still needs an external store |
| Re-entry after edge added / requeue | trivial `LPUSH`/`RPUSH` | trivial `ZADD` | trivial `SADD` | requires a **new** entry with a new ID — the old entry, if not yet ACKed, is still in the PEL and now orphaned unless explicitly `XACK`ed first |
| Memory per member (skiplist encoding, 1M members) | ~2 pointers/node (quicklist) | dict entry (24B) + skiplist node (~37B, see §7) + member SDS | dict entry only (`hashtable` encoding) | listpack entries batched in radix-tree nodes — cheapest per-entry but least flexible |

**Position:** ZSET. The requirement explicitly wants priority (`priority setting per node
implied by "queue"` semantics) and an atomic claim-N — `ZPOPMIN key N` gives claim-N as a single
built-in atomic primitive without even needing Lua for the pop half; Lua is only needed to *fuse*
the pop with the lease-side-effects (§9, Script B). A LIST would be the simplest and cheapest
choice if the ready set never needs priority, but losing this now to save ~15 bytes/member is a
bad trade against a well-known limitation coming back later as a feature request. A SET is
cheaper still but gives *no* fairness guarantee at all — `SPOP`'s selection is uniform-random, so
a node added under contention could theoretically starve indefinitely; acceptable only if the
host explicitly doesn't care about ordering. Streams are the right primitive for the **event
bus** (§10) precisely because of the PEL, but are the wrong primitive for the *ready set* because
(a) they cannot express priority, and (b) "requeue this specific already-delivered item" — needed
whenever a node is re-armed by a dynamic edge addition/removal, or after an error the host decides
to retry — is native to LIST/SET/ZSET (`ZADD` back in) but awkward on a Stream (append a new
entry, which then has a different ID than whatever the caller might already be tracking).

## 4. Lease deadlines: ZSET keyed by expiry

This one is settled, and matches the brief directly: `{s}:leases` is a ZSET, member = node id,
score = absolute deadline in epoch-milliseconds. Two operations drive it:

- **Claim**: `ZADD leases <deadline_ms> <id>` inside the same script that pops the ready set and
  stamps the node hash (Script B, §9) — one extra O(log N) op, fused into the same round trip.
- **Sweep**: `ZRANGEBYSCORE leases -inf <now_ms> LIMIT 0 <batch>` to find candidates, then `ZREM`
  each as it's processed (Script C, §9). `ZRANGEBYSCORE` with an explicit `LIMIT` is essential —
  without it, a sweep run after an outage that left 200,000 leases simultaneously expired would
  return the entire array in one command and then iterate all 200,000 inside a single blocking
  Lua invocation, violating the "bound every script" rule (§8) outright.

Why not `EXPIRE`/TTL per node instead of a hand-rolled ZSET? Because **Redis's own key expiration
is explicitly not a reliable timer** — see §11. A ZSET swept by an explicit, budgeted script is
the only way to get a deadline mechanism whose worst-case latency and worst-case single-call cost
are both under the library's control.

`ZREM` and `ZADD` are both O(log N) ([complexity documented on every ZSET command page](https://redis.io/docs/latest/commands/zadd/)),
so claiming/leasing/sweeping 1,000,000 in-flight nodes costs ~20 comparisons each, not a full scan
— this is the O(log n) bound the brief requires, and it holds regardless of how many *other*
scopes' leases live in other ZSETs (they're separate keys, so N here is "leases in this scope,"
not "leases in the whole deployment").

## 5. Per-node satisfied-predecessor tracking: SET vs. DECR counter

Two designs for "has every predecessor of node S finished?":

**A — SET of not-yet-satisfied predecessor ids**, `{s}:predp:<id>`. Node S is ready when
`SCARD({s}:predp:S) == 0`. Completing predecessor P does `SREM {s}:predp:S P` for every successor
S of P (fan-out bounded by P's out-degree, not by the DAG size).

**B — a bare integer counter**, `HINCRBY {s}:n:S pending -1`. Node S is ready when the counter
hits 0.

The counter is unambiguously cheaper: one 8-byte integer vs. an entire second key. At in-degree
≈3 the SET costs roughly 20–40 bytes total in `intset` encoding (§7) — not nothing, but not
decisive either. The reasons to prefer the SET despite that cost are about **what the two designs
can express, not what they cost**:

1. **Addressable removal.** The brief requires edges to be *removable* while the DAG is live. "Is
   `pred_pending[S]` still tracking `P`?" and "stop tracking `P` as a dependency of `S`" are
   `SISMEMBER`/`SREM` — O(1), idempotent (a second `SREM` of an already-gone member is a correct
   no-op returning `0`), and self-verifying via the return value. A bare counter has no notion of
   *which* dependency is being cancelled: `DECR` blindly trusts the caller that a genuinely
   still-pending edge is being removed, and a duplicate cancel-edge call (a real possibility under
   at-least-once delivery from whatever transport carries host mutation requests) silently
   double-decrements and fires the node early — a correctness bug, not a performance one.
2. **Observability.** "Node S has been stuck for six hours — what is it waiting on?" is
   `SMEMBERS {s}:predp:S`, instantly. A counter can tell you *how many* dependencies are
   outstanding, never *which*. For an operator debugging a stuck production DAG, this is the
   difference between a five-second query and an unanswerable question.
3. Note that fencing-token + status guards (§9, Script C) already make the *claim/complete* path
   idempotent on their own — a retried `complete_node` call is rejected by the `status != IN_PROGRESS`
   check before it ever reaches the successor fan-out, regardless of whether fan-out uses a SET or
   a counter. So the SET's idempotency is not what protects *that* path; it is what protects the
   independent, less-guarded **edge-removal** path, which has no equivalent natural guard.

**Position:** SET, specifically because dynamic edge removal is a first-class requirement and a
bare counter cannot support it correctly. If a future profiling pass shows in-degree distributions
with a long tail (some nodes with thousands of predecessors), revisit with a counter-plus-audit-log
hybrid — but do not default to the counter to save ~30 bytes/node when the modal in-degree is 3.

## 6. Adjacency lists as SETs, and why they should hold integers

`{s}:succ:<id>` is a SET of the ids that depend on `<id>`. At an average out-degree of 3, this is
comfortably inside Redis's small-set fast path — **provided the ids encode as integers**. Redis's
`intset` encoding (a sorted array of fixed-width integers, no per-element pointer or hash bucket)
applies automatically to a SET containing solely integers under `set-max-intset-entries` (default
512) ([memory optimization doc](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)),
and `OBJECT ENCODING` will report `intset` for such a set ([`OBJECT ENCODING` reference](https://redis.io/docs/latest/commands/object-encoding/)). A 3-member `intset` of
32-bit ids costs roughly a fixed encoding header plus 3×4 bytes — call it ~20–24 bytes total,
*dramatically* cheaper than the same 3 members in `hashtable` encoding (which pays a full
dict-entry, ~24 bytes, **per member**, on top of the SDS-encoded string form of each id). If node
ids are UUIDs or other non-numeric strings, the set instead falls to `listpack` encoding (Redis
≥7.2; `set-max-listpack-entries`/`-value`, defaults 128/64) or, past those thresholds, full
`hashtable` — still fine at fan-out 3, but 3–5× larger per set than `intset`.

**Actionable consequence:** assign node ids as dense per-scope `uint64` sequence numbers (an
`INCR` on `{s}:idseq`), not client-supplied strings or UUIDs, specifically so every adjacency SET,
every ZSET member, and every Lua-constructed key suffix (`{s}:n:" .. id`) stays in the cheapest
encoding available at every layer. Expose a stable external id → internal sequence mapping at the
API boundary if the host needs to name nodes with its own strings.

## 7. Memory internals: encodings, thresholds, and per-key overhead

### 7.1 The encoding cliff

Every Redis aggregate type has two representations: a compact one used below a configured size,
and a general one used above it. Redis's own memory-optimization guide states the ratio plainly:
small hashes/lists/sets-of-integers/zsets "are encoded in a very memory-efficient way that uses
*up to 10 times less memory*" than the general encoding, and that this applies transparently
purely based on config thresholds ([Redis memory optimization](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)):

| Type | Compact encoding | Threshold config (Redis ≥7.0 defaults) | General encoding |
|---|---|---|---|
| Hash | `listpack` | `hash-max-listpack-entries 512`, `hash-max-listpack-value 64` | `hashtable` |
| Sorted set | `listpack` | `zset-max-listpack-entries 128`, `zset-max-listpack-value 64` | `skiplist` |
| Set (all-int) | `intset` | `set-max-intset-entries 512` | `hashtable` |
| Set (general, ≥7.2) | `listpack` | `set-max-listpack-entries 128`, `set-max-listpack-value 64` | `hashtable` |
| List | `listpack` per node | `list-max-listpack-size` (bytes/entries per quicklist node) | `quicklist` of listpacks |

Crossing a threshold is a one-way, automatic, silent conversion the first time an offending write
lands — "if a specially encoded value overflows the configured max size, Redis will automatically
convert it into normal encoding" ([ibid.](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)). `OBJECT ENCODING <key>` is the only way to observe which side of the
cliff a given key currently sits on ([`OBJECT ENCODING`](https://redis.io/docs/latest/commands/object-encoding/)) — worth exporting as a metric in
production: an unexpected `hashtable` where `listpack` was assumed is usually the first sign a
payload field grew past 64 bytes somewhere.

Our per-scope `{s}:ready` and `{s}:leases` ZSETs will hold up to 1,000,000 members each and are
**always** `skiplist`-encoded in steady state — the 128-entry listpack threshold is irrelevant to
them (it's a floor, not something to raise your way out of at this N). The per-node `{s}:n:<id>`
hashes and `{s}:succ:<id>`/`{s}:predp:<id>` sets, by contrast, live *below* their thresholds by
design (a handful of fields/members each) and this is exactly what makes bucketing them
attractive (§7.3).

### 7.2 Per-key overhead, and the canonical Instagram case study

Every **top-level** Redis key — regardless of type or value size — pays a fixed structural tax
independent of the value: a `dictEntry` in the main keyspace dictionary (three 8-byte pointers:
key, value, next — 24 bytes on 64-bit), a `redisObject` header for the value (4 bits type + 4 bits
encoding + 24-bit LRU/LFU clock + 4-byte refcount + 8-byte pointer, conventionally cited as 16
bytes), and an SDS-encoded key string. Redis's own SDS header for strings up to 255 bytes
(`sdshdr8`) is, verbatim from source:

```c
struct __attribute__ ((__packed__)) sdshdr8 {
    uint8_t len;      /* used */
    uint8_t alloc;    /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};
```
— [`redis/src/sds.h`](https://github.com/redis/redis/blob/unstable/src/sds.h)

3 bytes of header plus a 1-byte null terminator, so a 20-byte key key string costs ~24 bytes, not
20. Summed: 24 (dictEntry) + 16 (robj) + ~24 (SDS key, 20-char key) ≈ **64 bytes of pure
bookkeeping before a single byte of value is counted**, and jemalloc's size-class rounding
typically adds another 8–20 bytes on top since allocations are rounded up to the nearest size
class rather than allocated exactly. This lands squarely in the widely-cited "50–100 bytes per
key" range used throughout the Redis operations community — it is not an official redis.io
published constant, but it falls directly out of the struct layouts above.

**This is not a hypothetical concern; it is the exact problem Redis's core team solved in public
in 2011.** Instagram, storing hundreds of millions of small key→value media-id mappings, measured
that the naive one-key-per-mapping approach needed ~70MB per 1,000,000 keys, extrapolating to
~21GB at their target scale. Pieter Noordhuis (Redis core team) suggested bucketing: split each
numeric id into a "bucket" (all but the last two digits) used as the hash key and a "sub-key"
(the last two digits) used as the hash field, so every ~100 logical entries share one physical
top-level key. The result: **16MB for the same 1,000,000 entries** — roughly a 4.4× reduction —
while keeping O(1) lookup, because `HGET`/`HSET` on a small listpack-encoded hash is still O(1)
amortized. At their full 300M-key target this was the difference between 21GB and just under 5GB
([Instagram Engineering, "Storing hundreds of millions of simple key-value pairs in Redis"](https://instagram-engineering.com/storing-hundreds-of-millions-of-simple-key-value-pairs-in-redis-1091ae80f74c)).
Redis's own memory-optimization doc still documents this exact technique today, with working Ruby
code, under "Using hashes to abstract a very memory-efficient plain key-value store on top of
Redis" ([Redis memory optimization](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)).

### 7.3 Applying the bucketing lesson to `dag-worker-go`

The naive design in §1 pays the ~70-byte top-level-key tax **three times per node**: once for
`{s}:n:<id>`, once for `{s}:succ:<id>`, once for `{s}:predp:<id>` (the last is transient — deleted
once a node fires — but present for every node's entire waiting lifetime). At 1,000,000 nodes
that is ~3,000,000 top-level keys and ~210MB of *pure overhead*, before a single status byte or
edge id is counted. That is not catastrophic on a modern server (§7.4 puts total memory in the
few-hundred-MB to low-GB range either way), but it is the single largest lever available, and it
is exactly the lever Instagram pulled.

The direct translation of the Instagram technique here: **bucket B nodes' hash fields into one
physical Redis HASH**, keyed by `{s}:n:<id / B>`, with fields named `<id % B>:status`,
`<id % B>:owner`, etc. This cuts the node-record key count from 1,000,000 to `1,000,000 / B`. The
constraint to respect is the `hash-max-listpack-entries` threshold from §7.1: with 6 hot fields
per node, `B` nodes contribute `6B` hash entries, so keeping the bucket inside the default
512-entry listpack ceiling caps `B` at ~85; raising `hash-max-listpack-entries` to 4096 (a global,
one-line `redis.conf` change with no other side effect at this field-size profile) supports
`B = 600` comfortably. **Adjacency and predecessor-pending SETs are harder to bucket** the same
way, because Redis has no "hash of sets" type — a bucketed hash field can hold a serialized list
of successor ids as one string (losing native `SADD`/`SREM`/`SCARD`/idempotent-removal semantics
and requiring a read-decode-mutate-encode-write cycle inside Lua for every edge mutation), or the
per-node adjacency SETs can be left un-bucketed and simply accepted as the residual per-key cost.
Given that adjacency mutations (`SADD`/`SREM` on a specific predecessor id, §5) are exactly the
operations that most benefit from native SET semantics, the recommended shape is: **bucket the
node-record hashes (pure win, no semantic loss), leave adjacency/pred-pending as per-node SETs**
(accept ~140MB of the ~210MB overhead figure above, in exchange for keeping edge mutations O(1)
and idempotent). Section 7.4 computes both the naive and the bucketed-hash numbers side by side.

## 8. Atomicity: EVAL/EVALSHA, Redis Functions, and the cost of blocking

Redis guarantees that a Lua script or Function executes atomically: **the entire server blocks for
every other client, on every other connection, for the script's whole runtime** — "the script's
execution blocks all server activities during its entire time, similarly to the semantics of
transactions" ([Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)); Functions carry the identical guarantee and the identical cost
("a function's execution blocks all server activities during its entire time... functions are
meant to finish executing quickly" ([Redis Functions](https://redis.io/docs/latest/develop/programmability/functions-intro/))). This is the central fact that shapes every
script in §9: **every loop inside a script must be bounded by a caller-supplied, small constant**
(claim count, sweep batch size, out-degree) and never by the size of the ready set, the lease set,
or the DAG. `lua-time-limit` (default 5000ms) does not save you here — once exceeded, Redis starts
returning `BUSY` to every *other* client while the offending script **keeps running to completion
regardless** ([Redis Lua scripting BUSY-error behavior, corroborated across Redis operational docs](https://redis.io/docs/latest/develop/programmability/eval-intro/)); the only actual stop mechanism, `SCRIPT KILL`, only works if
the script hasn't written anything yet, since killing a script that already wrote would violate
the atomicity guarantee.

**EVALSHA vs. Redis Functions** (Redis ≥7.0): both give identical atomicity; the difference is
lifecycle. `EVAL`/`EVALSHA` scripts live in a **volatile, unpersisted cache** — "the Redis script
cache is always volatile... may be cleared when the server restarts, during fail-over... or
explicitly by `SCRIPT FLUSH`" ([Scripting with Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)) — so a client must be prepared to reload every
script after any topology change (`NOSCRIPT` error → `SCRIPT LOAD` → retry). **Functions are
first-class database objects**: `FUNCTION LOAD` persists the library to RDB/AOF and replicates it
to replicas exactly like data, so `FCALL` never races a cold cache after failover ([Redis
Functions](https://redis.io/docs/latest/develop/programmability/functions-intro/)). In a Cluster deployment this cuts the other way for *load time*: Functions must be
loaded onto **every master node individually** — cluster-wide function propagation "is not
handled automatically by Redis Cluster" and is the operator's job via
`redis-cli --cluster-only-masters --cluster call host:port FUNCTION LOAD ...` ([ibid.](https://redis.io/docs/latest/develop/programmability/functions-intro/)).

**Position:** ship the four scripts in §9 as **Redis Functions**, not ad-hoc `EVAL`. The
library embeds a fixed, versioned set of operations — this is precisely the "logic belongs to the
database, not the client" case Functions were built for — and durability-through-restart matters
more here than the minor deployment friction of loading a library onto every Cluster master at
provisioning time (a one-time, scriptable step, not a per-connection concern).

## 9. Redis Cluster: hash tags, CROSSSLOT, and what "scope" buys you

Redis Cluster splits the keyspace into a fixed **16,384 hash slots**, each hosted by exactly one
master at a time, via `HASH_SLOT = CRC16(key) mod 16384` ([Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)). Any command
touching keys in more than one slot on the same call — `MSET`, `SUNIONSTORE`, a Lua script's
`KEYS` list, an `MGET` — fails outright with `CROSSSLOT Keys in request don't hash to the same
slot` unless every key involved hashes to the same slot.

**Hash tags are the escape hatch**, and they are exact and specified, not a convention: "if the
key contains a `{...}` pattern, only the substring between `{` and `}` is hashed" — with a
precise, documented rule for repeated/empty braces (first `{`, first `}` after it, non-empty
content) — [Cluster specification, Hash tags](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/). `{user1000}.following` and `{user1000}.followers`
provably land on the same slot; `foo{}{bar}` hashes as a whole (the first `{}` pair is empty, so
the rule doesn't fire) ([ibid.](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)). This is exactly why every key in §1's schema is written `{s}:...` —
every key belonging to scope `s` — the node hashes, both adjacency sets, both queues, both
counters, the event stream — is provably co-located on one Cluster node, so every multi-key Lua
script in §10 is legal under Cluster without exception, and every scope can, in principle, live on
a different shard, giving horizontal scale-out **by scope** for free.

Two important caveats the spec itself calls out:

- **Resharding suspends multi-key ops mid-slot-migration.** "Multi-key operations may become
  unavailable when a resharding of the hash slot the keys belong to is in progress... operations
  on keys that don't exist or are split between the source and destination nodes... generate a
  `-TRYAGAIN` error" ([Multi-keys operations](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)). A scope mid-migration will intermittently reject claim/complete/sweep
  calls with `TRYAGAIN`; the client must retry with backoff, not treat it as a hard failure.
- **One hash tag = one slot = one node's worth of CPU and memory for that scope, always.** A
  single pathologically large scope (an application that puts its *entire* workload under one
  `{scope}` tag instead of sharding by tenant/customer) cannot be split across Cluster nodes no
  matter how the cluster is resized — this is the direct trade for the atomicity guarantee. The
  library's public API should treat "scope" as the **unit of horizontal scale** and document that
  a single scope's node count and hot-key throughput are bounded by one Redis node's capacity.

### 9.1 Work distribution across multiple library instances

The brief asks for a recommendation on how independent `dag-worker-go` processes divide claim
traffic. Given the scope-per-slot design above, three of the four options the brief lists reduce
to the same underlying mechanism:

- **Pure pull-based competition** (every instance calls claim-N against the same `{s}:ready`
  ZSET) is what `ZPOPMIN`'s atomicity already gives for free — two instances calling claim
  concurrently cannot receive the same node, full stop, because the whole claim script (§10,
  Script B) is a single atomic unit. No extra coordination layer is needed for correctness.
- **Partition-per-scope** is not a separate mechanism so much as the natural consequence of the
  hash-tag design: if the host runs one scope per tenant, tenants already partition load across
  Cluster shards automatically, and pull-based competition happens *within* a scope's single shard.
- **Consistent hashing across instances** and **lease stealing** are solutions to a problem this
  design doesn't have: they exist to route work to specific *workers* deterministically (useful
  when workers hold local, non-transferable state, e.g. sticky sessions or a warm cache). Nothing
  in the brief implies workers are non-fungible; they pull generic units of work and ack success/
  failure. Adding consistent hashing here would add a coordination layer whose only job is to
  route requests to the pull-based queue that already load-balances correctly on its own.

**Recommendation:** pure pull-based competition against the per-scope ready ZSET, with
partition-per-scope as the (already-present, free) horizontal-scaling axis. Do not build
consistent hashing or lease stealing into v1; revisit only if a future profiling pass shows a
concrete workload where worker locality (not just claim fairness) matters.

## 10. Event bus: Streams vs. pub/sub vs. keyspace notifications

The brief's second reactive requirement — "anyone can subscribe to a queue/stream and receive
every status transition and every must-be-taken notice" — needs an actual event *log* with
replay and consumer-group semantics, not a fire-and-forget signal. Redis offers three candidates:

| | Plain Pub/Sub | Keyspace notifications | Streams |
|---|---|---|---|
| Delivery guarantee | none — "Redis Pub/Sub is *fire and forget*; if your client disconnects, and reconnects later, all events delivered during the disconnection are lost" ([Redis keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)) | same underlying transport as Pub/Sub — same loss-on-disconnect | durable log; `XREADGROUP` + PEL gives at-least-once even across consumer restarts |
| Replay / catch-up after downtime | impossible | impossible | `XREAD`/`XREADGROUP` from any historical ID |
| Multiple independent consumer groups | needs one channel per consumer, or fan-out logic client-side | same limitation | native — `XGROUP CREATE` per interested party, each with its own cursor |
| Competing consumers (load-balanced fan-out) | not supported (every subscriber gets every message) | not supported | native — a consumer group balances entries across its members |
| Bounded memory | N/A (nothing retained) | N/A | `XADD ... MAXLEN ~ N` caps the log approximately, O(1) amortized trim |
| Timing/reliability of "key expired" as a signal | N/A | explicitly best-effort: "there are no guarantees that the Redis server will be able to generate the `expired` event at the time the key TTL reaches zero... there can be a significant delay" ([Redis keyspace notifications, Timing of expired events](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)) | N/A |
| Per-node CPU cost | near zero | "the feature uses some CPU power," disabled by default ([ibid.](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)) | XADD cost, amortized MAXLEN trim cost |
| Cluster behavior | broadcast within a node's view only; clients must connect to every node region for full coverage in some setups | "keyspace events are node-specific... to receive all keyspace events of a cluster, clients need to subscribe to each of the nodes" ([Events in a cluster](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)) | one Stream key = one Cluster slot = already co-located with the scope under the `{scope}` hash tag; a client that already talks to the right shard for a scope sees that scope's whole event log |

**Position:** Streams, unambiguously, for anything the library calls a "subscription." Use
`XADD {s}:events MAXLEN ~ <retention> * id <id> status <n> ...` from inside every mutating script
(add-node, claim, complete, sweep — all four scripts in §11 already do this). The `~` modifier is
essential at 1,000,000-node scale: exact `MAXLEN` trimming is O(N) per trim and must remove
entries one at a time, while `MAXLEN ~` allows Redis to trim in whole radix-tree-node batches,
trading a small amount of retention slack for a trim cost close to O(1) amortized. Consumers use
`XREADGROUP` with one consumer group per independent subscriber (a metrics exporter, a UI
dashboard, an audit log) so a slow subscriber cannot starve a fast one, and `XAUTOCLAIM` lets a
newly-started replica of a crashed consumer instance pick up whatever entries were left pending —
`XAUTOCLAIM` "reassigns idle pending entries to a healthy consumer... so no event sits invisibly
past its processing window" and, since Redis 6.2, gives cursor-based iteration so a whole PEL scan
never has to happen in one call ([Redis streaming](https://redis.io/docs/latest/develop/use-cases/streaming/)).

Do **not** use keyspace notifications for anything the DAG's correctness depends on. Beyond the
disconnect-loses-everything property shared with Pub/Sub, `expired` events specifically are
explicitly documented as untimed and unreliable by Redis itself (§11) — which independently rules
out relying on `notify-keyspace-events Ex` as the timeout-detection mechanism, reinforcing the
ZSET-sweep design in §4.

## 11. RESP3 client-side caching: mostly orthogonal, one narrow use

`CLIENT TRACKING` (RESP3, Redis ≥6) lets a client cache read results locally and receive
server-pushed invalidation the moment a tracked key changes, either per-key (default mode, costing
server memory proportional to tracked keys × clients, capped by `tracking-table-max-keys`,
1,000,000 by default) or per-prefix (`BCAST` mode, zero server memory, coarser invalidation)
([Client-side caching reference](https://redis.io/docs/latest/develop/reference/client-side-caching/)). This is designed for read-heavy, write-light, latency-sensitive lookups —
"we don't want to cache many keys that change continuously... we want to cache keys that are
requested often and change at a reasonable rate" ([ibid.](https://redis.io/docs/latest/develop/reference/client-side-caching/)).

Almost nothing in this design fits that profile: `{s}:ready`, `{s}:leases`, and every node's
`status` field change constantly by construction — caching them client-side would mean receiving
an invalidation on nearly every local write, netting negative value. The one place client-side
caching is plausible is a **read replica used for dashboards/observability** that repeatedly polls
a small set of infrequently-mutated fields (a scope's config, a node's static payload once it's
terminal) — worth a `BCAST PREFIX {scope}:payload:` mode subscription for a UI layer, but this is
a nice-to-have for a consumer built on top of the library, not something the core library itself
should assume or build around. **Position: no client-side caching in the core library; document
it as an available pattern for downstream dashboard consumers.**

## 12. Active vs. lazy expiration: why TTL cannot be the timer

Redis expires keys two ways: **lazily**, when a command happens to touch an already-expired key
("checks the key's TTL... if expired, deletes it before running"), and **actively**, via a
background cycle that "picks 20 random keys from the set of keys with TTLs... if more than 25% of
them are expired, it repeats the process," adapting its rate via the `hz` config (default 10Hz).
Both are approximate by design, and Redis's own keyspace-notification docs are explicit that the
resulting `expired` event carries **no timing guarantee whatsoever**: "there are no guarantees
that the Redis server will be able to generate the `expired` event at the time the key TTL reaches
zero... if no command targets the key constantly, and there are many keys with a TTL, there can be
a significant delay" ([Redis keyspace notifications, Timing of expired events](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)). Crucially, the event fires "when the
Redis server deletes the key, not when the time to live theoretically reaches zero" ([ibid.](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)) —
the delay is unbounded in the worst case (a key nobody ever reads again, in a keyspace where the
active-expire sampler keeps landing on other keys), and even when it does fire, delivery rides on
Pub/Sub's fire-and-forget transport (§10), so a disconnected subscriber simply never finds out.

This rules out `EXPIRE`/TTL-per-node as the lease-timeout mechanism outright — not as a
performance concern but as a **correctness** one: a design that relies on `expired` events to
detect a stuck worker could leave a claimed node silently stuck well past its stated timeout, with
no bound on how far past. The ZSET-sweep design in §4/§11's Script C is the library's own,
actively-driven, cost-bounded substitute: the library controls exactly when sweeps run (a
background goroutine on a fixed interval) and exactly how much work each sweep call does (the
`LIMIT` argument), rather than trusting Redis's internal, best-effort, sampling-based cycle.

## 13. Persistence, WAIT/WAITAOF, and what durability the library can honestly promise

Redis offers point-in-time **RDB** snapshots (fork + copy-on-write dump, "you should be prepared
to lose the latest minutes of data" between snapshots) and **AOF** command logging with three
`fsync` policies — `always` (fsync every write, "very very slow, very safe"), `everysec` (default,
"you may lose 1 second of data if there is a disaster"), and `no` (no fsync, kernel decides, "the
faster and less safe method") ([Redis persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)). Both are **local-disk** durability — they say nothing about
whether a write ever left the primary.

Replication is the separate axis, and it is **asynchronous by default**: a write is acknowledged
to the client the instant the primary applies it, before any replica has seen it — "if a client
writes to the primary, the write is acknowledged immediately without waiting for replicas to
confirm." `WAIT numreplicas timeout` turns this into semi-synchronous replication *on demand*: it
blocks the calling connection until N replicas have applied everything written on that connection
so far, but as antirez's own introduction is explicit about, "`WAIT` does not allow you to
rollback an operation that was not propagated to enough slaves. It only offers... a way to inform
the client about what happened" ([antirez, WAIT: synchronous replication for Redis](https://antirez.com/news/66)) — it is an
after-the-fact durability check, not a transactional guarantee, and the write has already
happened locally regardless of what `WAIT` reports. `WAITAOF numlocal numreplicas timeout`
(Redis ≥7.2) extends the same idea to **disk** persistence specifically: it waits for the
replicas' AOF `fsync` to complete, so combined with `appendfsync always` on the replicas it is the
strongest synchronous-durability primitive Redis ships — at the direct cost of the `always`
fsync's latency penalty on every write in the critical path.

**What this means for `dag-worker-go`'s honest durability claim:** with default configuration
(async replication, AOF `everysec` or RDB-only), a primary failover can lose the last ~1 second
of writes to *any* structure in this design — a claim that never reaches a promoted replica
reappears as "never claimed," a completed node's fan-out to successors can vanish, taking the
whole downstream subtree back to an earlier state. The library cannot promise "your DAG survives
a Redis failover with zero lost work" without the host explicitly opting into `WAIT`/`WAITAOF` on
the specific calls that must survive it (most plausibly: `complete_node`, since re-doing a
completed unit of work is usually worse than re-doing a claim), and even then, `WAIT`'s own
disclaimer stands: it reports durability achieved, it does not create it retroactively. The
library's documentation should state this plainly rather than imply Redis gives ACID-style
durability by default — it does not, and no Redis deployment mode makes that claim either.

## 14. A note on distributed locks and fencing tokens

The claim/lease design in §11 is a lock in all but name (`SET NX` and `ZADD` into a leases set are
the same primitive at different granularities), and Redis's own documentation on distributed locks
is directly relevant, including its own caveats. Redis's canonical single-instance lock pattern is
`SET resource_name random_value NX PX ttl`, released by comparing the stored value before deleting
(a compare-and-delete, historically a small Lua script, now natively `DELEX key IFEQ value` as of
Redis 8.4) — precisely so a lock isn't deleted by a client that no longer holds it ([Distributed
Locks with Redis](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)). Redis's own docs now recommend going further and pair this with
fencing tokens: "you should implement fencing tokens. This is especially important for processes
that can take significant time... don't assume that a lock is retained as long as the process that
had acquired it is alive" ([ibid., Disclaimer about consistency](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/)) — exactly the scenario a slow/GC-paused
worker holding a claim represents.

Martin Kleppmann's critique of the multi-instance Redlock algorithm is the canonical statement of
*why* a bare random-value lock is not enough for correctness-critical resource access: a fencing
token must be a **monotonically increasing number**, checked and rejected by the *resource itself*
on every write, because only that gives a downstream write path a way to detect and discard a
stale client that resumed after its lease had already expired — "the storage service... rejects
any write with a token number lower than the highest one previously processed" ([Kleppmann, How to
do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)). Antirez's rebuttal disputes that Redlock specifically needs this (his
counter-argument: a linearizable fencing-token source makes the lock protocol redundant in the
first place, and Redlock's own elapsed-time re-check after quorum acquisition already bounds the
unsafe window under a bounded-clock-drift model) but **concedes the monotonic-clock point** and
does not dispute that fencing tokens are the right tool when the resource being protected can
itself enforce them ([antirez, news/101](https://antirez.com/news/101)).

`dag-worker-go` is exactly the case where the resource *can* enforce it, cheaply: the node's
`status`+`token` HASH fields (§9, Script C) are the "storage service" in Kleppmann's framing, the
per-claim `{s}:fence` counter is the monotonic source, and `complete_node`'s `cur_token ~= token`
check is the rejection. This sidesteps the entire Redlock debate — there is no multi-instance
consensus problem to solve, because the fencing check happens against the *same* single
authoritative Redis (or Cluster shard) that issued the token, not against an independently-
reasoned quorum of separate lock servers. The lesson to take from both sides of the Redlock
argument is narrower and uncontested by either: **never let a worker's lease-holding alone
authorize a write; always make the write itself carry and be checked against a token that only
increases.** Script B/C in §15 do exactly this.

## 15. Complete Lua scripts

All four scripts below share the schema in §1 and the construction convention required by
Redis's own scripting guidance — "the script should only access keys whose names are given as
input arguments... never access keys with programmatically-generated names" ([Scripting with
Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/)) — via the pattern real production Redis-backed queues (Bull/BullMQ, Sidekiq)
actually use: `KEYS[1]` is a single real key that anchors Cluster slot routing, and `ARGV[1]` is
the scope's key **prefix**, used to build every other same-slot key by string concatenation inside
the script. This is safe specifically *because* every key sharing the `{scope}` hash tag is
provably co-resident (§9) — it would not be safe without that hash-tag discipline, and every key
built this way in these scripts is checked against that invariant.

Every script below is bounded by a caller-supplied count/limit or by the DAG's local
out-degree/in-degree, never by the size of the ready set, lease set, or DAG (§10).

### 15.1 `add_node_with_edges` — create a node with its edges, atomically

```lua
-- add_node_with_edges.lua
-- KEYS[1] = "{scope}:ready"              -- routing anchor; shares the scope's hash tag
-- ARGV[1] = key prefix, e.g. "{scope}:"  -- every key this script touches is prefix .. "..."
-- ARGV[2] = new node id (pre-allocated by the caller via INCR "{scope}:idseq")
-- ARGV[3] = now_ms (caller-supplied wall clock; keeps the script's writes
--           deterministic under command-effects replication and AOF replay)
-- ARGV[4] = npred
-- ARGV[5 .. 4+npred]              = predecessor ids
-- ARGV[5+npred]                   = nsucc
-- ARGV[6+npred .. 5+npred+nsucc]  = successor ids
--
-- Returns {status, pending}: status 0 = NEW (unmet predecessors remain),
-- 1 = READY (already enqueued). Raises an error, with NO side effects
-- committed, if the id already exists or a named successor is not open
-- for new inbound edges (SUCCESS/ERROR/IN_PROGRESS).

local prefix = ARGV[1]
local id     = ARGV[2]
local now_ms = ARGV[3]
local npred  = tonumber(ARGV[4])
local cursor = 5

local preds = {}
for i = 1, npred do preds[i] = ARGV[cursor]; cursor = cursor + 1 end

local nsucc = tonumber(ARGV[cursor]); cursor = cursor + 1
local succs = {}
for i = 1, nsucc do succs[i] = ARGV[cursor]; cursor = cursor + 1 end

local node_key  = prefix .. "n:" .. id
local predp_key = prefix .. "predp:" .. id
local succ_key  = prefix .. "succ:" .. id
local ready_key = KEYS[1]

if redis.call("EXISTS", node_key) == 1 then
  return redis.error_reply("NODE_EXISTS " .. id)
end

-- A successor accepts a new inbound edge only while still open
-- (status 0 NEW or 1 READY). SUCCESS(3)/ERROR(4)/IN_PROGRESS(2) are closed.
for i = 1, nsucc do
  local sstatus = redis.call("HGET", prefix .. "n:" .. succs[i], "status")
  if sstatus ~= "0" and sstatus ~= "1" then
    return redis.error_reply("SUCCESSOR_CLOSED " .. succs[i])
  end
end

local pending = 0
for i = 1, npred do
  local pkey = prefix .. "n:" .. preds[i]
  local pstatus = redis.call("HGET", pkey, "status")
  if pstatus ~= "3" then                       -- predecessor not yet SUCCESS
    redis.call("SADD", predp_key, preds[i])
    redis.call("SADD", prefix .. "succ:" .. preds[i], id)
    pending = pending + 1
  end
end

local status = (pending > 0) and 0 or 1
redis.call("HSET", node_key,
  "status", status, "owner", "", "token", 0, "deadline", 0, "created", now_ms)

for i = 1, nsucc do
  local skey = prefix .. "n:" .. succs[i]
  redis.call("SADD", succ_key, succs[i])
  local wasReady = redis.call("HGET", skey, "status") == "1"
  redis.call("SADD", prefix .. "predp:" .. succs[i], id)
  -- A successor that was READY (SCARD predp == 0) just gained an unmet
  -- dependency: demote it back to NEW and pull it off the ready queue.
  if wasReady then
    redis.call("HSET", skey, "status", 0)
    redis.call("ZREM", ready_key, succs[i])
  end
end

if status == 1 then
  local seq = redis.call("INCR", prefix .. "seq")
  redis.call("ZADD", ready_key, seq, id)
end

redis.call("XADD", prefix .. "events", "MAXLEN", "~", "100000", "*",
  "id", id, "status", status)

return {status, pending}
```

Cost: O(npred + nsucc) — bounded by this node's own edge count, never by DAG size. At the
brief's average degree of 3 this is ~6 `redis.call`s plus a couple of `SADD`s, comfortably
sub-millisecond even accounting for the single-threaded blocking cost (§10).

### 15.2 `claim_ready_nodes` — atomic claim-N with lease and fencing token

```lua
-- claim_ready_nodes.lua
-- KEYS[1] = "{scope}:ready"
-- ARGV[1] = prefix
-- ARGV[2] = owner id (opaque string identifying the worker/process)
-- ARGV[3] = max nodes to claim this call -- keep this small (tens to low
--           hundreds); it is the loop bound that keeps this script's
--           blocking-server cost predictable (see Section 8)
-- ARGV[4] = now_ms
-- ARGV[5] = lease_ms override (0 = use each node's own stored default)
-- ARGV[6] = fallback_default_ms (library default, used if neither the
--           override nor a per-node default is set)
--
-- Returns a flat array {id1, token1, deadline1, id2, token2, deadline2, ...}.
-- Moves each claimed id from the ready ZSET into the leases ZSET (score =
-- deadline) and stamps owner/token/deadline/status=IN_PROGRESS on the node.

local ready_key  = KEYS[1]
local prefix     = ARGV[1]
local owner      = ARGV[2]
local count      = tonumber(ARGV[3])
local now_ms     = tonumber(ARGV[4])
local lease_ovr  = tonumber(ARGV[5])
local fallback   = tonumber(ARGV[6])
local leases_key = prefix .. "leases"
local fence_key  = prefix .. "fence"
local events_key = prefix .. "events"

-- ZPOPMIN itself is atomic and O(log(N) + count); it is wrapped here only
-- to fuse the pop with the lease/HSET/fencing side effects into one
-- server-blocking round trip instead of a non-atomic pop-then-lease pair.
local popped = redis.call("ZPOPMIN", ready_key, count)
if #popped == 0 then
  return {}
end

local result = {}
for i = 1, #popped, 2 do
  local id = popped[i]
  local node_key = prefix .. "n:" .. id

  local lease_ms = lease_ovr
  if lease_ms == 0 then
    local stored = redis.call("HGET", node_key, "default_timeout_ms")
    lease_ms = tonumber(stored) or fallback
  end

  local token    = redis.call("INCR", fence_key)
  local deadline = now_ms + lease_ms

  redis.call("HSET", node_key,
    "status", 2, "owner", owner, "token", token, "deadline", deadline)
  redis.call("ZADD", leases_key, deadline, id)
  redis.call("XADD", events_key, "MAXLEN", "~", "100000", "*",
    "id", id, "status", 2, "owner", owner, "token", token)

  result[#result + 1] = id
  result[#result + 1] = token
  result[#result + 1] = deadline
end

return result
```

Cost: O(log N) for the `ZPOPMIN`, plus O(count) for the fan-out over claimed nodes — bounded by
the caller's own batch-size argument, exactly the discipline §10 demands.

### 15.3 `complete_node_and_release_successors` — ack with fencing check, then fan out

```lua
-- complete_node_and_release_successors.lua
-- KEYS[1] = "{scope}:leases"
-- ARGV[1] = prefix
-- ARGV[2] = node id
-- ARGV[3] = fencing token the caller believes it holds
-- ARGV[4] = outcome: "0" = success, "1" = error
-- ARGV[5] = now_ms
-- ARGV[6] = error message (only meaningful when outcome == "1")
--
-- Returns 1. Rejects (no writes applied) with STALE_TOKEN or
-- NOT_IN_PROGRESS if this claim was already resolved by someone else --
-- the two guards a legitimate retry of this exact call must pass through
-- harmlessly (idempotent-by-rejection), and a genuinely stale worker
-- (past its lease, already swept by Script 15.4) must fail.

local leases_key = KEYS[1]
local prefix   = ARGV[1]
local id       = ARGV[2]
local token    = ARGV[3]
local outcome  = ARGV[4]
local now_ms   = ARGV[5]
local errmsg   = ARGV[6]
local node_key = prefix .. "n:" .. id

local cur_status = redis.call("HGET", node_key, "status")
local cur_token  = redis.call("HGET", node_key, "token")

if cur_status ~= "2" then
  return redis.error_reply("NOT_IN_PROGRESS " .. id)
end
if cur_token ~= token then
  return redis.error_reply("STALE_TOKEN " .. id)
end

redis.call("ZREM", leases_key, id)

if outcome == "1" then
  redis.call("HSET", node_key, "status", 4, "owner", "", "errmsg", errmsg,
    "finished", now_ms)
  redis.call("XADD", prefix .. "events", "MAXLEN", "~", "100000", "*",
    "id", id, "status", 4, "err", errmsg)
  return 1
end

redis.call("HSET", node_key, "status", 3, "owner", "", "finished", now_ms)
redis.call("XADD", prefix .. "events", "MAXLEN", "~", "100000", "*",
  "id", id, "status", 3)

-- Fan out to successors. Bounded by this node's out-degree, never by DAG
-- size -- SMEMBERS is safe here specifically because adjacency sets are
-- small by construction (Section 6); it would be a Section-8 violation on
-- the ready/leases ZSETs, which can hold up to 1M members.
local succs = redis.call("SMEMBERS", prefix .. "succ:" .. id)
for i = 1, #succs do
  local sid = succs[i]
  local predp_key = prefix .. "predp:" .. sid
  local removed = redis.call("SREM", predp_key, id)
  if removed == 1 and redis.call("SCARD", predp_key) == 0 then
    local skey = prefix .. "n:" .. sid
    if redis.call("HGET", skey, "status") == "0" then
      redis.call("HSET", skey, "status", 1)
      local seq = redis.call("INCR", prefix .. "seq")
      redis.call("ZADD", prefix .. "ready", seq, sid)
      redis.call("XADD", prefix .. "events", "MAXLEN", "~", "100000", "*",
        "id", sid, "status", 1)
    end
  end
end

return 1
```

Cost: O(out-degree) for the fan-out, O(log N) for the single `ZREM`/`ZADD` pair per newly-ready
successor. At out-degree 3, this is a handful of `redis.call`s regardless of whether the DAG has
1,000 or 1,000,000,000 nodes — the scaling property the whole design exists to deliver.

### 15.4 `sweep_expired_leases` — bounded timeout sweep

```lua
-- sweep_expired_leases.lua
-- KEYS[1] = "{scope}:leases"
-- ARGV[1] = prefix
-- ARGV[2] = now_ms
-- ARGV[3] = max_to_sweep -- hard bound on this call's work; the caller
--           loops, calling again while the returned count == max_to_sweep,
--           so an outage that leaves 200,000 leases simultaneously expired
--           is drained over many small, bounded calls, never one huge one.
--
-- Returns the number of leases it just expired to ERROR(4).

local leases_key = KEYS[1]
local prefix     = ARGV[1]
local now_ms     = ARGV[2]
local limit      = tonumber(ARGV[3])

local expired = redis.call("ZRANGEBYSCORE", leases_key, "-inf", now_ms,
  "LIMIT", 0, limit)

for i = 1, #expired do
  local id = expired[i]
  local node_key = prefix .. "n:" .. id
  -- Defensive check, not a race guard: nothing else runs inside this
  -- single atomic script between the ZRANGEBYSCORE read and this write,
  -- so this can only fire on a schema bug, never a concurrent client.
  if redis.call("HGET", node_key, "status") == "2" then
    local owner = redis.call("HGET", node_key, "owner")
    redis.call("HSET", node_key, "status", 4,
      "errmsg", "lease expired", "finished", now_ms)
    redis.call("XADD", prefix .. "events", "MAXLEN", "~", "100000", "*",
      "id", id, "status", 4, "err", "timeout", "owner", owner)
  end
  redis.call("ZREM", leases_key, id)
end

return #expired
```

Cost: O(limit) — the one and only script in this set whose bound is a raw operator-chosen
constant rather than a graph-local quantity, because "how many leases can expire at once" has no
graph-local bound (a whole worker fleet can die simultaneously). This is precisely why `LIMIT`
must never be omitted here.

## 16. Memory arithmetic: 1,000,000 nodes, 3 edges each

All figures use 64-bit Redis, jemalloc, no `maxmemory-policy` eviction pressure, and dense integer
node ids (§6) so every adjacency/pred-pending SET is `intset`-encoded. "Overhead" below always
means struct/pointer bookkeeping, not the caller's actual field values.

### 16.1 Per-structure unit costs

| Structure | Encoding | Fixed overhead / member | Notes |
|---|---|---|---|
| Top-level key (any type) | — | dictEntry 24B + robj 16B + SDS key (≈3B hdr+1B nul+len) + jemalloc rounding ≈ **8–20B** → **~60–90B total** | paid once per **top-level key**, independent of value size (§7.2) |
| `{s}:n:<id>` HASH, 6 hot fields, un-bucketed | `listpack` (well under 512/64 default thresholds) | ~11B listpack header + 6×(2–3B backlen/encoding + field-name bytes + value bytes) | one top-level key per node |
| `{s}:n:<id/B>` HASH, bucketed, B nodes/key | `listpack` (raise `hash-max-listpack-entries` to fit `6B`) | same per-field cost, but the ~70B top-level-key tax is divided by B | see §7.3 |
| `{s}:succ:<id>`, `{s}:predp:<id>` SET, ≤3 members | `intset` | ~8B header + 4B × member (int32 range) | one top-level key per node per set |
| ZSET member (`{s}:ready`, `{s}:leases`, `skiplist` encoding) | `skiplist` + parallel `dict` | dictEntry 24B + zskiplistNode: 8B score + 8B backward-ptr + (avg 1.33 levels × 16B/level ≈ 21B) ≈ **~53B** + member SDS (~4B hdr + id bytes) | verified against [`zskiplistNode` in `redis/src/server.h`](https://github.com/redis/redis/blob/unstable/src/server.h) — score/backward/level-array fields exactly as shown; level count follows Redis's `p=1/4` skip-list distribution, mean `1/(1-p)=1.33` |
| Stream entry | radix tree of listpacks | amortized ~20–40B/entry depending on field count | bounded total via `MAXLEN ~` regardless of node count (§10) |

### 16.2 Tier 1 — simplest correct design (per-node HASH + 2 per-node SETs)

Per node, steady state (fully wired, not currently ready/leased):

| Item | Bytes |
|---|---|
| `{s}:n:<id>` key overhead | ~70 |
| `{s}:n:<id>` field payload (6 short fields, ~4B name + ~4B value each) | ~48 |
| `{s}:succ:<id>` key overhead | ~70 |
| `{s}:succ:<id>` payload (3 × int32 in intset) | ~20 |
| `{s}:predp:<id>` key overhead (present only while pending; ignore once node fires — counted here as worst case, all nodes mid-flight) | ~70 |
| `{s}:predp:<id>` payload | ~20 |
| **Per-node total** | **~298 bytes** |

At 1,000,000 nodes: **~298MB**, of which **~210MB (70%) is pure top-level-key overhead**, not
application data. Add the shared, O(1)-key structures: `{s}:ready`/`{s}:leases` ZSETs (worst case,
every node briefly resident in one or the other, ~53B skiplist overhead + ~12B member id each ≈
65B × 1,000,000 ≈ 65MB, though in steady state only the *currently* ready-or-leased subset is
resident, typically a small fraction of 1M, not the whole graph) and the constant-size counters/
Stream. **Total working figure: order 300–350MB for the DAG structure itself**, before any
Stream retention or the host's own payload bytes.

### 16.3 Tier 2 — bucketed node-record hashes (B = 100, Instagram-style)

Bucketing only the `{s}:n:<id>` hash (leaving adjacency/pred-pending as per-node SETs, per the
§7.3 recommendation):

| Item | Bytes (per 100-node bucket, then ÷100 for per-node) |
|---|---|
| `{s}:n:<id/100>` key overhead, once per 100 nodes | 70 ÷ 100 = **0.7** |
| Per-node field payload inside the bucket (unchanged, ~48B, now with a longer field name carrying the sub-id, e.g. `"37:status"` ≈ +6B) | ~54 |
| `{s}:succ:<id>`, `{s}:predp:<id>` (unchanged, still per-node) | 70+20+70+20 = 180 |
| **Per-node total** | **~235 bytes** |

At 1,000,000 nodes: **~235MB**, a ~21% reduction from Tier 1 — smaller than Instagram's ~4.4×
because, unlike their pure key→value case, two-thirds of Tier 1's key count (the adjacency/
pred-pending SETs) is deliberately left un-bucketed here to preserve native SET semantics (§7.3).
Bucketing *those too* (accepting the loss of native `SADD`/`SREM`/`SCARD` in favor of a serialized
adjacency blob per bucket) would bring the total close to Instagram's ratio — roughly
**~70–90MB** for the whole 1,000,000-node structure — at the cost of every edge mutation becoming
a read-decode-mutate-encode-write instead of an O(1) `SADD`/`SREM`. Given edge mutations are a
named first-class dynamic-DAG operation (not a one-time bulk load like Instagram's case), this
dossier does **not** recommend going that far by default; Tier 2 (bucket the hash, leave adjacency
alone) is the right default, with full bucketing available as an opt-in "read-mostly DAG" mode.

### 16.4 What this means in absolute terms

Even Tier 1's ~300MB structural cost is small relative to typical Redis deployment sizes (a
single `cache.r6g.large`-class instance ships 13GB+ of usable memory) — the arithmetic in this
section is not an argument that Tier 1 is unaffordable, it is the concrete demonstration of *why*
the Instagram-style bucketing lever exists and how much it is worth pulling: real money and
real replica-count savings at 10× or 100× this node count, where the per-key tax, not the
application data, dominates the bill.

## Recommendations for dag-worker-go

1. **Node record = HASH**, hot fields (`status`, `owner`, `token`, `deadline`) split from any
   large host-supplied payload field, so mutation-heavy Lua scripts never touch payload bytes and
   the hot-field hash stays inside `hash-max-listpack-entries`/`-value` regardless of payload size
   (§2, §7.1).
2. **Ready set = ZSET**, score = monotonic sequence (or `priority*2^32 + sequence` for combined
   priority+FIFO), claimed via `ZPOPMIN key N` fused with lease side effects in one Function call
   (§3, §15.2). Do not use a LIST unless a future measurement shows priority is genuinely unused —
   removing priority later is easy; adding it back under load is not.
3. **Lease deadlines = ZSET keyed by absolute epoch-ms, swept via `ZRANGEBYSCORE ... LIMIT`**, on
   a fixed operator-configured interval and batch size — never rely on `EXPIRE`/TTL or keyspace
   `expired` events for timeout detection; both are explicitly documented by Redis as unreliable
   timers (§4, §12, §15.4).
4. **Predecessor tracking = SET, not a bare counter**, specifically because dynamic edge removal
   needs addressable, idempotent, per-predecessor cancellation that a counter cannot express
   correctly (§5). Readiness is `SCARD(predp) == 0` — do not additionally maintain a parallel
   "pending" integer field; one source of truth avoids a whole class of desync bugs.
5. **Adjacency lists = SETs of dense integer node ids** (assign ids via a per-scope `INCR`
   sequence, not client-chosen strings/UUIDs) specifically to land in `intset` encoding, the
   cheapest aggregate encoding Redis has (§6).
6. **Ship the four core operations as Redis Functions (`FUNCTION LOAD`), not ad-hoc `EVAL`**, for
   restart/failover-safe persistence of the library's own logic; own the operational step of
   loading functions onto every Cluster master (§8).
7. **Every `{scope}` gets one hash tag, applied to every key the scope owns, with no exceptions**
   — this is what makes every multi-key Function in §15 legal under Cluster and makes "scope" the
   unit of horizontal scale-out (§9). Document scope sizing limits (one scope = one shard's worth
   of CPU/memory, permanently) as a first-class API contract, not an implementation detail.
8. **Pull-based competition on the ready ZSET is sufficient for work distribution**; do not build
   consistent hashing or lease stealing for v1 — `ZPOPMIN`'s atomicity already gives correct,
   fair, race-free distribution across any number of competing instances (§9.1).
9. **Streams are the event bus** (`XADD ... MAXLEN ~`), one Stream per scope, consumer groups per
   independent subscriber, `XAUTOCLAIM` for subscriber-crash recovery. Plain Pub/Sub and keyspace
   notifications are unsuitable for anything the library's correctness or a subscriber's
   completeness depends on (§10).
10. **Fence every write that follows a claim.** The node's own `token` field is the fencing
    counter; `complete_node`'s `cur_token ~= token` check is the enforcement point. This sidesteps
    the entire Redlock multi-instance debate because there is no multi-node consensus problem here
    — the check happens against the same shard that issued the token (§14).
11. **Publish an explicit, written durability statement**, not an implied one: default
    async-replication + AOF-everysec means up to ~1 second of any operation (claim, complete,
    sweep) can be lost on an unclean primary failover. Offer `WAIT`/`WAITAOF` as an opt-in,
    documented, per-call cost for hosts that need `complete_node` in particular to survive a
    failover, and do not claim stronger guarantees than that (§13).
12. **Default to Tier 2 bucketing (§16.3) for the node-record hash once a deployment crosses
    roughly 1M nodes per scope**; keep adjacency/pred-pending as per-node SETs unless a specific
    workload proves to be read-mostly enough to justify full bucketing's loss of native SET
    mutation semantics (§7.3, §16.3).

## Open questions

- **Error propagation policy.** When a node reaches ERROR (whether via worker-reported failure or
  lease-timeout), should its successors be blocked forever, auto-cancelled, or left for the host
  to decide via an explicit API call? The scripts in §15 leave successors untouched on error
  (matching the literal requirement text), but this needs an explicit, documented policy rather
  than being an accidental consequence of what the sweep script happens to do.
- **Retry semantics after timeout/error.** Is "error-with-timeout" always terminal, or should the
  library offer a `requeue_node` operation that re-arms a node's `predp` set (already-satisfied
  predecessors stay satisfied) and pushes it back onto `{s}:ready`? This is a small addition to
  §15's scripts but changes the public status vocabulary if a distinct "retrying" state is wanted.
- **Cross-shard/Cluster scope migration.** If a scope outgrows one Cluster shard's practical
  capacity, is there a supported live-migration path (e.g., drain to a new scope tag, replay via
  the event Stream), or is scope size a hard, pre-declared capacity-planning decision the host
  must get right up front? The hash-tag design (§9) makes this a real operational question, not a
  hypothetical one.
- **Payload size ceiling.** §2/§7.1 assume host payloads mostly stay under the 64-byte
  `hash-max-listpack-value` default; large payloads (arbitrary blobs, big JSON documents) should
  probably be forced into a *separate* key from the hot-field hash regardless of size, but the
  exact threshold and whether the library enforces it or merely documents it needs a decision.
- **Multi-scope fairness on a shared Redis without Cluster.** In a single-primary, non-Cluster
  deployment, every scope's `{scope}:events` Stream/`XADD` cost and every scope's Lua script cost
  share the same single-threaded command loop (§8) — a noisy-neighbor scope with a huge claim
  batch size can starve every other scope's latency. Cluster sharding by scope fixes this
  structurally; a single-node deployment needs either an admission-control layer or a documented
  "co-tenant scopes share fate" caveat.
- **Bucket size (`B`) as a public tuning knob vs. an internal constant.** §16.3 picks `B=100` by
  analogy to the Instagram case study, but the right value depends on the host's actual field
  count/size and its `hash-max-listpack-entries` tolerance — worth exposing as a documented config
  rather than hard-coding, but that adds a migration concern if it's ever changed on a live scope
  with existing bucketed keys.
