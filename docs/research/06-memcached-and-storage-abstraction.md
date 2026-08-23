# Memcached as a Backend, and the Storage Abstraction Itself

Two linked questions for dag-worker-go: (A) can memcached honestly be a storage backend for a
dynamic DAG with per-node optimistic concurrency, and if not fully, what tier of participation
is honest; (B) what does the `Store` interface set look like so that a new backend is a few
hundred lines, Redis/Postgres get to use their native atomics instead of emulating someone
else's, and the interface never lies about what a backend can guarantee.

---

## Part A — Memcached as a backend

### A.1 The primitive set, exactly

Memcached's classic (non-meta) text protocol gives you eleven operations. Everything else in
every memcached client library is built out of these. The authoritative source is the protocol
spec shipped in the memcached repo itself:

- `set/add/replace <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n` — `add` fails with
  `NOT_STORED` if the key exists, `replace` fails with `NOT_STORED` if it doesn't
  ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)).
- `append/prepend <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n` — "The append and
  prepend commands do not accept flags or exptime. They update existing data portions, and
  ignore new flag and exptime settings" — i.e. these mutate the value blob only, atomically,
  but cannot touch metadata ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)).
- `cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n<data>\r\n` — stores only if
  the server's current CAS token for that key still equals `<cas unique>`. Returns `STORED`,
  `EXISTS` (token stale — someone else wrote first), or `NOT_FOUND` (key evicted or never
  existed) ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)).
- `gets <key>*\r\n` — like `get` but each `VALUE` line carries the current CAS token: `VALUE
  <key> <flags> <bytes> <cas unique>\r\n<data>\r\n`. This is the *only* way to obtain a
  writable CAS token.
- `delete <key> [noreply]\r\n` — unconditional; there is no delete-if-CAS-matches in the
  classic protocol (`EX`/`C` on `md` fixes this in the meta protocol, below).
- `incr/decr <key> <value> [noreply]\r\n` — "the item must already exist for incr/decr to
  work; these commands won't pretend that a non-existent key exists with value 0; instead,
  they will fail" — data is treated as a decimal ASCII 64-bit unsigned integer; `decr`
  underflow clamps to `0` rather than wrapping ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)).
  Each is a single atomic server-side operation — no read-modify-write round trip, no lost
  updates between concurrent incrementers.
- `touch <key> <exptime> [noreply]\r\n` and `gat`/`gats <exptime> <key>*\r\n` — update-TTL and
  get-and-touch. `gats` also returns the CAS token, so it's the efficient way to combine a
  read with a TTL refresh.

Every one of these is scoped to **exactly one key**. There is no verb in the classic protocol
that touches two keys atomically. `get <key>*` retrieves several keys in one round trip but
that is a batched read, not a transaction — nothing stops another client's write from landing
between two of the keys you fetched. That single fact — no multi-key atomicity, ever, at any
protocol version — is the fact that shapes every subsequent conclusion in this section.

Practical ceiling: the default max item size is 1 MiB, raised with `-I` up to 1 GiB, but the
memcached team's own guidance is "setting item max size above 1MB is not recommended" because
it wrecks slab memory efficiency
([GitHub discussion #1066](https://github.com/memcached/memcached/discussions/1066);
[max-item-size issue #473](https://github.com/memcached/memcached/issues/473)). A node record
with a moderate payload (status, edges, small metadata blob) fits easily; a node that embeds
large arbitrary work-item payloads does not, and should store payloads elsewhere and keep only
a pointer in the node record regardless of backend.

### A.2 What is fundamentally absent

| Missing capability | Why it matters for dag-worker-go | Source |
|---|---|---|
| **Multi-key atomic ops** | Adding an edge means touching two node records (successor's dependency count, predecessor's fan-out list) plus a scope index — memcached cannot make that one atomic unit under any circumstance. | [protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt) — every verb takes one key |
| **Sorted/ordered structures** | No ZSET equivalent for "ready queue ordered by priority/enqueue-time" or "nodes with pending timeout ordered by deadline." | absent from the entire protocol.txt and meta spec |
| **Scan-by-index / enumerate keys** | You cannot ask memcached "give me all node keys under scope X." There is no `KEYS`, no cursor-based `SCAN`. A per-scope index must be a value you maintain yourself (e.g. a JSON/bitset blob under one key), and *that* index is itself subject to the multi-key-atomicity problem above the moment more than one writer touches it. | absent from protocol.txt; contrast with Redis `SCAN`, which does at least guarantee "if an element is inside the collection when a full iteration starts, and is still there when the iteration terminates, then at some point SCAN returned it" — memcached offers no analogous cursor at all ([Redis SCAN guarantees](https://redis-doc-test.readthedocs.io/en/latest/commands/scan/)) |
| **Persistence** | None, by design. `extstore` extends the *value* store onto flash but explicitly does not make the cache crash-safe: "Data stored with extstore is not durable — if the Memcached process crashes, data cannot be recovered," and a restart clears the flash store entirely ([memcached flash storage docs](https://docs.memcached.org/features/flashstorage/)). Memcached's own "warm restart" feature preserves data across a *graceful* process restart on the same host only, not across crashes, not across hosts ([Warm Restart docs](https://docs.memcached.org/features/restart/)). |
| **Eviction under memory pressure is silent and adversarial to correctness** | LRU eviction is slab-class-local: an item can be evicted while plenty of RAM sits unused in other slab classes ([new_lru.txt](https://github.com/memcached/memcached/blob/master/doc/new_lru.txt); ["Memcached and unexpected evictions"](https://medium.com/@ivaramme/memcached-and-unexpected-evictions-a3a50a239108)). With extstore specifically, if the disk writer lags, memcached evicts from the LRU tail *in RAM* rather than block, "even when there is available space on disk" ([extstore eviction issue #922](https://github.com/memcached/memcached/issues/922); [#881](https://github.com/memcached/memcached/issues/881)). For dag-worker-go this means a node record — including its edges and status — can vanish out from under an in-progress DAG at any time with no notification and no way to distinguish "evicted" from "never existed" or "deleted." |
| **CAS token uniqueness is only a same-process, same-lifetime guarantee** | The spec's own language is thin: "cas unique> is a unique 64-bit value of an existing entry" ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)) — it does not claim the token space survives a restart, and warm-restart aside, a plain process restart or an evict-then-recreate cycle can reissue a numerically identical token to a semantically different item. This is exactly the ABA problem (§A.5) and it is not solved for you at the protocol layer. |

### A.3 The meta protocol changes the shape of the problem, not the ceiling

Memcached 1.6+ ships a binary-oriented but still text-framed *meta protocol* — `mg` (meta get),
`ms` (meta set), `md` (meta delete), `ma` (meta arithmetic) — built for byte-efficiency and to
expose internal item state that the classic protocol hid
([Meta Text Protocol docs](https://docs.memcached.org/protocols/meta/);
[MetaCommands wiki](https://github.com/memcached/memcached/wiki/MetaCommands/962ecdc4af4eaef72d42f1c1816bb7fb1f2d6044)).
Structurally: "Commands are 2 characters, followed by key, flags, and tokens requested by
flags" — and critically, **`mg` explicitly takes only one key**: "Unlike 'get' metaget can only
take a single key" ([protocol.txt](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)).
So the meta protocol does not lift the single-key ceiling; it makes single-key operations
richer.

**Relevant flags for CAS-based concurrency control:**

| Flag | Command | Meaning |
|---|---|---|
| `c` | `mg` | Return the item's CAS token in the response |
| `C<token>` | `ms`, `md` | Conditional operation: only proceed if the item's current CAS equals `<token>`; server returns `EX` (exists/mismatch) otherwise |
| `E<token>` | `ms` | Override the CAS token the server will assign after a successful set, letting the client impose its own version numbering scheme (e.g. a monotonic logical clock) instead of trusting the server's internal counter |
| `N<seconds>` | `mg` | Autovivify: if the key is missing, atomically create it with the given TTL as a placeholder, and tell *this* client it "won" the right to populate it — the mechanism behind stampede-safe cache-fill |
| `T<seconds>` | `mg`, `ms` | Set/refresh TTL |
| `I` | `ms`, `md` | Invalidate rather than delete: mark the item stale (bump its "win" epoch) but keep the bytes, so the next reader gets a `W`/`Z` signal instead of a hard miss |
| `q` | all | Quiet: suppress the nominal-success response, only speak up on error — halves round-trip chatter in pipelined batches |
| `O<token>` | all | Opaque application token, reflected back verbatim — used to correlate pipelined requests |
| `k` | `mg` | Reflect the key back in the response |
| `b` | all | Key is base64-encoded (needed for binary/non-ASCII keys) |

**Response codes:** `HD` (success, header only, no value), `VA <size> <flags...>` (success with
value attached), `EN`/`NF` (miss/not found), `NS` (not stored — `add`-style condition failed),
`EX` (CAS/condition mismatch), `MN` (meta no-op, used to flush a pipeline).

**The stampede-control flags (`N`/`W`/`Z`/`X`)** are the meta protocol's most sophisticated
piece of state machinery and worth naming precisely because they are the closest thing
memcached has to a lease:

- `mg key N30 v c` on a miss atomically creates a stub item with 30s TTL and returns the
  requester a **`W` (won)** flag: "you own repopulating this."
- Any other concurrent `mg` with the same `N` flag against the same still-stub key gets a
  **`Z` (lost)** flag instead, and should *not* try to populate — it should wait and retry
  the plain read.
- `md key I` marks an item **stale** (`X`) instead of deleting it, so readers still get the
  old value plus an `X` flag meaning "this is stale, exactly one of you should refresh it,"
  with the same W/Z race resolution as above.

This is a real, working single-flight/lease primitive scoped to **one key**. It is not a
general distributed lock, it is not a scheduling primitive across many keys, and it has no
concept of "this lease's holder crashed, someone else should be able to steal it before the
TTL passes" beyond the plain TTL expiry itself — there is no fencing token attached to `W`, so
if the item is deleted and recreated (or evicted and refilled) while the original winner is
mid-flight, nothing keys the winner's write to a specific "epoch" the way etcd's or ZooKeeper's
version-and-fencing model does (see §A.4).

Client library reality check for Go: the venerable `bradfitz/gomemcache` fork family speaks
only the classic ASCII protocol. Meta-protocol support exists but is comparatively young —
`pior/memcache/meta` and `QuangTung97/go-memcache` implement mg/ms/md/ma directly
([pior/memcache](https://pkg.go.dev/github.com/pior/memcache),
[QuangTung97/go-memcache](https://github.com/QuangTung97/go-memcache)) — so a memcached
backend for dag-worker-go that wants meta-protocol CAS tokens is committing to a narrower,
newer dependency, not the ecosystem default.

### A.4 What memcached looks like next to KV stores that actually support CAS-shaped correctness

The brutal comparison, because "memcached has CAS" invites the wrong mental model if you don't
also know what real CAS-capable stores offer:

| Store | Atomic unit | Multi-key? | Fencing/version model |
|---|---|---|---|
| **Memcached (classic + meta)** | One key | No | 64-bit CAS token per item, no cross-restart or cross-eviction guarantee |
| **etcd** | `Txn{Compare, Success, Failure}` | Yes — a single `Txn` can compare N keys' `value`/`version`/`mod_revision`/`create_revision` and conditionally apply a batch of puts/deletes across all of them in one round trip, "etcd does not allow multiple changes to the same key in a TXN [but] the store's revision is incremented only once for the transaction" ([etcd API docs](https://etcd.io/docs/v3.5/learning/api/); [transactional write tutorial](https://etcd.io/docs/v3.5/tutorials/how-to-transactional-write/)) |
| **DynamoDB** | `TransactWriteItems` | Yes — up to 100 items, each with its own `ConditionExpression`, or a `ConditionCheck` action that asserts a condition on an item without writing it; "the actions are completed atomically so that either all of them succeed, or all of them fail" ([AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)) |
| **Cassandra** | One partition, via Paxos-backed LWT (`IF`/`IF NOT EXISTS`) | Only within one partition, and it costs 4 replica round trips minimum — "LWTs are 4-10x slower than regular writes" ([AxonOps on Paxos v2](https://axonops.com/blog/paxos-v2-and-lightweight-transactions/); [Cassandra guarantees docs](https://cassandra.apache.org/doc/latest/cassandra/architecture/guarantees.html)) |

The pattern across every genuinely CAS-capable distributed store is the same: **the atomic
unit is either widened to cover several keys (etcd, DynamoDB) or the version/CAS token is
explicitly tied to a durable, monotonic revision number that survives process restarts** (etcd
`mod_revision`, DynamoDB item versions backed by durable storage). Memcached gives you neither
widening nor durability of the version space — it gives you a single-key, single-process-
lifetime CAS token and nothing else. That is a strictly weaker primitive than every other store
this project is asked to support, and weaker in a way that matters specifically for a DAG,
where "add an edge" is inherently a two-record operation (dependant + dependency, or
dependant + scope index).

### A.5 Exactly what CAN and CANNOT be honored

**CAN be honored, faithfully, no asterisks:**

- **Optimistic concurrency on a single node record.** `gets nodekey` → mutate the decoded
  struct → `cas nodekey ... <token>` (or `mg nodekey c v` → `ms nodekey C<token> ...` on the
  meta protocol) is a textbook single-item CAS loop and memcached does this as well as any
  store on the list.
- **Idempotent status writes that don't need to observe fan-out.** A worker ack that only
  needs to flip *its own* node's status, with no requirement to atomically wake up
  dependents in the same operation, is a plain CAS write.
- **Best-effort TTL-based lease on a single node**, using the meta protocol's `N`/`W`/`Z`
  stampede tokens as a "who gets to process this node" hint — with the caveat in §A.3 that
  there is no fencing token, so a slow/stalled worker that wins a lease and then pauses (GC,
  scheduler preemption) can still write after a second worker has taken over, unless the
  write itself is additionally CAS-guarded against the node's own version (which brings you
  back to the single-key CAS loop, not the lease flags).

**CANNOT be honored, at any protocol version, full stop:**

- **A scannable per-scope index.** You can maintain a value under `scope:X:index` that lists
  member node keys, but (a) it is itself subject to the same-eviction-anytime and
  no-multi-key-atomicity problems as everything else, and (b) once it grows past ~1 MiB it
  cannot be stored at all without client-side sharding of the index itself, which is a
  research project of its own.
- **Atomic multi-node mutations** — adding an edge, decrementing a dependency counter on N
  successors when a node completes, or any operation that must leave two or more keys in a
  jointly-consistent state. There is no `Txn`, no Lua-equivalent server-side script, no
  MULTI/EXEC. (Memcached deliberately has no server-side scripting akin to Redis Lua or
  Redis Functions — that door does not exist.)
  - *A less obvious form of the same problem*: even the "atomic increment" of `incr`/`decr`
    cannot be composed with a check on a *different* key. You can atomically decrement a
    successor's pending-dependency counter, but you cannot atomically decrement it **and**
    verify it's the specific worker's turn to enqueue the successor when it hits zero — that
    requires either a second CAS round trip (introducing a race window between the `incr`
    reply and the follow-up `cas`) or accepting at-least-once semantics on the enqueue.
- **A reliable timeout sweep.** Sweeping "which nodes have been in-progress past their
  worker-timeout" requires either a sorted index by deadline (absent) or a full key
  enumeration (absent — no `SCAN`, no `KEYS`). The best available substitute is relying on
  the item's own memcached TTL as the timeout and reacting only reactively, on the next
  `gets`/`mg` of that exact key by someone else — there is no proactive sweep, no push
  notification, and nothing analogous to Redis keyspace-notification events on expiry.
- **A durable audit trail / replay of status transitions.** Nothing memcached stores survives
  a crash, an LRU eviction, or an extstore-under-pressure eviction
  ([extstore durability caveat](https://docs.memcached.org/features/flashstorage/)). A
  library whose headline promise is "every node status transition is observable" cannot make
  that promise durable on this backend, only best-effort-while-the-item-is-resident.

### A.6 The recommendation: read-through/write-behind cache tier, never the system of record

Three options were on the table; here is why the third wins outright.

1. **"Offer a reduced-capability tier"** — implement `Store` with the mandatory methods and
   simply return `ErrCapabilityUnsupported` (or fail conformance) for `Scanner`,
   `AtomicIndexer`, and `TimeoutSweeper`. This is honest about *what* is missing but dishonest
   about *durability*: a caller who only uses the "supported" surface (single-node CAS writes,
   single-node reads) would still silently lose data on eviction/restart with zero API signal
   that anything went wrong. Reduced capability without a durability disclaimer is a trap.
2. **"Refuse to be the durable store"** — have the backend registration explicitly declare
   `Durable() bool` returning `false`, and let the host program decide what to do with that.
   This is honest, but it leaves an entire tier of the contract (the scope index, the timeout
   sweep, the DAG topology itself) with no home at all if memcached is the only backend
   configured — the library would have to refuse to start, which is a hard "no" the project
   brief didn't ask for.
3. **Read-through/write-behind cache in front of a real backend** — memcached is registered
   as a `CacheLayer` that wraps a durable `Store` (Redis, Postgres, or even the in-memory
   backend shared inside one process): reads try memcached first via `mg key c v`, miss falls
   through to the durable store and repopulates memcached with `ms key C0 ...` (CAS-`0` means
   "only set if absent," memcached's `add`-equivalent, so two racing repopulations don't
   stomp each other with stale data); writes go to the durable store first and then
   *invalidate* (not update) the memcached copy — invalidate-on-write is strictly safer than
   update-on-write against a store with memcached's silent-eviction-and-eventual-consistency
   character, because a delayed or dropped invalidation just means a future read is one round
   trip slower, never that a client observes a value older than the one it just wrote through
   a different path.

**This dossier recommends option 3, unconditionally.** Concretely: `memcached` should not
implement `dagstore.Store` at all. It should implement a separate, smaller
`dagstore.NodeCache` interface (get/set-if-absent/invalidate on the single node-record blob
keyed by `scope+nodeID`) that any `Store` decorator can compose in front of, with the single
job of shaving read latency and read amplification off the hot path (repeatedly re-fetching a
node that hasn't changed while its dependents are still running). Every write-path guarantee —
CAS ordering, the scope index, the timeout sweep, the transition log — stays entirely on the
durable backend. This also sidesteps the ABA hazard in §A.5 entirely: since memcached never
holds the authoritative CAS token, a stale or wrapped token in the cache can only ever cause an
extra cache miss, never a lost or corrupted write.

### A.7 The CAS-retry loop, with backoff, and the ABA-safety argument

Even confined to `NodeCache`, the single-flight fill-on-miss path benefits from a genuine CAS
retry loop (for the "populate memcached with the value I just read from Postgres" step, so two
concurrent cache-fills after a miss converge instead of duplicating writes indefinitely under
load). Here is the loop, targeting the meta protocol so the CAS token travels with `mg`/`ms`
directly:

```go
package memcache

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrCASConflict is returned by the underlying client when an ms C<token>
// request comes back EX (the item's CAS token no longer matches).
var ErrCASConflict = errors.New("memcache: cas conflict")

// ErrGone is returned when the item disappeared between read and write —
// eviction, expiry, or a concurrent unconditional delete.
var ErrGone = errors.New("memcache: item missing")

// casRetryConfig bounds the loop so a hot key under heavy contention degrades
// to "give up and let the durable read serve this request" instead of
// spinning forever.
type casRetryConfig struct {
	MaxAttempts int           // e.g. 5
	BaseDelay   time.Duration // e.g. 2ms
	MaxDelay    time.Duration // e.g. 50ms
}

// mutateWithCAS reads the current value+token via mg (v c), applies fn, and
// writes it back via ms C<token>. On EX it re-reads and retries with
// exponential backoff + full jitter, per the AWS backoff-and-jitter guidance
// (https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).
func (c *Client) mutateWithCAS(
	ctx context.Context,
	key string,
	fn func(current []byte, found bool) (next []byte, skip bool),
	cfg casRetryConfig,
) error {
	delay := cfg.BaseDelay

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		cur, token, found, err := c.metaGet(ctx, key) // mg key c v
		if err != nil {
			return err
		}

		next, skip := fn(cur, found)
		if skip {
			return nil // caller decided no write is needed
		}

		var storeErr error
		if found {
			storeErr = c.metaSet(ctx, key, next, withCASToken(token)) // ms key C<token>
		} else {
			storeErr = c.metaSet(ctx, key, next, withAddOnly()) // ms key ME (add-if-absent)
		}

		switch {
		case storeErr == nil:
			return nil
		case errors.Is(storeErr, ErrCASConflict):
			// Someone else wrote first. Loop and re-read — this is the
			// detection half of "detect, don't prevent" optimistic
			// concurrency: https://redis.antirez.com/fundamental/atomic-updates.html
		case errors.Is(storeErr, ErrGone):
			found = false // fall through to add-if-absent path next loop
		default:
			return storeErr // a real transport/server error — do not retry blindly
		}

		// Exponential backoff with full jitter, capped, and ctx-aware sleep.
		jittered := time.Duration(rand.Int63n(int64(delay) + 1))
		select {
		case <-time.After(jittered):
		case <-ctx.Done():
			return ctx.Err()
		}
		if delay *= 2; delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}
	return errors.New("memcache: exhausted CAS retry attempts")
}
```

**ABA-safety argument.** The classic ABA hazard is: thread reads value `A` with token `t1`,
gets preempted; another thread changes `A`→`B`→`A` again, and if the second `A` happens to
carry the *same* token `t1` (e.g. after the item was deleted and independently recreated with
the server's counter having wrapped or been reset), the first thread's CAS on `t1` would
wrongly succeed even though the intervening `B` write should have invalidated it
([Understanding and Effectively Preventing the ABA Problem, Stroustrup et al.](https://www.stroustrup.com/isorc2010.pdf);
[Lock-Free Concurrent Data Structures, CAS and the ABA-Problem](https://users.fmi.uni-jena.de/~nwk/LockFree.pdf)).
The general fixes in the lock-free literature are (1) tagged/versioned pointers that carry an
ever-incrementing generation counter alongside the value so a wraparound back to `A` is
distinguishable from the original `A` by generation, and (2) safe-memory-reclamation schemes
(hazard pointers, epoch-based reclamation) that prevent a slot from being reused at all while
any thread might still be holding a stale reference to it.

Applied here: because `NodeCache` is *never* the source of truth, the ABA hazard is
structurally defanged rather than solved. The only consequence of a stale-but-numerically-equal
CAS token succeeding on a wrongly-matched write is that the **cache** ends up holding a value
that is wrong for at most until the next invalidation-on-write from the durable path arrives —
and per §A.6, every write to the durable store unconditionally invalidates (not merges into)
the cache entry regardless of what the cache's own CAS state says. So the true fix is not a
tagging scheme inside memcached at all: it's that memcached's CAS token is never treated as an
authoritative version — it is only ever a local optimization to avoid a thundering-herd
refill, and the actual version of record (the row's `xmin`/`updated_at` in Postgres, or the
Redis Lua script's read of the field it's about to write) is what every correctness argument
rests on. This is the same principle etcd, DynamoDB, and Cassandra apply by construction (their
CAS tokens are tied to a durable, monotonic revision — see §A.4); memcached-as-cache adopts it
by *policy* instead, since the store itself cannot provide it.

### A.8 The general technique: lock-free structures on a CAS-only KV API

The literature on hardware CAS (single memory word, compare-and-swap, ABA via tagged pointers
or hazard pointers — Michael & Scott's queue, Treiber's stack) transfers to a **CAS-only KV
API** with one structural change: there are no pointers, only *keys*, and "swap" means
"replace the entire value blob for that key." The recipe used by every production
CAS-only-KV structure (ZooKeeper znode versions, etcd's revision-gated writes, and this
project's own `NodeCache` argument above) is:

1. **Every mutable record carries its own logical version**, stored *inside* the value blob
   (not just relying on the store's opaque CAS token, which — per §A.5/A.7 — may not survive
   restarts or evictions). A `uint64 Version` field bumped on every successful write is
   sufficient; it plays the role of the ABA-safe tagged pointer.
2. **A multi-record structure (queue, index, DAG edge list) built on a single-key-CAS store is
   built as a value that names other values**, and every operation that must span records is
   decomposed into an ordered sequence of single-key CAS writes such that **any prefix of the
   sequence, observed by another actor, is itself a valid state** — i.e. the structure has no
   moment where it's illegal for a crashed writer to have stopped. This is the same
   discipline as lock-free linked-list insertion (link the new node in with one CAS on the
   predecessor's pointer, in a specific order that keeps the list valid at every step) — see
   the classic treatment in the [ABA-problem lock-free lecture notes](https://users.fmi.uni-jena.de/~nwk/LockFree.pdf).
   Concretely for a DAG edge: write the new edge as a *pending* entry under the successor's
   key first (CAS), then flip the predecessor's fan-out list to reference it (CAS) — a reader
   that only sees step one treats the edge as not-yet-effective; a reader that sees both has a
   consistent edge; there is no state in between that is wrong, only "not yet finished."
3. **Where a store offers genuine multi-key atomicity (etcd `Txn`, DynamoDB
   `TransactWriteItems`, a Postgres transaction), skip the decomposition entirely** — the
   whole reason to have a capability-negotiated storage interface (Part B) is that this
   multi-step, ABA-aware, retry-driven dance is *only* needed on backends that lack the native
   primitive, and forcing every backend through it (the "lowest common denominator" trap)
   throws away exactly the atomics the brief calls out as something Redis/Postgres should get
   to use natively.
4. **Never build a lock-free stack/queue directly on memcached-style CAS.** Michael & Scott's
   MS-queue and Treiber's stack are shaped around linking a *node* into the structure via CAS
   on the *predecessor's next-pointer* — but that requires the predecessor to be discoverable
   at a stable address (the "head" pointer). A KV CAS store has no equivalent of atomically
   swapping "head" among an unbounded, dynamically-named set of candidate nodes without an
   external allocator for node identity, and memcached additionally has none of the safe
   memory reclamation machinery (hazard pointers, epochs) that real lock-free structures
   depend on to keep a concurrently-freed node's memory valid long enough for another thread
   to finish dereferencing it — an evicted memcached key is simply *gone*, there is no
   reclamation deferral. This is the mechanical reason memcached cannot host the DAG's ready
   queue, only cache single node reads.

---

## Part B — The storage abstraction itself

### B.1 Design goals, restated as constraints

1. **Narrow core.** A conforming backend must implement one interface with roughly a dozen
   methods; "a new backend is a few hundred lines" (per the brief) is only achievable if the
   mandatory surface is genuinely small and everything backend-specific is optional.
2. **Capability-rich when the backend allows it.** Redis gets `ZADD`/`BZPOPMIN`-class ready
   queues and Lua-scripted atomic transitions; Postgres gets `SELECT ... FOR UPDATE SKIP
   LOCKED` and `LISTEN`/`NOTIFY`; neither should be forced through a lowest-common-denominator
   emulation of the other's primitive.
3. **Honest about capabilities.** A caller must be able to ask, at runtime, "does this backend
   support atomic multi-successor fan-out?" and get a real answer, not silently degrade or
   silently emulate with a warning buried in a doc comment.

### B.2 Prior art survey

**containerd — narrow core + orthogonal facets.** `content.Store` decomposes into `Ingester`
(begin a write, identified by ref), `Provider` (read by digest once committed), `Manager`
(`Info`/`Update`/`Walk` over committed metadata), and `IngestManager` (list/abort in-flight
writes) — "until ingestion is complete, its content is not visible through Provider or
Manager. Once ingestion is complete, it is no longer exposed through IngestManager"
([containerd content package](https://pkg.go.dev/github.com/containerd/containerd/content)).
The `Snapshotter` interface is similarly narrow — nine methods (`Stat`, `Update`, `Usage`,
`Mounts`, `Prepare`, `View`, `Commit`, `Remove`, `Walk`, `Close`) covering an entire
filesystem-layering backend, with `Info{Kind, Name, Parent, Labels, Created, Updated}` as the
one shared metadata shape every driver returns
([snapshotter.go](https://github.com/containerd/containerd/blob/main/core/snapshots/snapshotter.go)).
Lesson: separate "the mutation lifecycle" (begin/commit/abort) from "the read/query surface"
into different small interfaces rather than one large one.

**Terraform — capability split via a second interface, not a flag.** `backend.Backend` is six
methods total: config schema/validation/configure, `StateMgr(workspace) (statemgr.Full,
diags)`, `Workspaces()`, `DeleteWorkspace()`
([backend.go](https://github.com/hashicorp/terraform/blob/main/internal/backend/backend.go)).
Locking is *not* part of `Backend` — it's a separate `statemgr.Locker` interface (`Lock(info
*LockInfo) (string, error)`, `Unlock(id string) error`) that a `StateMgr` result may or may not
also satisfy, checked with a type assertion at the call site, and the documentation is explicit
that "not all backends support locking." `LockInfo` carries `ID`, `Operation`, `Info`, `Who`,
`Version`, `Created`, `Path` — enough to render "who is holding this and since when" in an
error message without the backend needing to know anything about presentation
([statemgr locker.go](https://github.com/hashicorp/terraform/blob/main/internal/states/statemgr/locker.go);
[Terraform state locking docs](https://developer.hashicorp.com/terraform/language/state/locking)).
Lesson: locking is optional and independently type-asserted, never a boolean flag on the core
interface.

**gocloud.dev — driver package hides the portable API, `As` escapes it.** The pattern
throughout blob/pubsub/docstore is: a `driver` sub-package defines the low-level interface
implementors write against, the top-level package wraps it in a friendlier, uniform,
provider-agnostic API, and every wrapped type carries an `As(interface{}) bool` escape hatch
that does a provider-specific type assertion so power users can reach the underlying S3/GCS/
Mongo client when the portable API doesn't cover something. `docstore/driver.Collection`
centers on `RunActions(ctx, actions []Action, opts)`, `Key(Document) interface{}`, and
`RevisionField() string` — optimistic concurrency is modeled as a named revision field the
driver reads/writes/compares, converted to/from an opaque `[]byte` via
`RevisionToBytes`/`BytesToRevision`, so Mongo can use its own `_etag` shape and DynamoDB its
own version-number shape behind the same portable field
([docstore/driver package](https://pkg.go.dev/gocloud.dev/docstore/driver)).

**Thanos objstore — capability negotiation as an explicit "supported options" query, not type
assertion.** `Bucket` embeds `BucketReader` and adds `Upload(ctx, name, r io.Reader,
opts ...ObjectUploadOption) error` plus, critically,
`SupportedObjectUploadOptions() []ObjectUploadOptionType` — callers are told to "validate which
object upload options (IfNotExists, IfMatch, IfNotMatch) are supported by the provider" *before*
using them, and `ValidateUploadOptions` gives a reusable helper for that check
([objstore.go](https://github.com/thanos-io/objstore/blob/main/objstore.go);
[objstore README](https://github.com/thanos-io/objstore/blob/main/README.md)). This is a
second, equally valid style of capability negotiation to the type-assertion style: a runtime
enum query rather than an interface-satisfaction check. It's the right fit when the varying
capability is "does this specific verb accept this specific option flag" (many small booleans)
rather than "does this backend implement this entire extra facet" (locking, watching).

**Vitess topo.Conn — one interface, every topology backend (ZK/etcd/consul/in-memory)
implements all of it, versions are opaque.** `Conn.Update(ctx, filePath string, contents
[]byte, version Version) (Version, error)` takes an opaque `Version interface{ String() string
}` — "passing nil version permits unconditional updates" — and returns a new opaque version on
success; `Delete` takes the same opaque version or nil for unconditional. This is the
single-key-CAS pattern from Part A, generalized: the interface never assumes the version is a
number, an etag, or a revision — only that it's comparable by the backend and round-trippable
by the caller
([topo/conn.go](https://github.com/vitessio/vitess/blob/main/go/vt/topo/conn.go)). Vitess also
separates `TryLock` (fail-fast) from `Lock` (blocking) from `LockName` (lock a name with no
backing file, for coordination that isn't about protecting a specific stored value) — three
shades of the same primitive that a single `Lock(blocking bool)` boolean flag would muddy.

**Temporal — RangeID as a durable fencing token, not merely a CAS token.** Each history shard
tracks an in-memory `RangeID`, "a monotonically increasing generation number used for fencing
… shard ownership fencing via RangeID prevents split-brain, ensuring only one instance can
write to a shard," and every persistence call for that shard is required to present the current
`RangeID`; a stale one is rejected with a shard-ownership-lost error rather than silently
applied ([Temporal history-service architecture](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md);
[shard ownership assertion issue #3135](https://github.com/temporalio/temporal/issues/3135)).
This is the direct answer to "what replaces a memcached CAS token when you actually need
crash-safe exclusivity across a fleet of worker processes racing for the same partition of
work": a fencing token that is (a) monotonic, (b) durable in the backing store itself, and (c)
checked by every write, not just advisory to whichever process currently believes it holds the
lease. dag-worker-go's multi-instance work-distribution story (a separate open question in this
project) should borrow this exact shape rather than re-deriving it.

**The idiomatic Go capability-negotiation pattern, independent of any of the above projects,**
is `database/sql/driver`'s own approach: a `driver.Conn` is required to implement only `Prepare`,
`Close`, `Begin`; if it additionally implements `driver.Pinger`, `database/sql` uses that for
`DB.PingContext`; if it implements `driver.ExecerContext`/`QueryerContext`, `database/sql`
skips preparing a statement and calls those directly; if not, it falls back to
prepare-then-exec. This is "optional interface + type assertion at the call site," done in the
Go standard library itself, and it is the exact shape dag-worker-go's `Store` core + optional
facets should take.

**The conformance-test-suite pattern.** `testing/fstest.TestFS(fsys fs.FS, expected
...string) error` "walks the entire tree of files in fsys, opening and checking that each file
behaves correctly," checks that symlink metadata behaves correctly if `fs.ReadLinkFS` is also
implemented, and returns a combinable multi-error rather than failing on the first mismatch
([testing/fstest docs](https://pkg.go.dev/testing/fstest)) — a single exported function any
`fs.FS` implementor calls from their own `_test.go`. gocloud.dev's `blob/drivertest` runs the
same idea at far larger scale: `RunConformanceTests(t *testing.T, newHarness HarnessMaker,
asTests []AsTest)` drives ~21 test groups (List/ListWeirdKeys/ListDelimiters, Read/Attributes,
Write/CanceledWrite/UploadDownload/IfNotExist, Metadata/MD5, Copy/Delete/Keys, SignedURL,
ConcurrentWriteAndRead, and a driver-specific `As` check) against whatever `driver.Bucket` the
harness constructs, so every provider package (`s3blob`, `gcsblob`, `azureblob`, `fileblob`,
`memblob`) gets the identical battery of behavioral tests for free, and a new provider proves
itself by passing the shared suite rather than writing its own from scratch
([blob/drivertest/drivertest.go](https://github.com/google/go-cloud/blob/master/blob/drivertest/drivertest.go)).
`docstore/drivertest` runs the analogous suite for revision-based optimistic concurrency,
including an `AsTest` interface for verifying the escape-hatch type assertions actually reach a
real provider-native handle
([docstore/drivertest](https://pkg.go.dev/gocloud.dev/docstore/drivertest)).

### B.3 The proposed interface set for dag-worker-go

**Design decision, stated up front:** the core interface is scoped to *records* (nodes, edges,
scope indices, transition events) and does **not** try to be a generic KV store. A generic KV
`Get(key) []byte` interface would push all of the DAG-shaped semantics (what a node record
looks like, what a status transition looks like) into every backend implementation, defeating
"a new backend is a few hundred lines." Instead, the core interface knows about *nodes* and
*edges* as first-class shapes, and backends translate that into their own native
representation (a Postgres row, a Redis hash + ZSET pair, an in-memory `sync.Map`).

```go
package dagstore

import (
	"context"
	"errors"
	"time"
)

// Version is an opaque, backend-defined optimistic-concurrency token —
// modeled directly on Vitess's topo.Version
// (https://github.com/vitessio/vitess/blob/main/go/vt/topo/conn.go).
// Callers must treat it as a black box: obtain it from a Get, pass it back
// unmodified to a conditional write, and never construct or compare one
// themselves. A nil Version on a write means "unconditional" — same
// convention as topo.Conn.Update.
type Version interface {
	String() string
}

// NodeStatus is the deliberately minimal public vocabulary. Everything else
// (which worker holds it, its retry count, its scheduling priority) is
// private payload, not public state — see the companion state-machine
// research doc for the full justification.
type NodeStatus uint8

const (
	StatusNew NodeStatus = iota
	StatusInProgress
	StatusSuccess
	StatusError
)

// Node is the portable shape every backend must be able to store and load.
// Payload is opaque bytes the host program defines; the library never
// inspects it.
type Node struct {
	Scope     string
	ID        string
	Status    NodeStatus
	Payload   []byte
	Deadline  time.Time // zero if no worker currently holds it
	UpdatedAt time.Time
}

// Edge is directed: From must complete before To becomes eligible.
type Edge struct {
	Scope    string
	From, To string
}

// Sentinel errors every backend must map its native errors onto, so callers
// can branch on behavior without a type switch per backend.
var (
	ErrNotFound       = errors.New("dagstore: node not found")
	ErrVersionMismatch = errors.New("dagstore: version mismatch")   // CAS/optimistic-lock failure
	ErrAlreadyExists  = errors.New("dagstore: node already exists")
	ErrCapability     = errors.New("dagstore: capability not supported by this backend")
)

// Store is the mandatory core. Every backend — memory, Redis, Postgres —
// must implement exactly this and nothing more to be usable at all.
// It intentionally does NOT include listing, scanning, or timeout sweeping:
// those are optional facets (below) because not every backend can offer
// them with the semantics the library needs (see Part A for memcached).
type Store interface {
	// GetNode returns ErrNotFound if absent. The returned Version must be
	// passed to PutNode for an optimistic-concurrency write.
	GetNode(ctx context.Context, scope, id string) (Node, Version, error)

	// PutNode writes unconditionally if expected is nil (create-or-overwrite),
	// or conditionally if expected is non-nil — ErrVersionMismatch if the
	// stored version has moved on, ErrNotFound if expected is non-nil but no
	// node exists to compare against. Returns the new Version on success.
	PutNode(ctx context.Context, n Node, expected Version) (Version, error)

	// CreateNode is PutNode's add-only cousin: ErrAlreadyExists if the node
	// is already present. Exists as its own method (not PutNode with a
	// sentinel Version) because "does this key exist at all" is cheaper to
	// express natively on every backend (Redis SETNX/SET NX, Postgres
	// INSERT ... ON CONFLICT DO NOTHING RETURNING, memcached add) than to
	// simulate via a fabricated "expect absent" Version value.
	CreateNode(ctx context.Context, n Node) (Version, error)

	// Transition performs a compare-and-move public-status change and MUST
	// be atomic with respect to any other Transition or PutNode racing on
	// the same node. Backends that have native single-record atomics
	// (Redis WATCH/Lua, Postgres UPDATE ... WHERE status = $expected) use
	// them directly; a backend with nothing better may fall back to the
	// PutNode CAS loop internally, but the atomicity guarantee to the
	// caller is non-negotiable — see conformance test T-ATOMIC-TRANSITION.
	Transition(ctx context.Context, scope, id string, from, to NodeStatus) error

	// AddEdges registers zero or more directed edges. Implementations MUST
	// guarantee that, from any external observer's point of view, either
	// all edges in one call are visible or none are (batch atomicity per
	// call, not necessarily across calls) — see conformance test
	// T-EDGE-BATCH-ATOMICITY. Backends without native multi-key
	// transactions (see Part A) must refuse via ErrCapability rather than
	// offer partial-batch semantics silently.
	AddEdges(ctx context.Context, edges ...Edge) error

	// DeleteNode removes a node and is unconditional; conditional delete is
	// an optional facet (ConditionalDeleter, below) because not every
	// backend can offer it as cheaply as an unconditional delete.
	DeleteNode(ctx context.Context, scope, id string) error

	// Close releases backend resources (pool, connections).
	Close(ctx context.Context) error
}
```

**Optional capability facets** — a backend implements zero or more of these; callers check with
a type assertion, exactly as `database/sql/driver` checks for `driver.Pinger`:

```go
// Lister is implemented by any backend that can enumerate a scope's nodes
// without a full table/keyspace scan — Postgres via an indexed WHERE
// scope = $1, Redis via a scope-specific SET/ZSET of member IDs maintained
// alongside the node hashes. Memcached cannot implement this — see Part A.
type Lister interface {
	// ListNodes returns nodes in a scope with cursor-based pagination.
	// cursor == "" starts from the beginning; a non-empty returned cursor
	// means more pages remain, matching the Redis SCAN convention of an
	// opaque, backend-defined cursor rather than an offset
	// (https://redis-doc-test.readthedocs.io/en/latest/commands/scan/).
	ListNodes(ctx context.Context, scope string, cursor string, limit int) (nodes []Node, nextCursor string, err error)
}

// ReadyQueue is implemented by any backend that can atomically hand out
// exactly-once-in-flight "ready" nodes to competing worker processes across
// multiple library instances. Redis implements this on Streams
// (XADD/XREADGROUP/XACK/XCLAIM — https://redis.io/docs/latest/develop/data-types/streams/),
// Postgres implements it on SELECT ... FOR UPDATE SKIP LOCKED
// (https://www.postgresql.org/docs/current/sql-select.html), the in-memory
// backend implements it on a channel + a pending-set. Memcached cannot
// implement this at all (Part A.5) and must not attempt to fake it with a
// polling loop over individually-fetched keys, which would violate the
// AckTimeout guarantee below under concurrent competing consumers.
type ReadyQueue interface {
	// ClaimReady blocks (subject to ctx) until at least one ready node is
	// available, hands it to exactly one caller across the whole fleet, and
	// starts its ack-timeout clock. The returned ClaimToken must be passed
	// to Ack or Nack.
	ClaimReady(ctx context.Context, scope string, ackTimeout time.Duration) (Node, ClaimToken, error)

	// Ack confirms successful processing; the node's status is expected to
	// already have been moved to Success/Error by a prior Transition call —
	// Ack here only clears it from the in-flight/pending set.
	Ack(ctx context.Context, token ClaimToken) error

	// Nack releases the claim early (worker detected its own failure before
	// the ack timeout), making the node immediately reclaimable rather than
	// waiting out the full ackTimeout.
	Nack(ctx context.Context, token ClaimToken) error
}

// TimeoutSweeper is implemented by any backend that can reliably discover
// claims whose ack timeout has elapsed without a proactive per-node timer.
// Redis Streams offers this via XPENDING/XAUTOCLAIM idle-time
// (https://redis.io/docs/latest/commands/xautoclaim/); Postgres via a
// deadline column plus a periodic SKIP LOCKED sweep query. Memcached cannot
// implement this (Part A.5) — no scan, no ordered index by deadline.
type TimeoutSweeper interface {
	// SweepTimedOut finds claims whose deadline has passed, transitions
	// those nodes to StatusError (timeout), and returns how many were swept.
	// Backends decide their own sweep granularity; the library calls this
	// on a ticker, not per-node.
	SweepTimedOut(ctx context.Context, scope string, now time.Time) (int, error)
}

// EventStream is implemented by any backend that can fan status
// transitions and "must be claimed" notices out to subscribers without the
// subscriber polling. Redis: Pub/Sub or Streams consumer groups. Postgres:
// LISTEN/NOTIFY. In-memory: a fan-out channel. A backend without this
// facet still supports subscription at the library level via a
// library-managed polling adapter built ONLY on the mandatory Store
// surface, at reduced timeliness — but it must be labeled as polling, not
// presented as equivalent.
type EventStream interface {
	Subscribe(ctx context.Context, scope string) (<-chan Event, error)
}

// ConditionalDeleter is implemented by backends that can delete-if-version-
// matches as a single call rather than emulating it with a read-then-CAS-
// then-delete loop (Postgres: DELETE ... WHERE version = $1; Redis: a small
// Lua script; memcached's meta protocol: md key C<token>, which is why
// memcached IS capable of this one facet even though it fails Lister,
// ReadyQueue, and TimeoutSweeper — capability negotiation is per-facet, not
// all-or-nothing per backend).
type ConditionalDeleter interface {
	DeleteNodeIf(ctx context.Context, scope, id string, expected Version) error
}

// ClaimToken is opaque and backend-defined, analogous to Version.
type ClaimToken interface {
	String() string
}

type Event struct {
	Scope     string
	NodeID    string
	Kind      EventKind // TransitionEvent | MustClaimEvent
	From, To  NodeStatus
	At        time.Time
}

type EventKind uint8

const (
	TransitionEvent EventKind = iota
	MustClaimEvent
)
```

**Capability discovery** follows the `database/sql/driver` / Thanos pattern exactly — a small
enum-returning helper for coarse-grained "which facets exist" queries, plus type assertions at
the call site for actually using them:

```go
// Capabilities lets a caller (or the library's own bootstrap code) log or
// refuse-to-start based on what a configured backend can do, without
// needing five separate type assertions scattered through the codebase.
type CapabilityReporter interface {
	Capabilities() CapabilitySet
}

type CapabilitySet uint32

const (
	CapList CapabilitySet = 1 << iota
	CapReadyQueue
	CapTimeoutSweep
	CapEventStream
	CapConditionalDelete
	CapMultiKeyAtomicEdges // AddEdges is truly atomic across nodes, not per-call best-effort
)

func (cs CapabilitySet) Has(c CapabilitySet) bool { return cs&c != 0 }
```

```go
// Call-site usage — exactly the shape of database/sql checking for
// driver.Pinger, or Thanos checking SupportedObjectUploadOptions.
func startSweeper(ctx context.Context, s dagstore.Store, scope string) {
	sweeper, ok := s.(dagstore.TimeoutSweeper)
	if !ok {
		log.Printf("backend %T has no native timeout sweep; falling back to library-level polling sweep (reduced timeliness)", s)
		go pollingFallbackSweep(ctx, s, scope)
		return
	}
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				sweeper.SweepTimedOut(ctx, scope, time.Now())
			case <-ctx.Done():
				return
			}
		}
	}()
}
```

### B.4 What each backend guarantees, to pass conformance

The conformance suite (§B.5) is what actually enforces this table — it is not documentation
that can silently drift, it is a shared `_test.go` every backend package imports and runs
against its own harness, exactly as `blob/drivertest.RunConformanceTests` does for every
gocloud.dev provider
([blob/drivertest.go](https://github.com/google/go-cloud/blob/master/blob/drivertest/drivertest.go)).

| Capability | In-memory | Redis | Postgres | Memcached (as `NodeCache`, not `Store`) |
|---|---|---|---|---|
| `Store` core | Yes — `sync.Map` + per-node `sync.RWMutex`, Version = incrementing `uint64` cast to string | Yes — one `HASH` per node, Version = Redis `OBJECT FREQ`-independent app-level `rev` field, CAS via a 3-line Lua script (`if redis.call('HGET',KEYS[1],'rev')==ARGV[1] then redis.call('HSET',...) end`) — [atomic update patterns](https://redis.antirez.com/fundamental/atomic-updates.html) | Yes — one row per node, Version = `xmin` or an explicit `revision BIGINT`, CAS via `UPDATE ... WHERE id=$1 AND revision=$2` | **N/A — does not implement `Store`** (Part A.6) |
| `Lister` | Yes — scope index is an in-process `map[string]map[string]struct{}` under the same lock | Yes — a `ZSET` per scope keyed by node ID, scored by insertion order, `ZRANGE` for pagination | Yes — indexed `WHERE scope = $1 ORDER BY id LIMIT ... OFFSET`/keyset pagination | No |
| `ReadyQueue` | Yes — buffered channel per scope + an in-process pending-set with a `time.AfterFunc` per claim | Yes — Streams: `XADD` on ready, `XREADGROUP GROUP workers ... COUNT 1 BLOCK ...`, `XACK` on Ack, claim = `(stream ID, consumer group)` pair ([Redis Streams docs](https://redis.io/docs/latest/develop/data-types/streams/)) | Yes — `SELECT id FROM nodes WHERE scope=$1 AND status='ready' ORDER BY priority FOR UPDATE SKIP LOCKED LIMIT 1`, claim = row id + `deadline` column set in the same statement ([PostgreSQL SKIP LOCKED docs](https://www.postgresql.org/docs/current/sql-select.html)) | No |
| `TimeoutSweeper` | Yes — a min-heap of deadlines, swept on a ticker | Yes — `XAUTOCLAIM stream group consumer min-idle-time 0-0 COUNT 100`, reassigns entries idle past the ack timeout ([XAUTOCLAIM docs](https://redis.io/docs/latest/commands/xautoclaim/)) | Yes — `UPDATE nodes SET status='error_timeout' WHERE status='in_progress' AND deadline < now() RETURNING id` | No |
| `EventStream` | Yes — `chan Event` fan-out, buffered, drop-oldest on a slow subscriber (documented, not silent) | Yes — a second Streams consumer group purely for events, or Pub/Sub for at-most-once | Yes — `LISTEN dag_events` / `NOTIFY dag_events, payload` (payload ≤ 8000 bytes — large events carry a node key, not the full record) | No |
| `ConditionalDeleter` | Yes | Yes — Lua script | Yes — `DELETE ... WHERE revision=$2` | **Yes** — `md key C<token>` on the meta protocol IS a real conditional delete on a single cached copy (Part A.3); this is the one facet memcached honestly supports, which is exactly why capability negotiation must be per-facet |
| `CapMultiKeyAtomicEdges` | Yes — global scope lock held for the duration of `AddEdges` | Yes — Lua script touching both node hashes and the scope ZSET in one round trip | Yes — a single `BEGIN; ...; COMMIT` transaction | No — memcached is not even a `Store`, so this bit is moot |

**Exact conformance semantics per capability** (what a backend must prove to earn the bit, not
just what it typically does):

- **`Store.Transition` atomicity**: under N concurrent goroutines calling `Transition(from=A,
  to=B)` and `Transition(from=A, to=C)` against the same fresh node with status `A`, exactly
  one must succeed and the rest must return a distinguishable "not in expected state" error —
  never both succeeding, never the node ending up in a status neither caller requested.
- **`AddEdges` batch atomicity**: a concurrent `GetNode`/`ListNodes` observer, sampled at an
  arbitrary point during a 3-edge `AddEdges` call, must see either zero or all three edges
  reflected in whatever structure records them (fan-out list, dependency counters) — partial
  visibility fails conformance outright, per `CapMultiKeyAtomicEdges` above.
- **`ReadyQueue.ClaimReady` exclusivity**: under N competing consumers across (simulated)
  multiple process instances hitting the same backend, each ready node must be delivered to
  exactly one consumer per claim epoch — re-delivery after `ackTimeout` elapses without an
  `Ack` is correct and expected (at-least-once), but two *simultaneous* un-timed-out claims on
  the same node is a conformance failure.
- **`TimeoutSweeper.SweepTimedOut` no-double-sweep**: calling it concurrently from two
  goroutines against the same overdue claim must transition that node to
  `StatusError`(timeout) exactly once — the second caller observes the already-swept state and
  counts it as zero newly-swept, not as an error, and not as a double-transition.
- **`Version` round-trip fidelity**: `Version.String()` must be usable purely as an opaque
  comparison/storage token — conformance feeds a `Version` obtained from one `GetNode` back
  into a `PutNode` after serializing it to string and back (as a caller persisting it across a
  process restart might), and the backend must still honor it correctly, mirroring Vitess's
  own opaque-`Version` contract ([topo/conn.go](https://github.com/vitessio/vitess/blob/main/go/vt/topo/conn.go)).

### B.5 The conformance suite, sketched

Following `blob/drivertest` and `testing/fstest.TestFS` directly: one exported function per
capability group, a `Harness` interface each backend package implements in its own
`_test.go`, and a single `RunConformance` entry point that backend authors call with one line:

```go
package dagstoretest

import (
	"context"
	"testing"

	"example.com/dagstore"
)

// Harness is implemented once per backend package (memstore, redisstore,
// pgstore) — modeled on gocloud.dev's blob/drivertest.HarnessMaker
// (https://github.com/google/go-cloud/blob/master/blob/drivertest/drivertest.go).
type Harness interface {
	// MakeStore returns a fresh, empty Store backed by a real instance of
	// the backend (e.g. a Docker-Composed Redis on a non-default port for
	// end-to-end runs, miniredis for fast unit runs).
	MakeStore(ctx context.Context) (dagstore.Store, error)
	Close()
}

// RunConformance is the single call every backend's TestConformance
// function makes. It runs the mandatory Store suite unconditionally, then
// runs each optional-facet suite only if the returned Store satisfies that
// facet's interface — exactly the shape of testing/fstest.TestFS walking
// whatever the fs.FS actually implements
// (https://pkg.go.dev/testing/fstest).
func RunConformance(t *testing.T, mk func(context.Context) (Harness, error)) {
	t.Run("Store/CreatePutGet", func(t *testing.T) { testCreatePutGet(t, mk) })
	t.Run("Store/TransitionAtomicity", func(t *testing.T) { testTransitionAtomicity(t, mk) })
	t.Run("Store/AddEdgesBatchAtomicity", func(t *testing.T) { testAddEdgesAtomicity(t, mk) })
	t.Run("Store/VersionRoundTrip", func(t *testing.T) { testVersionRoundTrip(t, mk) })
	t.Run("Store/ConcurrentCASConverges", func(t *testing.T) { testConcurrentCAS(t, mk) })

	t.Run("Capability/Lister", func(t *testing.T) { runIfCapable[dagstore.Lister](t, mk, testListerPagination) })
	t.Run("Capability/ReadyQueueExclusivity", func(t *testing.T) { runIfCapable[dagstore.ReadyQueue](t, mk, testReadyQueueExclusivity) })
	t.Run("Capability/TimeoutSweepIdempotent", func(t *testing.T) { runIfCapable[dagstore.TimeoutSweeper](t, mk, testSweepNoDoubleFire) })
	t.Run("Capability/EventStreamOrdering", func(t *testing.T) { runIfCapable[dagstore.EventStream](t, mk, testEventOrdering) })
	t.Run("Capability/ConditionalDelete", func(t *testing.T) { runIfCapable[dagstore.ConditionalDeleter](t, mk, testCondDelete) })
}

// runIfCapable skips (not fails) the sub-suite when the backend doesn't
// implement the facet — mirroring Thanos's SupportedObjectUploadOptions
// check rather than failing a backend for honestly not offering something
// (https://github.com/thanos-io/objstore/blob/main/README.md).
func runIfCapable[C any](t *testing.T, mk func(context.Context) (Harness, error), fn func(*testing.T, C)) {
	ctx := context.Background()
	h, err := mk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	s, err := h.MakeStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := s.(C)
	if !ok {
		t.Skipf("backend does not implement %T; skipping facet suite", c)
		return
	}
	fn(t, c)
}
```

A memcached `NodeCache` package does **not** call `RunConformance` at all — it is not a
`Store` — and instead ships its own much smaller conformance suite
(`nodecachetest.RunConformance`) covering only get/set-if-absent/invalidate semantics, which is
itself the visible, structural proof that memcached was deliberately scoped out of the `Store`
contract rather than silently failing most of it.

---

## Recommendations for dag-worker-go

1. **Do not implement `dagstore.Store` on memcached.** Ship it as `dagstore.NodeCache` — a
   three-method (`Get`, `SetIfAbsent`, `Invalidate`) read-through decorator that wraps any real
   `Store`, using `mg key c v` / `ms key ME` / `md key` on the meta protocol. This is the one
   design decision in this dossier to hold as non-negotiable; every alternative (reduced tier,
   `Durable() bool` flag) leaves a durability trap reachable through the public API.
2. **Depend on the meta protocol (`mg`/`ms`/`md`), not the classic ASCII protocol**, even
   though it commits to a newer client library (`pior/memcache` or
   `QuangTung97/go-memcache`), because `C<token>`/`ms ME` give exact add-if-absent and
   CAS-conditional-set semantics in one round trip that the classic protocol needs two
   commands (`gets` then `cas`) to approximate with a wider race window.
3. **Never treat a memcached CAS token as an authoritative version.** Every correctness
   argument in the library must rest on the durable backend's version field; memcached's token
   is purely a local optimization against thundering-herd refills, matching how every store
   that actually offers durable CAS (etcd, DynamoDB, Cassandra) ties its version to durable
   revision rather than an ephemeral in-memory counter.
4. **Build `Store` as a narrow mandatory core (create/get/put-with-version/transition/
   add-edges/delete/close) plus five optional capability interfaces** (`Lister`, `ReadyQueue`,
   `TimeoutSweeper`, `EventStream`, `ConditionalDeleter`), discovered by type assertion at call
   sites and summarized by an optional `CapabilityReporter` for logging/preflight — this is
   the containerd/Terraform/database-sql pattern, not a single fat interface.
5. **Give Redis its Lua-scripted atomic transitions and Streams-based ready queue, and
   Postgres its `SKIP LOCKED`/`LISTEN`/transactional `AddEdges`, as first-class implementations
   of the optional interfaces — never emulate one backend's primitive on top of another's.**
   The whole point of optional capability interfaces is that a backend which can do a thing
   natively does it in one round trip; a backend that can't either declines (`ErrCapability`)
   or falls back to a clearly-labeled, reduced-timeliness library-level polling adapter that
   callers can distinguish from the native path.
6. **Write the conformance suite before the second backend, not after all three.** Model it
   directly on `blob/drivertest.RunConformanceTests` and `testing/fstest.TestFS`: one exported
   `RunConformance(t, harnessMaker)` that every backend package's own test file calls, with
   capability sub-suites that `Skip` (not fail) when a facet isn't implemented — this is what
   makes the capability table in §B.4 a live contract instead of documentation that quietly
   goes stale.
7. **Model `Version` and `ClaimToken` as opaque `interface{ String() string }` values**,
   exactly as Vitess's `topo.Version` does, never as a concrete `uint64` or `string` in the
   public API — this is what lets Postgres use `xmin` or a `revision` column and Redis use a
   Lua-script-returned value without either backend leaking its native representation into the
   shared interface.
8. **For the multi-instance work-distribution question (a separate open design area), reuse
   Temporal's `RangeID`-style fencing rather than inventing a new primitive**: a durable,
   monotonic per-scope-partition token that every write must present, so a stalled instance
   that wakes up after another instance has taken over a partition gets rejected rather than
   silently corrupting state — the structural reason memcached's `W`/`Z` lease flags are
   insufficient for this role (Part A.3) is precisely that they lack this fencing property.

## Open questions

- **Is a single node-level CAS loop (§A.7) actually needed inside `NodeCache`, or is
  invalidate-on-write from the durable store sufficient on its own?** If reads always check
  memcached first and only ever repopulate on a genuine miss (never on a value that's merely
  "old"), the retry loop in §A.7 may be over-engineering for a component whose only job is
  shaving read latency — worth prototyping both and measuring cache-fill duplication under
  load before committing to the retry-loop complexity.
- **Should `AddEdges` batch atomicity be a hard conformance requirement, or should there be a
  weaker `CapEdgeBatchBestEffort` tier for a hypothetical future backend that can't offer true
  multi-key atomicity but also isn't memcached (e.g. a very constrained embedded KV)?** This
  dossier assumes only memory/Redis/Postgres/memcached-as-cache are in scope, all of which
  either offer true batch atomicity or are excluded from `Store` entirely, so the question is
  moot today but may not stay moot if a fifth backend (e.g. DynamoDB, which *does* offer
  `TransactWriteItems` up to 100 items and would fit the strong tier fine) or a weaker one is
  added later.
- **Does `ReadyQueue.ClaimReady` need an explicit `ClaimToken`-embedded fencing counter now**,
  per the Temporal `RangeID` recommendation, or can that be deferred until the multi-instance
  work-distribution design (a separate research area) actually settles on partitioning vs.
  pull-based competition vs. lease-stealing? Baking a fencing token into `ClaimToken` today
  costs little and forecloses fewer options later, but the exact shape of the token (per-scope
  epoch vs. per-node epoch vs. per-partition epoch) depends on that unresolved distribution
  design.
- **Is `EventStream` in-scope for every backend's conformance suite, or should it be allowed
  to degrade to at-most-once (Redis Pub/Sub, Postgres `NOTIFY` payload-size limits) without
  failing conformance**, given the library's headline promise is that "anyone can subscribe...
  and receive every node status transition"? An at-most-once event stream silently violates
  "every" under a slow-subscriber or connection-drop scenario; this dossier did not settle
  whether the conformance suite should require at-least-once delivery (forcing, e.g., a
  Streams-consumer-group implementation on Redis rather than Pub/Sub) or accept degraded
  guarantees as long as they're truthfully reported via `CapabilitySet`.
