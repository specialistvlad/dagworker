# Correctness and Concurrency Review — dagworker

Scope: the lease protocol and everything concurrent, across the core module and all three
backends (`storage/memory`, `storage/redis`, `storage/postgres`). Methodology: read every
mutating path in all three backends line by line, cross-checked against `docs/spec/01-contract.md`
and the relevant ADRs, then built and ran the full test matrix, including the multi-instance
end-to-end suite against live PostgreSQL and Redis, with `-race`.

Housekeeping note before the findings: this repository is not idle. During this review,
`storage/postgres/pipeline.go` was observed mid-edit — a build run failed with four `undefined:`
errors (`scanNodeFromRows`, `insertNodeSQL`, `insertEventSQL`, `scanEventEffect`) referencing
symbols that `engine.go` supplies, and `engine.go`/`pipeline.go` share an identical mtime
(`Aug 23 05:49:46`) from a process other than this review. A re-run seconds later built clean and
every subsequent build and test pass in this report is against the settled state. I flag this only
so the findings below are read as "true of the code as it stood when checked," not as a claim that
the tree is static — worth knowing if a finding stops reproducing.

## Test results

- `go build ./...` and `go vet` — clean, once the file above stopped moving.
- Core module, `-race -count=1`: **pass** (`dagworker`, `internal/pq`, `storage/memory`).
- `storage/postgres`, `-race -tags=integration`, live PostgreSQL on `127.0.0.1:15432`: **pass**.
- `storage/redis`, `-race -tags=integration`, live Redis on `127.0.0.1:16379`: **pass**.
- `test/e2e`, `-race -tags=integration` (the multi-instance suite, including
  `TestTwoInstancesNeverDoubleDispatch`, `TestSurvivingInstanceRecoversADeadOnesWork`,
  `TestFanOutCrossesInstances`): **pass**, all three backends.

One process note: `DAGWORKER_INTEGRATION=1 go test ./...` **alone**, without `-tags=integration`,
silently compiles `integrationBackends()` as the empty stub (`test/e2e/backends.go` is
`//go:build !integration`) and every shared-backend test reports `SKIP`, not `FAIL`. The Makefile's
`integration` target gets this right (`-tags=integration` is present), but a reviewer or a new
contributor running the obvious command gets a clean-looking, fully-green run that never touched
Redis or PostgreSQL. That is a discoverability nit, not a defect — recorded here mainly because it
is exactly the kind of gap that makes a real regression in the cross-instance path look
green in CI if the tag were ever dropped from one job.

None of what follows is "the tests are wrong." The suite that exists is good and it passes. The
findings below are things it does not check.

---

## BLOCKER — Deleting and re-adding a node under the same ID resets the fencing epoch, and a stale lease from the old generation can be honoured against the new one

This is the one that matters most, because it breaks the single claim the whole design rests on:
*"A dead worker cannot lose work... a worker that stalls and comes back finds its write refused
rather than overwriting whoever took over"* (README, "Why you might want this"). It fails, with no
error and no log line, in an operation the project's own ADR treats as ordinary usage.

### The mechanism

Every backend's fencing check is `phase == Claimed AND epoch == presented`:

- memory: `storage/memory/lease.go:220-224` (Complete), `:283-287` (Extend)
- postgres: `storage/postgres/lease.go:336-349` (`loadLeasedNode`, used by Complete),
  `storage/postgres/lease.go:468-474` (Extend's `UPDATE ... WHERE ... AND epoch = $3`)
- redis: `storage/redis/lua_scripts.go:332-336` (`scriptComplete`),
  `:374-377` (`scriptExtend`)

`epoch` is not a value that is unique for the lifetime of a `(Scope, NodeID)` pair. It is a counter
that lives on the node's storage row/hash/record and is reset to **zero** every time a node is
created:

- memory: `storage/memory/graph.go:67-93` (`create`) allocates via `s.alloc()`, which zeroes the
  record (`storage/memory/scope.go:151-159`, `s.recs[h] = nodeRec{}`) — `epoch` is a `uint64` field,
  so its zero value is 0.
- postgres: `storage/postgres/migrations/0001_init.sql:92` — `epoch bigint NOT NULL DEFAULT 0`.
- redis: `storage/redis/lua_prelude.go:605` — `createNode` sets `'attempt', 0, 'epoch', 0` on every
  fresh hash.

`Claim` increments it from there (`storage/memory/lease.go:176`, `redis` `scriptClaim`
`storage/redis/lua_scripts.go:301`, postgres `claimSQL`'s `epoch = n.epoch + 1` at
`storage/postgres/lease.go:63`). Within one node's continuous lifetime this is exactly the
monotonic fencing token ADR-0011 and the contract (`docs/spec/01-contract.md:25`) describe. But
`RemoveNode` hard-deletes the row/record/hash and its index entry —
`storage/memory/graph.go:367` (`delete(s.index, id)`), postgres's `deleteNodeRow` inside
`storage/postgres/graph.go:434-485`, and redis's `deleteNodeKeys` inside `scriptRemoveNode`
(`storage/redis/lua_scripts.go:179-222`) — and a later `AddNode`/`AddNodes` with the **same**
`NodeID` is, on every backend, indistinguishable from creating a brand-new node: memory's
`materialise` (`storage/memory/graph.go:129-143`) only special-cases an ID that is still present
in `s.index`; postgres's `materialiseAddNodesBatch` (`storage/postgres/graph.go:64-86`) does a
fresh `INSERT` the moment `loadForUpdateByExternal` reports no row; redis's `scriptAddNodes` does
the same the moment `EXISTS` is false. The new row starts at `epoch = 0` again.

### Why that is exploitable without any malice

The contract is explicit that the trust model is cooperative and that it does **not** defend
against a worker that forges an epoch or replays an old ack on purpose
(`docs/spec/01-contract.md:229-232`, §4.6). That is a documented, accepted limitation and this
finding is not a restatement of it. What I am describing requires no forgery and no bad actor —
only an ordinary paused worker (GC stall, container freeze, network partition) doing exactly what
the fencing design exists to make safe, colliding with an ordinary admin operation the project's
own ADR-0036 names as the expected use of `RemoveNode`: *"correcting a mistaken `AddNode`,
retracting a node that should never have been created"* — the natural next step after which is to
add the corrected node back under the same identifier.

Concrete sequence, reproducible on any of the three backends:

1. `Configure(scope, ScopeConfig{MaxAttempts: 1, DefaultLeaseTimeout: 50*time.Millisecond, ...})`.
2. `AddNode(scope, "job", payloadV1)`.
3. `lease1, _ := m.Claim(ctx, scope)` — `lease1.Epoch == 1`. The worker holding `lease1` now stalls
   forever (simulate: just stop calling anything on it).
4. Let the lease expire and get reclaimed (advance the test clock, then `Sweep`, or let a second
   `Claim` reclaim it inline). With `MaxAttempts: 1` the node goes straight to `StatusError` /
   `ReasonTimeout` — terminal.
5. `RemoveNode(scope, "job", CascadeReject)` — succeeds; the node is terminal, not in flight, has
   no successors.
6. `AddNode(scope, "job", payloadV2)` — a new, unrelated generation of "job".
7. `lease2, _ := m.Claim(ctx, scope)` — first-ever claim of the new generation, so
   `lease2.Epoch == 1` too. A legitimate worker is now doing real work under `lease2`.
8. The worker from step 3 finally wakes up and calls `m.Ack(ctx, lease1, oldResult)`.
9. **Expected:** `ErrLeaseMismatch` or `ErrNotFound`. **Actual:** the fencing check is
   `phase == Claimed && epoch == presented`; the new generation's row is at `phase = Claimed`,
   `epoch = 1`, and `lease1.Epoch == 1` — the check passes. The step-8 call completes the
   **new** node with the **old** worker's stale result, releases its successors on the strength of
   stale data, and the legitimate worker holding `lease2` will get `ErrLeaseMismatch` when *it*
   later tries to report the real outcome — the roles are reversed from what the design promises.

This is not a narrow corner: the most common single value any node's epoch ever holds is 1 (its
first claim), so the very first claim of any two generations of the same ID collide by default. No
adversary, no forged token, no clock skew — just an honest worker that was slow and an operator who
did the one cleanup operation the docs describe as ordinary.

### Why nothing already catches this

`Lease.Token []byte` (`store.go:39-46`) is explicitly reserved for exactly this class of problem —
"a future backend may populate with a signed capability" — but no shipped backend populates it
today, so it offers no protection. `docs/adr/0036-...md` specifies removal semantics for successors,
the topological order, and subscriber cursors in detail, but never once mentions what happens to
the *fencing sequence* of a recycled ID. The conformance suite's own fencing tests
(`T-FENCE-STALE-ACK`, `T-FENCE-DOUBLE-ACK`, `dagstoretest/trigger_lease.go:317-341`) both stale a
lease by reclaiming the *same* node record — neither ever deletes and recreates the ID. Postgres is
the one backend that already has the ingredient for a real fix sitting unused: `dagw.nodes.id` is a
`bigserial` that is never reused across a delete, so fencing on `(id, epoch)` instead of
`(node_id, epoch)` would close this for that backend outright. Memory and Redis would need an
explicit generation counter that survives the row's deletion (e.g., a per-scope "highest epoch ever
issued for this NodeID," persisted independently of the node record, and consulted as the starting
point rather than 0 on every create) to get the same property.

**Recommendation:** treat this as a pre-1.0 blocker. At minimum, document the hazard prominently
next to `RemoveNode` (today's doc comment says nothing about it) until it is fixed. The actual fix
does not need to be exotic — a monotonically increasing scope-wide (or NodeID-keyed, tombstoned)
sequence used to seed a new generation's epoch above every value that generation's ID has ever
issued would close it on all three backends without changing the wire shape of `Lease`.

---

## BLOCKER — `Claim`'s batch size has no upper bound anywhere in the stack, and on Redis it turns into a full-server stall

The task's own question — "are the Lua scripts... bounded?" — has a clean answer for every script
except this one.

`ClaimRequest.Max`'s doc comment (`store.go:61-65`) says only "Values below one are treated as
one." There is no ceiling. Follow it through:

- `Manager.ClaimBatch` (`claim.go:85-103`) and `buildClaim` (`claim.go:45-62`) pass `n` straight
  through as `req.Max`; nothing clamps it.
- Redis: `Store.Claim` (`storage/redis/ops_lease.go:17-32`) does `max := req.Max; if max < 1 { max
  = 1 }` — floor only — and hands it to `scriptClaim` as `maxN`
  (`storage/redis/lua_scripts.go:260-317`), whose `while granted < maxN do` loop
  (line 280) scans every candidate kind's ready `ZSET` on each iteration (line 286). Every other
  loop in this same script file is bounded by something: `AddNodes` by `cfg.MaxBatchSize`
  (`storage/redis/lua_scripts.go:28-30`), `Sweep` by its `limit` argument (`:386-398`),
  `promoteScheduled` by the `PROMOTE_CAP` constant (`storage/redis/lua_prelude.go:573-587`). Claim
  is the one path nobody bounded.
- Postgres: `claimLoop` (`storage/postgres/lease.go:97-133`) does one row-locking round trip per
  requested node (`claimOne` at `:73-92`, called from the `for len(res.Leases) < want` loop at
  `:111`) — `Max` nodes means `Max` sequential round trips inside one open transaction holding
  `FOR UPDATE SKIP LOCKED` locks the whole time.
- Memory: `Store.Claim` (`storage/memory/lease.go:140-196`) has the identical unbounded
  `for len(res.Leases) < want` shape at line 164, executed under the scope's single `sync.Mutex`
  (`storage/memory/scope.go:67-68` and its doc comment), so a huge claim blocks every other
  operation on that scope for its duration.
- HTTP adapter: `adapters/http/claim.go:81` — `maxNodes := max(req.MaxNodes, 1)` — the field comes
  straight from the JSON request body with the same floor-only treatment. A client can send
  `{"max_nodes": 5000000}` today.

The one accidental mitigation is `ScopeConfig.MaxInFlight`, checked inside every backend's claim
loop (`storage/redis/lua_scripts.go:281-284`, `storage/postgres/lease.go:100-114`,
`storage/memory/lease.go:165-167`) — but its **default is 0, meaning unlimited**
(`config.go:86-88`), so out of the box it protects nothing, and even configured, it only helps once
enough nodes are already in flight; a first claim against a million-node fresh scope still runs the
full loop.

Redis is the severe case, because Lua execution in Redis is single-threaded and blocks the entire
event loop for the whole script: a client that asks for a few million leases from a scope that has
that much ready work does not just slow itself down, it **stalls every other client on that Redis
instance** — every other scope, every other tenant, every `Complete`/`Extend`/`Claim` from every
other process sharing that Redis — for as long as the script runs. There is no way to abort a
write-performing Lua script short of `SCRIPT KILL` refusing (it has already written) or a full
`SHUTDOWN NOSAVE`. Given the README's own headline claim is "point several instances at the same
Redis... they compete for work correctly," this is precisely the shared resource a single caller's
mistake (or a bug that computes `n` from a dynamic count instead of a fixed page size) can take
down for everyone else on it.

**Recommendation:** clamp `Max` the same way `MaxBatchSize` already clamps `AddNodes` — add a
`ScopeConfig` field (or reuse `MaxBatchSize`) and enforce it in `buildClaim` before the request ever
reaches a store, so every backend gets the protection for free rather than needing three separate
fixes.

*Related, smaller version of the same "not bounded" theme:* the Pearce-Kelly reorder step
(`storage/redis/lua_prelude.go:306-368`, the `addEdgeOrder` port) is a faithful copy of memory's
algorithm (`storage/memory/topo.go`) and is correct, but its cost is proportional to the affected
region, which is unbounded in the worst case (a single edge inserted badly out of causal order in a
large, densely connected scope can touch a large fraction of it). On memory and Postgres that is a
CPU/lock-duration cost paid by the caller. On Redis it is, again, the same single-threaded-stall
risk as above, just triggered by `AddEdges`/`AddNodes` instead of `Claim`. `MaxBatchSize` bounds the
number of *specs* in one call, not the size of the topological disruption one adversarial edge can
cause — worth knowing if a workload builds graphs in something other than roughly causal order.

---

## MAJOR — Two concurrent `Complete` calls can deadlock in PostgreSQL, and the error is not mapped to anything the caller can act on

The task asks directly: does PostgreSQL hold locks in a consistent order, and is there a deadlock?
There is one, and it is real, not theoretical — I want to be precise about exactly when.

The scope-wide advisory lock (`storage/postgres/util.go:63-79`, `lockScopeGraph`) deliberately
serialises every *structural* mutation (`AddNodes`, `AddEdges`, `RemoveEdges`, `RemoveNode`,
`Cancel`, `CancelScope` — all confirmed routed through `beginGraphTx`,
`storage/postgres/graph.go:242-277`) against every other one, and the claim/complete/extend/sweep
hot path deliberately never takes it, relying on `SKIP LOCKED` instead
(the reasoning at `storage/postgres/util.go:67-75` is sound and this is a good design: `SKIP
LOCKED` never blocks, so `Claim`/`Sweep` genuinely cannot deadlock against anything).

`Complete`, however, does **not** take the scope-wide lock (correctly, by that same design — it
would serialise the whole scope's completions otherwise) and its termination cascade,
`terminate` (`storage/postgres/engine.go:400-451`), acquires row locks with a plain, blocking
`FOR UPDATE` (`loadForUpdate`, `:250-254`) as it walks a breadth-first queue. Each node's *direct*
successor set is locked in ascending internal-id order (`successorsForUpdate`,
`:256-278`, `ORDER BY c.id`) — but that ordering is only local to one node's own successor list.
Across two or more BFS *levels*, in two different transactions with two different roots, the
ordering is not globally monotonic, because a node reached indirectly (through a deeper level) can
have a smaller id than one reached directly (at a shallower level) in the same cascade.

Concrete graph that produces the crossed lock order (ids increase in the order shown, which is what
`ORDER BY c.id` sees):

```
A(id=1) --> C(id=5)
A(id=1) --> F(id=100) --> D(id=6)
B(id=2) --> D(id=6)
B(id=2) --> F(id=100)
```

This is a valid DAG (A and B are independent roots; F precedes D; both A and B precede F and D
directly, which is legal — D simply has two predecessors). `Complete(A)`'s BFS: lock `A`; its direct
successors sorted ascending are `[C(5), F(100)]`; dequeue `C` (lock 5), dequeue `F` (lock 100), *then*
`F`'s own successor `D` is enqueued and dequeued last (lock 6). **T1's lock order: 1, 5, 100, 6** —
note 100 is locked before 6.

`Complete(B)`'s BFS: lock `B`; its direct successors sorted ascending are `[D(6), F(100)]` (6 < 100,
both are direct), so `D` is dequeued and locked first, then `F`. **T2's lock order: 2, 6, 100** —
note 6 is locked before 100.

If `A` and `B` are two independent, ready root nodes claimed by two different workers who `Ack`
concurrently, T1 holds `F(100)` while waiting for `D(6)`; T2 holds `D(6)` while waiting for `F(100)`
— a textbook AB-BA deadlock. PostgreSQL's own deadlock detector will find this (default
`deadlock_timeout` 1s) and abort one transaction with `40P01 deadlock detected`. That is not a hang
— but it is also not mapped to any `dagworker` sentinel anywhere in `storage/postgres/lease.go` or
`errors.go`; it surfaces from `terminate`'s `loadForUpdate`/`UPDATE` calls
(`storage/postgres/engine.go:413-434`) as a plain wrapped `fmt.Errorf("postgres: terminate: ...: %w",
err)`. The caller's `Ack`/`Nack` gets back an error that is not `ErrLeaseMismatch`, not a context
error, not anything the contract's `§4.3` ("MUST NOT retry a fenced mismatch... MUST NOT report it
as transient") gives guidance on, and nothing retries it automatically. The work the worker actually
did is safe (the whole transaction rolled back, so nothing was recorded), but the worker has no
principled way to know that from the error it received, and no code path resubmits the `Ack` for it.

This needs a wide, "several independent roots feeding a shared diamond at different depths"
shape to trigger, and one root at a time is fine (the deadlock only appears with real concurrency
between two `Complete` calls), which is presumably why it has not shown up in the existing test
suite — nothing in `dagstoretest` or `test/e2e` completes two nodes concurrently against a graph
shaped like the one above.

**Recommendation:** either retry a `40P01` inside `Complete`/`Extend` (the transaction already
rolled back cleanly, so retrying the same fenced write is safe and idempotent by construction), or
map it to a documented, explicitly-retryable sentinel so a caller's own retry loop can distinguish
"transient, try again" from "fenced, do not."

---

## MINOR — a durable subscription reports a clean end (`Err() == nil`) when `Manager.Close()` actually terminated it

`Manager.Close`'s doc comment (`manager.go:107-112`) promises the caller "no callback firing and no
channel being written after it returns," and its actual shutdown path
(`manager.go:113-142`) is careful about *why* each subscription ended: for the in-process fan-out
path, the per-subscription goroutine (`subscribe.go:190-201`) deliberately does **not** call
`s.finish` on the `<-m.closed` branch, leaving that to `Close` itself
(`manager.go:130-140`, `s.finish(ErrClosed)`), specifically so `Subscription.Err()` correctly
reports `ErrClosed` for a manager-driven shutdown versus `nil` for a caller-driven
`Subscription.Close()`.

The durable path (`Subscribe` with `Durable`/`From`/`Replay` set) does not carry this distinction
through. `startDurable`'s goroutine (`subscribe.go:206-243`) has a single unconditional
`defer s.finish(ctx.Err())` (line 219) that runs on **every** return path, including the
`case <-m.closed: return` branch (line 235). If the *caller's own* context is still live when
`Manager.Close()` runs (the ordinary case — nothing requires a Subscribe caller to tie its context
to the Manager's lifetime), `ctx.Err()` is `nil` at that point, so the subscription finishes with
`s.err == nil` — indistinguishable from a graceful, caller-initiated close. By the time `Close`'s own
loop (`manager.go:130-140`) reaches this subscription, `s.done` is already `true`
(`closeLocked`, `subscribe.go:69-79`, is a no-op once `done`), so `s.finish(ErrClosed)` there does
nothing.

Nothing is lost or leaked — the channel is closed correctly and the goroutine exits either way —
but a caller that branches on `Subscription.Err()` to decide "was this a clean end or did the host
shut down under me" gets the wrong answer specifically for durable subscriptions, and only for
durable ones; the two subscription kinds disagree on the same documented contract. Worth a one-line
fix (only call `s.finish(ctx.Err())` on the `ctx.Done()`/`src`-closed paths, and let `Close`'s own
`ErrClosed` loop handle the `m.closed` case exactly as the non-durable path already does).

---

## Verified — things I went looking for a hole in and did not find one

Worth stating plainly, since a review that only lists problems reads as more alarming than the code
deserves:

- **Two workers can never be granted the same node.** On memory, every mutating operation holds the
  scope's single `sync.RWMutex` for its entire duration (`storage/memory/scope.go:67-68`,
  `lease.go:151-152`), so `Claim` is trivially atomic. On PostgreSQL, `claimSQL`
  (`storage/postgres/lease.go:53-68`) is one `WITH ... FOR UPDATE SKIP LOCKED` CTE chained into the
  granting `UPDATE`, so two racing transactions can never lock, let alone grant, the same row — the
  `T-CLAIM-ATOMIC` conformance test and `TestTwoInstancesNeverDoubleDispatch` both exercise this
  under real concurrency and both pass with `-race`. On Redis, the entire claim body is one Lua
  script; Redis's single-threaded execution model makes the "atomic" half of the port's contract
  free.
- **Self-edges and duplicate edges are handled consistently and correctly across all three
  backends.** A spec that names itself as its own dependency is rejected at validation time
  (`node.go:299-311`, `NodeSpec.Validate`) before it ever reaches a store; a public `AddEdge(from,
  from)` is separately rejected at the manager (`manager.go:388-390`) and independently inside every
  backend (memory `storage/memory/graph.go:257-259`, redis `storage/redis/lua_scripts.go:131-134`,
  postgres `storage/postgres/graph.go:338-339`). Re-adding an edge that already exists is a
  documented, tested no-op on all three (`hasEdge` guards in memory/redis; the `EXISTS` check in
  postgres `linkDependency`, `storage/postgres/engine.go:549-559`) — I traced the case where the
  same self-reference could slip through `AddNodes`' forward-linking path
  (`storage/memory/graph.go:147-172`) instead of the single-edge path and confirmed
  `NodeSpec.Validate` already closes it upstream, so there is no backend-specific gap here.
- **An edge into an already-terminal node is rejected on all three backends**
  (`ErrAlreadyTerminal`: memory `storage/memory/graph.go:161-163` and `:265-267`, redis
  `storage/redis/lua_scripts.go:80-84`, postgres `storage/postgres/engine.go:560-562`), which is what
  makes ADR-0036's "a terminal node never regains an unresolved dependency" invariant actually hold
  rather than merely being asserted in prose.
- **Per-edge satisfaction is genuinely idempotent, and dependency counters do not drift.** Every
  backend gates the decrement/increment on a per-edge boolean rather than a bare counter:
  memory's `markSatisfied` checks `edges[i].satisfied` before flipping it
  (`storage/memory/scope.go:395-417`); postgres's `markEdgeSatisfied` is an `UPDATE ... WHERE
  satisfied = false` whose `RowsAffected()` gates everything downstream
  (`storage/postgres/engine.go:280-292`); redis's Lua does the same pattern. A repeated fan-out (the
  same predecessor's terminal transition observed twice, e.g. by two racing sweepers) costs a
  no-op, never a double count. `RemoveEdge` on an edge that already resolved is likewise a
  documented, implemented no-op (ADR-0036's point 3, matched by `unlinkEdge`'s guarded decrements at
  `storage/memory/scope.go:451-494` and postgres's `unlinkDependency` at
  `storage/postgres/engine.go:609-639`, both using a "was already unsatisfied" check before
  touching the tally).
- **The fencing check is `phase AND epoch`, not epoch alone**, on all three backends. This matters
  because a reclaim (`failAttempt`) never bumps the epoch by itself — only a fresh `Claim` does — so
  an implementation that fenced on epoch alone would let a late `Extend`/`Complete` land against a
  node that has moved to `Scheduled` or `Done` but still carries the same epoch value the stale
  worker holds. All three backends correctly also check phase (`storage/memory/lease.go:221`,
  `storage/postgres/lease.go:344`, `storage/redis/lua_scripts.go:334`), closing that gap. (This
  makes the BLOCKER above more interesting, not less: the phase+epoch combination is correct for a
  node's *own* lifetime and still insufficient the moment the node's identity is deleted and
  reissued, because phase resets to non-`Claimed` for a brand-new row too.)
- **The Pearce–Kelly incremental topological order is correct**, including the edge cases the task
  calls out. I traced `forwardSearch`/`backwardSearch`/`reorder` in `storage/memory/topo.go` by hand
  against a mutual-dependency batch (`A` and `B` both declared in one `AddNodes` call, each
  depending on the other) — `materialise` creates both nodes before `linkBatch` links any edge
  regardless of their order in the batch, so the *cycle check itself*, not batch ordering, is what
  correctly rejects the mutual dependency. The self-edge case (`x == y`) cannot reach
  `addEdgeOrder` at all, because both call sites reject it before the topo code runs (see above).
  Postgres's port (`storage/postgres/topo.go`) is a line-for-line translation onto SQL reads under
  the same scope-wide lock, and Redis's (`storage/redis/lua_prelude.go:306-368`) is the same
  algorithm again. All three pass the same conformance suite, including reorder-heavy insertion
  orders.
- **Goroutine lifecycle is clean.** `Manager.Close` (`manager.go:113-142`) genuinely waits on
  `m.wg.Wait()` before returning, and every background goroutine (`maintain`, the non-durable and
  durable subscription pumps) selects on `m.closed`/`ctx.Done()` and exits promptly; I did not find
  a path that fires after `Close` returns for the in-process side. `storage/postgres/notifier.go`'s
  `run()` is properly tracked by its own `wg` and `stop()` waits for it; the per-`listenOnce`
  bridging goroutine (`storage/postgres/notifier.go:140-146`) is short-lived by construction (it
  exits the instant its local `ctx` is cancelled by the enclosing `defer cancel()`) and is not a
  leak, just untracked, which is fine given its bounded lifetime. Both storage backends'
  `Store.Close()` deliberately do **not** wait for `Watch`/`WaitForWork` goroutines to fully exit —
  they only unblock them — and say so identically in their doc comments
  (`storage/memory/memory.go:259-260`, `storage/postgres/postgres.go:168-170`); that is a
  consistent, intentional, and correctly weaker contract than `Manager.Close`'s, not an
  inconsistency between the two backends.
- **A dead worker's node is not silently stranded** under the documented configuration. Every
  `Claim` inline-reclaims expired leases before looking for ready work
  (`storage/memory/lease.go:158`, `storage/postgres/lease.go:159-166`, redis
  `storage/redis/lua_scripts.go:272-273`), and the background `maintain` loop
  (`manager.go:403-429`) sweeps every scope on an interval when enabled (the default). The one way
  to actually strand a node is to call `WithoutBackgroundSweeper()` *and* have no future `Claim` ever
  happen on that scope again — and the code's own comment on `maintain` already says as much
  ("a dead worker in an otherwise idle scope is noticed promptly... which might be hours away"
  without it). That is a disclosed tradeoff, not a hidden one.

---

## Verdict

The concurrency primitives this project is built around — the fenced CAS, the `SKIP LOCKED`
claim path, the scope-wide advisory lock for structural edits, the Pearce-Kelly incremental
order — are implemented correctly and consistently across all three backends, and the parts I could
attack with a mental adversary (self-edges, duplicate edges, terminal-node edges, per-edge
idempotency, phase-vs-epoch fencing, goroutine shutdown) held up under close reading and, where I
could reach a live backend, under `-race` and real multi-instance contention. That is not nothing:
most projects at this stage have at least one of those wrong, and this one does not. But "no node
is ever double-claimed" is not the whole of the lease protocol's job, and the two places I found
real problems are both in the part of the job that is easy to leave unexamined because the happy
path never exercises it: what happens when identity is not eternal (a deleted-and-recreated node ID
quietly resets the fencing sequence that is supposed to be forever-increasing, on all three
backends, defeating the exact "stale worker comes back" scenario the whole design exists to
handle), and what happens when a caller asks for more than a sane amount of anything (an unbounded
`Claim` batch size that turns into a full outage on the Redis backend specifically, because nothing
else in that Lua surface was left unbounded — this one clearly was, by omission rather than
design). Neither is exotic to trigger, and neither is caught by the otherwise very thorough
conformance suite. I would not stake a production system that recreates node IDs, or that exposes
the HTTP/gRPC surface to a caller who can pick its own batch size, on this code today without first
closing the epoch-recycling gap and putting a ceiling on `Claim`. Everything else here is
genuinely solid work.
