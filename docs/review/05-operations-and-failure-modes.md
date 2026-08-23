# Operations and Failure Modes

Reviewed as the person who gets paged. Scope: production behavior under real failure
conditions, observability, `cmd/dagworkerd` as a deployable, resource bounds, and
retention/GC. Every claim below is cited to a file:line I actually read; the surrounding
context for each was re-verified against `go build`/`go vet` (both clean) rather than
taken on faith from a comment.

The short version: the concurrency and fencing core is careful, well-tested, and mostly
does what its ADRs say. The two places nobody stress-tested — the claim-batch size and the
background maintenance loop — are exactly the two places a 3am incident will start.

---

## 1. `Manager.ClaimBatch`'s size is unbounded end to end — a live remote DoS (BLOCKER)

`docs/spec/01-contract.md:407` states the normative complexity contract: `Claim | O(log R)`.
Nowhere does that table parameterize `Claim` by batch size the way it does for `Sweep (batch
m) | O(m log n)` a few lines below it (`docs/spec/01-contract.md:410`). But `Manager.ClaimBatch`
takes a caller-supplied `n` and every layer beneath it multiplies the real cost by `n` with no
ceiling anywhere:

- `claim.go:85` — `ClaimBatch(ctx, scope, n int, ...)` passes `n` straight through:
  `req, err := m.buildClaim(scope, max(n, 1), opts)` (`claim.go:96`, via `buildClaim` at
  `claim.go:45`). No upper clamp exists in `buildClaim`, `ClaimRequest`, or `ScopeConfig`.
- `storage/memory/lease.go:140-192` — `Claim` takes `s.mu.Lock()` (the **whole scope's**
  mutex) and loops `for len(res.Leases) < want` (`want := max(req.Max, 1)`, line 161) with no
  ceiling, appending a full `Node` snapshot (up to the 256 KiB payload cap, per
  `docs/adr/0026`) into `res.Leases` on every iteration. A large `n` against a scope with a
  large ready set holds that scope's single mutex — blocking every other `Claim`, `Ack`,
  `AddNodes`, and `Inspect` call against it — for as long as the loop runs, and can build a
  multi-gigabyte response in one call.
- `storage/redis/ops_lease.go:29-30` — `max := req.Max; if max < 1 { max = 1 }`. Same
  absence of a ceiling, handed to the Lua script as `ARGV`.
- `storage/redis/lua_scripts.go:280` — `while granted < maxN do ... end` inside
  `scriptClaim`. This loop runs **inside one atomic Lua script**, and Redis executes Lua
  scripts on its single event-loop thread with the entire keyspace blocked for the duration —
  not just the scope being claimed against. A caller who asks for a few hundred thousand
  nodes on a Redis-backed deployment with a well-populated ready set does not slow down one
  scope; it freezes the entire Redis instance — every other scope, every other tenant, every
  health check — for as long as the script runs. This is precisely the failure mode the
  package's own doc comment says the `{scope}` hash-tag design exists to avoid
  (`storage/redis/redis.go:41-50`, "a scope is the natural unit of horizontal scale-out"); a
  pathological `Max` defeats that isolation completely, because Cluster slot ownership does
  not help when one node's event loop is the bottleneck.
- `storage/postgres/lease.go:97-129` — `claimLoop` has the identical shape: `for
  len(res.Leases) < want` with `want := max(req.Max, 1)` (line 168), one round trip per node,
  inside one transaction holding `SKIP LOCKED` row state the whole time. Less catastrophic
  than Redis's single-thread stall, but still an unbounded-length transaction and an
  unbounded result set built in server and client memory.
- `adapters/http/claim.go:81-84` — and here is the part that makes this a security finding,
  not just a footgun for a trusted embedder: `maxNodes := max(req.MaxNodes, 1)` comes
  straight off the wire (`MaxNodes int` in `adapters/http/wire.go:374`, an ordinary JSON
  field with no range validation), and is handed to `ClaimBatch` unmodified. **The HTTP
  worker-facing endpoint has no authentication (§8 below).** Any client that can reach
  `POST /v1/scopes/{scope}/nodes:claim` — which, per the daemon's own
  `cmd/dagworkerd/docker-compose.yml`, is a published, non-loopback port — can send
  `{"max_nodes": 999999999}` in a few dozen bytes and take down the shared Redis instance for
  every tenant on it, or wedge a Postgres-backed scope's claim path, or freeze an
  in-memory-backed scope for every other worker.

`Manager.AddNodes` gets this right — `MaxBatchSize` is a real, enforced, per-scope-configurable
ceiling (`storage/memory/graph.go:106`, `storage/redis/ops_graph.go:42`,
`storage/postgres/graph.go:39`, all rejecting with `ErrInvalidArgument` over the limit). There
is no reason `Claim`'s `n`/`Max` should be treated differently, and the `SweepBatchSize` field
already establishes the precedent of a batch-size knob for exactly this kind of unbounded-loop
risk — it simply was never applied to `Claim`.

**What I'd demand before shipping:** clamp `ClaimRequest.Max` to `ScopeConfig.MaxBatchSize` (or
a dedicated `MaxClaimBatch`) in `buildClaim`, at the one place all three backends already
share, and reject values above it the same way `AddNodes` does.

---

## 2. The background maintenance loop has no per-call timeout and no fault isolation between scopes (BLOCKER / MAJOR)

`manager.go:403-434`:

```go
func (m *Manager) maintain(ctx context.Context) {
    ...
    for {
        select {
        case <-m.closed:
            return
        case <-m.cfg.clock.After(interval):
        }
        scopes, err := m.store.Scopes(ctx)
        ...
        for _, scope := range scopes {
            if m.isClosed() { return }
            m.sweepScope(ctx, scope)
            m.collectScope(ctx, scope)
        }
    }
}
```

`ctx` here is `bgCtx`, built at `manager.go:76` as `context.WithCancel(context.Background())`
— cancelled only by `Close`. It carries no deadline. `sweepScope` (`manager.go:431-444`) calls
`m.store.Sweep(ctx, scope, 0)` with that same undeadlined context, and `collectScope`
(`manager.go:446-469`) does the same for `CollectTerminal`. Neither the Postgres nor the Redis
backend sets a server-side statement timeout anywhere I could find
(`storage/postgres/postgres.go` has no `statement_timeout`, and `pgxpool.Config` is never
customized with one) — so a wedged connection, a lock held by another transaction, or a
half-open TCP session during a network partition blocks that `Sweep` call for as long as the OS
takes to notice the dead socket, which without keepalive tuning can be tens of minutes.

The failure this produces: the maintenance loop is **one goroutine, iterating scopes
sequentially, with no per-call timeout**. If any single scope's `Sweep` or `CollectTerminal`
call hangs — the exact shape of "database is slow" or "network partition to the database" —
the entire loop stops making progress. Not just for the hung scope: every scope enumerated
after it in that tick never gets swept or collected either, and no further ticks ever fire,
because the goroutine is parked inside the one call that never returns. There is no log line
for this (the `WarnContext` calls at `manager.go:417` and `manager.go:435` only fire on an
*returned* error, not on a call that never returns), and no metric — `dagworkerd`'s `/metrics`
doesn't touch the `Manager` at all (§9). An operator would observe this only indirectly: dead
workers in otherwise-idle scopes stop getting reclaimed, and a configured `TerminalRetention`
silently stops collecting, fleet-wide, with zero signal that either has happened.

The blast radius is real but bounded by one mitigating fact the code itself documents
(`manager.go:405-407`): "Expired leases are also reclaimed inline by whoever next asks for
work... This loop exists so that a dead worker in an otherwise idle scope is noticed promptly."
So a scope under active load keeps working correctly — inline reclaim on the `Claim` path uses
the *caller's* context, not `bgCtx`, and is unaffected. The actual damage is: (a) any
**idle** scope's dead-worker leases now linger indefinitely (until something claims against
that scope again, which for an idle scope may be never), and (b) `TerminalRetention`-driven GC
silently stops **everywhere**, for every scope in the process, the moment any one scope's
backend call hangs — turning a documented, deliberately-opt-in disk-growth control (§5, ADR-0034)
into something that can silently stop working during precisely the kind of incident (DB under
load, partial network partition) where storage pressure is most likely to already be a
problem.

This is also, tellingly, untested. `fault_test.go`'s `faultStore` (lines 21-75) injects
*immediate* errors (`return dw.SweepResult{}, errInjected`) for `Sweep`, `Scopes`,
`ScopeConfig`, `CollectTerminal`, `Claim`, and `WaitForWork` — a good, deliberate test of "the
backend answers, but with an error." Nothing in `fault_test.go`, `gaps_test.go`, or the
`test/e2e` suite injects latency or a call that never returns. The exact failure mode a
real outage produces (slow, not immediately-erroring) is the one gap in an otherwise
thoughtful fault-injection harness, and it is the gap that would have caught this.

**What I'd demand before shipping:** wrap each `sweepScope`/`collectScope` call in a bounded
`context.WithTimeout` derived from `bgCtx` (a few seconds is plenty — these are supposed to be
cheap, indexed operations), and make one scope's timeout a logged, counted event rather than a
silent stall for the rest of the fleet's scopes.

---

## 3. Redis's blocking-`Claim` wakeup opens one dedicated connection per blocked caller (MAJOR)

`storage/redis/watch.go:179-216`:

```go
func (s *Store) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
    ...
    sub := s.rdb.Subscribe(ctx, s.keyBell(scope))   // line 195
    defer func() { _ = sub.Close() }()
    ch := sub.Channel()
    ...
}
```

`go-redis`'s `PubSub.Subscribe` acquires and **holds** a dedicated connection from the client's
pool for the entire life of the subscription (verified against
`go-redis/v9@v9.22.0/pubsub.go:74`, `conn()` — the connection is cached on `c.cn` and not
released until `Close`). `storage/redis/redis.go:140` opens the client with
`goredis.NewClient(&goredis.Options{Addr: addr})` and never sets `PoolSize`, so it defaults to
`10 * runtime.GOMAXPROCS(0)` (confirmed in `go-redis/v9@v9.22.0/options.go:497-498`) — commonly
20-40 connections on a typical container.

Every blocked `Manager.Claim` call against this backend, once it exhausts its immediate attempt,
calls `waitForWork` (`claim.go:157-179`), which for a doorbell-capable backend opens exactly
this kind of `WaitForWork` call per invocation of the wait loop. On an idle scope with, say, 50
concurrently blocked workers — a perfectly ordinary fleet size, and far below the "10,000 idle
blocked workers" figure `docs/adr/0033` itself uses as a sizing example for the poll-load
tradeoff — those 50 blocked callers alone can exceed the default connection pool, starving
every *other* Redis operation on that client (claims that already have work ready, `Ack`,
`AddNodes`, the daemon's `/readyz` probe) of a connection. This is not a slow-degradation
story; once the pool is exhausted, unrelated calls start blocking or erroring, so a fleet that
merely has more idle workers than the pool has connections can go from "healthy" to "backend
unreachable for everything" without any change in load on the graph itself.

Contrast this with the other two backends' mechanics, which get it right: the in-memory
backend's doorbell is a plain broadcast channel with zero connection cost
(`storage/memory/watch.go:78-101`), and Postgres shares **one** long-lived `LISTEN` connection
across every scope and every blocked caller via `notifier` (`storage/postgres/notifier.go:21-30`,
one connection, fanned out in-process through a `map[string]chan struct{}`). Redis is the one
backend where blocked-caller count is directly proportional to consumed connections, and nothing
in the code, the README's backend comparison table, or `docs/adr/0033`'s "per-backend doorbell
mechanics" section flags this as a sizing constraint an operator needs to plan for.

**What I'd demand before shipping:** either multiplex all of a `Store`'s `WaitForWork` calls
onto one shared subscription per scope (mirroring the Postgres `notifier` design, which this
codebase already knows how to build), or document the connection-pool sizing formula
(`idle workers per scope × scopes ≤ PoolSize`) prominently enough that nobody discovers it by
running out of connections in production.

---

## 4. The thundering-herd-safe doorbell described in ADR-0033 does not exist; every backend broadcasts (MAJOR — normative ADR contradicted by shipped code)

`docs/adr/0033` is unusually specific and unusually emphatic about one design point: the wakeup
mechanism must be a **counting signal**, not a broadcast, because broadcast-wake-all is "precisely
the thundering-herd cost this ADR's counting-signal design exists to avoid" (ADR-0033,
"Alternatives considered"). The decision includes concrete pseudocode for a `doorbell.Register`/
`doorbell.Signal(scope, kind, n)` type living in the core, where `Signal` "wakes at most `n`
waiters — `n` being the number of nodes the write that just committed actually readied... never
every registered waiter."

None of that exists. There is no `doorbell` type anywhere in the core module (`grep` for `type
doorbell` and `m.doorbell` across every non-storage, non-test `.go` file returns nothing). The
actual implementation in `claim.go:117-179` is a plain retry loop that, when a doorbell is
available, calls the backend's `WaitForWork` once per iteration inside a bounded
`context.WithTimeout`. Each backend's `WaitForWork` is, in turn, a **broadcast**:

- `storage/memory/watch.go:78-101` — `ringDoorbell` does `close(s.doorbell); s.doorbell =
  make(chan struct{})`. Closing a channel is Go's idiomatic broadcast primitive: every
  goroutine currently selecting on `bell` at `WaitForWork`'s `case <-bell:` (line 98) wakes at
  once. There is exactly one doorbell channel per **scope** (not per `(scope, kind)` as
  ADR-0033 specifies), so a transition on any kind wakes every waiter blocked on any kind in
  that scope.
- `storage/postgres/notifier.go:71-76` — `ring` does `close(ch); delete(n.bell, scope)`. Same
  broadcast-on-close idiom, same "every waiter for this scope, not just readied-count many."
- `storage/redis/watch.go` — plain Redis pub/sub (`PUBLISH`/`SUBSCRIBE`), which is
  broadcast to every subscriber on the channel by construction; there is no mechanism in the
  protocol to wake only N of them.

So the exact "Alternatives considered — Rejected" design in ADR-0033 is what shipped, on all
three backends, unanimously. Correctness is not at risk — every ADR-0033 says about lost/spurious
wakeups being harmless still holds, because a woken loser just re-registers and retries
(`claim.go:117-127`) — but the specific performance property the ADR spent several paragraphs
justifying ("bounds the thundering-herd cost to O(readied nodes), not O(blocked waiters)") is
false for the shipped code, which is O(blocked waiters) on every readiness event, every backend,
by construction. Under exactly the load pattern the ADR calls out as the risk case — a large
fan-out completing and readying many successors at once, with many idle workers parked on the
scope — every parked worker wakes simultaneously and re-attempts `Claim`, contending for the
same scope's lock (memory) or issuing a burst of redundant `EVAL`/transaction round trips
(Redis/Postgres), most of which lose the race and go back to sleep.

Neither `docs/adr/0041` (amendments discovered during implementation) nor `docs/adr/0042`
(backend deviations) — the two documents that exist specifically to reconcile "what the ADR
says" against "what got built" — mentions this at all (`grep -i poll` and inspection of both
files turn up nothing about the doorbell design). This is the kind of gap the review brief
asked me to check for directly: a normative ADR whose central claim about the shipped system's
behavior is not true, and the project's own deviation-tracking process didn't catch it.

**What I'd demand before shipping:** either implement the counting-signal doorbell ADR-0033
actually specifies, or write the ADR that supersedes it and states honestly that all three
backends broadcast — and then re-evaluate whether the "10,000 idle workers" sizing guidance
elsewhere in that ADR is still valid under the real (worse) wakeup cost.

---

## 5. Per-scope `SweepInterval` is stored, validated, and exposed over every adapter — and never actually consulted (MAJOR — inert configuration)

`ScopeConfig.SweepInterval` round-trips faithfully through every layer: it's resolved with a
conservative fallback (`config.go:146-147`), persisted per-scope in memory
(`storage/memory/scope.go`), Redis (`storage/redis/encode.go:46,66,85`), and Postgres
(`storage/postgres/scope_admin.go:30,89`, `storage/postgres/codec.go:261`), and surfaced through
both network adapters (`adapters/grpc/gen/.../node.pb.go:892`, `adapters/http/wire.go:265`). An
operator can call `Configure(ctx, "hot-scope", ScopeConfig{SweepInterval: time.Second})` and
every part of the system will accept it, store it, and echo it back.

It has no effect. `manager.go:406`:

```go
interval := m.cfg.defaults.Resolved().SweepInterval
```

is computed **once**, from the `Manager`-wide `WithScopeDefaults` option set at construction time
— not from the per-scope stored config — and used as the single ticker period for the loop that
then iterates and sweeps *every* scope on that same cadence (`manager.go:408-433`). A per-scope
`SweepInterval` override changes nothing about how often that scope's leases get proactively
reclaimed or collected in the background; the only way to actually change that cadence is to
change the `Manager`-level default that applies to the whole process.

This matters operationally in the shape the ADRs themselves keep insisting on serving: "many
small scopes with different SLAs" (ADR-0034's whole premise) and "a low-traffic scope vs. a hot
one" (ADR-0033's own note on `WithPollInterval` being a `ClaimOption` for exactly this reason).
An operator who tunes one hot scope's `SweepInterval` down to notice dead workers faster gets
silent no-op; they will not find out until a dead worker's node in that scope sits unreclaimed
for the manager-wide default's duration, and nothing errors, logs, or otherwise indicates the
setting had no effect. It is not validated against — `cfg.validate()` (`config.go:159-179`)
happily accepts and stores any positive `SweepInterval`.

**What I'd demand before shipping:** either derive per-scope-eligible sweep timing from the
stored config (e.g., track a per-scope next-due time seeded from that scope's own
`SweepInterval`), or remove the field and document plainly that sweep cadence is process-wide.

---

## 6. `cmd/dagworkerd`'s shutdown and metrics diverge from ADR-0037, undocumented (MAJOR — documentation trust)

`docs/adr/0037` (Decision, point 4) states plainly what the composition root is supposed to do:
"config layering, `/healthz`/`/readyz` split, **graceful shutdown that actively releases the
replica's in-flight leases** rather than passively waiting out their timeout, **RED/USE
metrics**."

The shipped daemon does neither of those two things, by deliberate, well-reasoned choice — but
a choice the ADR itself was never updated to reflect:

- `cmd/dagworkerd/daemon.go:17-27`'s own doc comment says the daemon "enforces the deviation
  this project's own hard rule makes from that dossier's step 4: never release a lease a worker
  might still be about to acknowledge... this is a deliberate, narrower choice than actively
  reassigning still-live leases on shutdown." I think this is the *right* engineering call —
  actively reassigning a lease a worker might complete a moment later is how you get duplicate
  side effects on a system that otherwise goes to real lengths to keep "at-most-once accepted
  effect" true. But it is the opposite of what ADR-0037 states as the decision, and that
  contradiction is recorded only in a code comment in the module that deviates, not in the ADR
  itself or in `docs/adr/0041`/`0042` (the two documents that exist precisely to catalogue
  exactly this kind of implementation-vs-decision gap — searched, no mention).
- `cmd/dagworkerd/admin.go:94-131`'s `/metrics` exposes four gauges: `dagworkerd_up`,
  `dagworkerd_draining`, `dagworkerd_uptime_seconds`, `dagworkerd_goroutines`. No RED metrics,
  no USE metrics, nothing about the DAG. This is honestly disclosed in
  `cmd/dagworkerd/README.md`'s "Known limitations" section as a consequence of the
  daemon not being allowed to modify the adapter packages to add instrumentation hooks — a
  fair and well-argued constraint. But it is still a normative ADR's stated decision that the
  shipped code does not implement, and — again — the gap is recorded in a README's limitations
  section, not in the ADR whose decision it contradicts, nor in the amendments/deviations
  ledger that exists for this purpose.

Neither divergence is a bug relative to what I'd actually want running in production (I'd take
"leases survive a restart" and "honestly incomplete metrics" over "revoke everything on every
deploy" and "metrics I can't trust" any day). The finding here is about documentation discipline:
this project holds itself to an unusually high standard — two dedicated ADRs exist solely to
reconcile design against implementation — and at least these two normative claims slipped
through that process anyway. If I'm told "check the code against the ADRs," these are the two
places where doing that literally turns up a "no."

**What I'd demand before shipping:** an ADR amendment (or a `0043`) that records both
decisions honestly, so the next reader doesn't have to diff a code comment against `0037` to
discover the daemon doesn't do what its own governing ADR says it does.

---

## 7. `Subscribe`'s fan-out cost is paid by every write in the process, not just the affected scope (MAJOR under the deployment shape the ADRs target)

`subscribe.go:238-269`, `publish`:

```go
func (m *Manager) publish(scope Scope, effects []Effect) {
    if len(effects) == 0 { return }
    m.mu.RLock()
    if len(m.subs) == 0 { m.mu.RUnlock(); return }
    subs := make([]*Subscription, 0, len(m.subs))
    for _, s := range m.subs { subs = append(subs, s) }
    m.mu.RUnlock()

    for _, ef := range effects {
        ev := Event{ ... }
        for _, s := range subs {
            if s.opts.wants(ev) { s.offer(ev) }
        }
    }
}
```

`m.subs` (`manager.go:31`) is a single flat map of every live in-process subscription on the
`Manager`, across every scope — there is no per-scope index. `publish` is called synchronously,
inline, on the caller's own goroutine, from every mutating call: `AddNodes` (`manager.go:198`),
`Claim`/`ClaimBatch` (`claim.go:92`, `claim.go:139`), `Ack`/`Nack`/`Skip`/`Extend`
(`claim.go:230`), `RemoveEdge`/`RemoveNode`/`Cancel` and friends. Every one of those calls, for
every effect it produces, iterates **every subscription anywhere in the Manager** and evaluates
`opts.wants(ev)` (a cheap scope/kind comparison, `event.go:206-216`) before deciding to deliver.
The per-subscriber channel send is genuinely non-blocking (`subscribe.go:88-131`, correctly
implementing ADR-0022's "never block the producer"), so this isn't a liveness bug — but the
*iteration* itself is real, synchronous CPU work on the calling goroutine, and its cost is
`O(total subscriptions in the Manager)` per effect, regardless of which scope the effect
belongs to.

This directly undercuts the "many small scopes" deployment shape the ADRs repeatedly call out
as a first-class target (ADR-0034's whole premise, ADR-0033's `WithPollInterval` reasoning). The
natural monitoring pattern for that shape is one dashboard subscription per scope. A deployment
with a few thousand concurrently-running small scopes, each with its own status subscriber,
pays a few-thousand-iteration tax on **every single `Ack` in every unrelated scope** — a
completely bystander cost with no way to opt out short of not subscribing at all. This is not
the kind of O(n) the README's Performance section measures and defends (that section is about
graph size, not subscriber count), but it is exactly the kind of "unbounded fan-out" the review
brief asked about, and nothing in the code, tests, or docs bounds or even measures it.

**What I'd demand before shipping:** index `m.subs` by scope (a `map[Scope][]*Subscription`
alongside or instead of the flat map), so `publish` only ever iterates the subscribers who could
possibly want the event.

---

## 8. No authentication or transport security on the worker-facing surface at all (MAJOR, and the enabling condition for §1)

`cmd/dagworkerd/README.md`'s "Known limitations" section honestly discloses "No TLS support
yet on the gRPC or HTTP listeners" and explains the operational assumption (TLS termination
belongs to a mesh or load balancer in front of the daemon). What it does not mention anywhere —
and I could not find addressed anywhere in the adapters, the daemon, or the ADRs — is
**authentication or authorization**. `grep` across `adapters/grpc` and `adapters/http` for any
auth-shaped identifier (`auth`, `apikey`, `token`-as-credential, `tls` client cert checking)
turns up only the *lease* fencing token (`adapters/grpc/tasktoken.go`, `adapters/http/leaseid.go`)
— an opaque handle that identifies a specific claimed lease, explicitly documented as carrying
no privilege ("AsWorker records who claimed the node, for observability. It has no bearing on
correctness," `claim.go:31-34`) — not a credential that gates who may call `ClaimNode`,
`AddNodes`, `CancelNode`, or the `POST /v1/scopes/{scope}/nodes:claim` endpoint in the first
place.

ADR-0037 itself gestures at the right shape — "`WorkerService` and `ControlService` are separate
gRPC services specifically so a worker's mTLS identity never needs `AddNodes`/`CancelNode`
privileges by default" (`docs/adr/0037-network-surface-in-scope.md:80-82`) — but that's a
statement about *how an mTLS identity would be scoped if one existed*, not evidence that one
does. Nothing in `grpcadapter.New` or `httpadapter.New` accepts an interceptor, middleware, or
credential-checking hook for the operator to wire one in themselves either
(`adapters/grpc/*.go`, `adapters/http/server.go` — no such extension point).

The practical consequence: `docker-compose.yml`'s own documented stance — "the HTTP and gRPC
ports are published, the admin port deliberately is not... its audience is the orchestrator's own
network, not the public internet" — draws exactly the wrong line for this threat model. The admin
port is loopback-only by default and carries only observability; the *worker-facing* port,
which is the one actually reachable, is the one with no authentication and (per §1) an unbounded
resource-exhaustion primitive sitting behind it. Anyone who can route a packet to the published
`--http-addr` or `--grpc-addr` — a compromised adjacent container, a misconfigured network
policy, a "we'll add auth later" staging environment that outlives its intended lifetime — can
claim work meant for someone else, cancel scopes, add poison nodes, or trigger §1's DoS. In a
"service mesh terminates TLS and enforces mTLS identity in front of me" deployment this is a
reasonable division of labor; the gap is that this expectation is stated for TLS and left
completely unstated for authorization, so an operator following the documented Docker Compose
example as given gets neither.

**What I'd demand before shipping:** at minimum, document the authorization assumption as
explicitly as the TLS one is documented ("this daemon assumes every caller that reaches it is
already trusted; put it behind a mesh/gateway that authenticates before proxying"), and fix §1
so that an untrusted caller reaching this surface cannot take down a shared backend even by
accident.

---

## 9. Retention-off-by-default is the right call — but `dagworkerd` gives you no way to see it coming (MAJOR observability gap)

`TerminalRetention` defaulting to `0` ("never collect") is well-reasoned and I agree with it:
`docs/adr/0034` (line 128) makes the honest argument that a library-wide default aggressive
enough to serve a job-queue tenant would silently destroy a pipeline-shaped tenant's results
before they noticed, and "disabled" fails safe in the correct direction. `config.go:151-153`
implements this faithfully, and the maintenance loop (`manager.go:456-458`) genuinely never
deletes anything unless a scope explicitly opts in — I checked the actual gate, not just the
comment: `if cfg.TerminalRetention <= 0 { return }`.

The problem is what happens to an operator who takes the default at face value for a
long-running deployment and never revisits it — which ADR-0034 itself calls out as "a real
operational footgun that must be... made observable... so it is diagnosable rather than a
silent hang" (Consequences, Negative). That observability was never built:

- `cmd/dagworkerd/admin.go`'s `/metrics` (§6 above) exposes nothing about the graph at all —
  no node count, no per-scope `Total`/`NonTerminal`, nothing that would let an operator graph
  "this scope's node count only ever goes up" and alert on it before it becomes a disk-full or
  OOM incident. `Manager.Stats` (`manager.go:324`, backed by the real `ScopeStats` type at
  `store.go:146-164`, which already has exactly the right field — `Total uint64`) is sitting
  right there, unused by the daemon's own metrics endpoint. This would have been a small
  addition, not a redesign.
- The one thing that *does* correctly log is the collector itself when it runs
  (`manager.go:466-468`, `InfoContext(... "collected terminal nodes" ...)`) — but there's
  nothing logged or counted for the (default, common) case of "retention is off and this
  scope's node count just crossed some concerning threshold." An operator finds out about
  unbounded growth from a disk-full page or an OOM-killed process, not from a dashboard.
- The risk is sharpest exactly where the daemon's own quick-start points a new user first:
  `cmd/dagworkerd`'s default `--store` is `memory` (`cmd/dagworkerd/README.md`'s config table),
  and the in-memory backend's node table lives entirely in the daemon process's own heap with
  no external system to page an alert on. `dagworkerd --store=memory --http-addr=:8080` run as
  a long-lived service for a job-queue-shaped workload that never calls `Seal`+configures
  retention grows that process's RSS forever, silently, until the OS OOM-kills it — and the
  first anyone hears about it is the daemon disappearing.

This isn't an argument for turning retention on by default (ADR-0034's reasoning holds); it's
that the "must be made observable" half of that same ADR's own stated requirement was not
delivered.

**What I'd demand before shipping:** add per-scope `Total`/`NonTerminal` (and ideally
`Sealed`) gauges to `/metrics`, sourced from `Manager.Stats`/`Scopes` — no adapter
instrumentation hook required, unlike the RED-metrics gap in §6 — and put a paragraph in the
`cmd/dagworkerd` README next to the retention config fields making the "in-memory + no
retention + long-running = OOM" risk explicit, not just implicit in an ADR's Consequences
section.

---

## What the library gets right

To be fair, and because a review that only lists problems is as useless as one that finds
none:

- **Fencing is real and I could not find a hole in it.** Every lease-mutating path — `Ack`,
  `Nack`, `Extend`, the sweeper's own reclaim — is a compare-and-swap on the epoch
  (`storage/memory/lease.go:266-296` for `Extend`'s epoch check is representative), so a
  worker that was merely paused (container freeze, GC pause, clock jump forward causing a
  premature reclaim) and comes back finds its write refused with `ErrLeaseMismatch`
  rather than corrupting whatever a replacement worker already recorded. I traced this through
  all three backends and it holds everywhere I checked.
- **A poison node is bounded correctly, including via the timeout path, not just explicit
  `Nack`.** `failAttempt` (`storage/memory/lease.go:43-59`) is, by its own doc comment, "the
  single path for every way an attempt can fail" — and I verified the sweep-driven
  timeout-reclaim path actually calls it (`storage/memory/lease.go:83`,
  `s.failAttempt(h, dw.ReasonTimeout, ...)`), so a node that crashes every worker that touches
  it (never reaching an explicit `Nack`) still exhausts `MaxAttempts` and goes terminal,
  rather than looping forever. The cost is real (each attempt costs a full lease timeout,
  default 30s, before the node cycles back to the ready set — three worker crashes minimum
  before quarantine at defaults), but the behavior is correct and worth planning capacity
  around rather than worth fixing.
- **Shutdown ordering in `cmd/dagworkerd` is genuinely well thought through**
  (`cmd/dagworkerd/daemon.go:196-224`): fail `/readyz` first (before anything else, to hide the
  unavoidable load-balancer polling-interval latency), drain both adapters concurrently while
  `/healthz` keeps answering 200 throughout, close the Manager and store only after draining
  completes, and stop the admin listener last so the orchestrator can observe every step. The
  deliberate choice not to release in-flight leases on shutdown (§6) is the right call even
  though it contradicts the ADR text.
- **Secrets handling is correct and unusually well explained**: `--redis-password-file`/
  `--postgres-dsn-file` take file paths, never values on a flag or env var
  (`cmd/dagworkerd/secrets.go`), the `Config` struct never holds a secret's contents, and the
  startup log line echoes paths only (`cmd/dagworkerd/logging.go:60-75`) — this is exactly the
  right shape and the README's justification for it is precise, not hand-wavy.
- **Zero panics in any non-test, non-generated production code path** (checked with `grep -rn
  "panic(" . | grep -v _test.go | grep -v /gen/`, empty), and the HTTP adapter additionally
  wraps every request in a recovery middleware (`adapters/http/server.go:62-64`,
  `recoveryMiddleware`) — a defense-in-depth layer for the panics that will inevitably show up
  anyway.
- **The e2e multi-instance suite tests the failure modes that actually matter for this
  design**, not just happy-path CRUD: `TestTwoInstancesNeverDoubleDispatch`,
  `TestSurvivingInstanceRecoversADeadOnesWork`, `TestFanOutCrossesInstances` (all in
  `test/e2e/multiinstance_test.go`) exercise exactly the "no coordinator, competing instances,
  one dies mid-job" scenarios a distributed lease system lives or dies by, against all three
  real backends. `TestAbandonedWorkIsRecovered` and `TestGraphGrowsWhileRunning`
  (`test/e2e/lifecycle_test.go`) do the same for the dynamic-DAG-specific risks. This is a
  genuinely above-average test suite for what it covers.
- **`go build ./...` and `go vet ./...` are both clean** across the whole repo, and the core
  module really does have zero non-stdlib dependencies (checked `go.mod`), which is a real,
  verifiable claim, not marketing.

---

## Clock jumps, container pauses, disk full, OOM — the rest of the checklist

- **Clock jump.** Postgres and Redis both correctly use their own server clock for every
  deadline comparison (`clock_timestamp()` in Postgres, `redis.call('TIME')` in Lua — both
  documented in their package doc comments and consistent with ADR-0008), so client-side clock
  skew across a fleet of workers cannot desynchronize lease expiry for those two backends. The
  in-memory backend is the one exception worth naming explicitly: `storage/memory/scope.go:147`,
  `func (s *scope) now() int64 { return s.store.clock.Now().UnixNano() }`, uses the *host
  process's own* wall clock (`SystemClock`, `clock.go:31-33`) directly. Since claimer and
  storage are the same process for this backend there's no cross-machine disagreement to
  worry about, but a wall-clock step backward (an NTP correction, a manually adjusted VM clock)
  can freeze lease-expiry detection for the duration of the jump, and a step forward can cause
  spurious reclaims of leases that are, by any human measure, still well within their timeout.
  The fencing epoch means a spuriously-reclaimed worker's late `Ack` is correctly refused
  rather than corrupting state — this is a duplicated-work risk, not a correctness risk — but
  it's worth a documentation line since none of the backend doc comments call it out for the
  in-memory case specifically.
- **Container pause / GC pause / SIGSTOP.** Handled correctly by the general lease design: a
  paused worker simply stops extending its lease, the lease expires on the *storage* clock
  (never the paused worker's), the node is reclaimed and handed to someone else, and if the
  original worker resumes and tries to `Ack`, the epoch fence refuses it. This is the system
  working as designed, not a gap.
- **Disk full / storage write failures.** Nothing library-specific to say here — Postgres and
  Redis both surface ordinary write errors, which propagate up through `Store.Complete`/`Claim`/
  etc. as ordinary errors rather than being swallowed or panicking (consistent with the
  "zero panics" finding above). I did not find a code path that treats "the backend rejected a
  write" as anything other than "the backend rejected a write."
- **A worker that hangs vs. a worker that crashes.** Both are the same case from the library's
  point of view — no heartbeat/no `Ack` before the deadline — and both are handled identically
  and correctly via lease expiry. There is no separate "hung but still holding a TCP connection
  open" detection, but there doesn't need to be: the lease deadline is the only signal this
  design needs, by construction.
- **A scope that grows without bound.** Covered in depth in §9 (retention) and §7 (subscriber
  fan-out) above; the short version is "the defaults are safe, but nothing warns you before the
  growth becomes an incident."

---

## Is it observable? Is `Manager.Inspect` enough?

`Manager.Inspect` (`manager.go:302`, backed by `Inspection` at `node.go:318-347`) is a good
building block: for one named node it gives you `Phase` (distinguishing blocked-vs-ready, which
`Status` deliberately doesn't — `node.go:321-324`), `Waiting []NodeID` ("the predecessors that
are not yet terminal... 'why is this node stuck', which is the first question every operator
asks" — the comment is accurate, it does answer that question), `Successors`, `Rank`,
`LeaseDeadline`, and `ReadyAt`. If you already know which node is stuck, this genuinely tells
you why.

It is not, by itself, enough to *find* the stuck node in a graph you haven't already localized
a problem in. There is no bulk query for "nodes that have been `Blocked` or `InProgress` longer
than X," and `ListNodes`' `ListOptions` (`store.go:337-346`) filters only by `Statuses` and
`Kinds`, not by age or lease-deadline proximity — so diagnosing "why has this 50,000-node scope
stalled" from the outside means listing every non-terminal node and calling `Inspect` on each
one yourself, client-side, with no library help narrowing the search. `ScopeStats`
(`store.go:146-169`) gives you the aggregate counts (`Blocked`, `Ready`, `InProgress`, etc.) for
free and O(1), which is a genuinely good first triage signal ("is this scope actually stuck, or
just busy") — but getting from "the aggregate looks wrong" to "here is the specific node" is
entirely on the caller, and `dagworkerd` doesn't even expose the aggregate (§9).

Logging is minimal by design (six call sites total in the entire core module, all `Warn`/`Info`,
all in `manager.go`/`claim.go` — the library correctly defaults to `slog.DiscardHandler`,
`config.go:81-83`, so it never writes anywhere uninvited) and there is no metrics/instrumentation
hook of any kind in the core — no callback, no `expvar`, nothing. That is a defensible design for
an embeddable library with a zero-dependency goal, but it means every bit of production
observability for a `dagworker`-based system is the *host application's* responsibility to
build by subscribing to events or polling `Stats`, and `cmd/dagworkerd` — the one place that
could have done this work once for everyone who doesn't want to embed — does the least it could
plausibly do and says so.

---

## Would I deploy this? What would I demand first?

I would deploy the *library*, embedded, behind my own worker code, on the in-memory or Postgres
backend, for a moderate-scale job-queue or pipeline workload, today — the fencing design is
sound, the test suite covers the scenarios that actually break systems like this, and the
defaults fail in safe directions almost everywhere I checked.

I would not deploy `dagworkerd` as a network-facing service on the Redis backend, and I would
not put the HTTP/gRPC surface on any network segment shared with anything I didn't fully trust,
until:

1. §1 is fixed — `ClaimBatch`/`Claim`'s node count is bounded by the same kind of
   `MaxBatchSize` ceiling `AddNodes` already has, at minimum on the network-facing adapters.
2. §2 is fixed — the background maintenance loop has a per-call timeout, so one slow backend
   call can't silently stop retention and reclaim fleet-wide.
3. §8's authorization gap is at minimum documented as loudly as the TLS gap already is, so
   nobody deploys this thinking the daemon protects itself.
4. §9's per-scope node-count visibility exists in `/metrics`, so "retention is off and nobody
   noticed" is a graph you can see coming instead of a postmortem.

None of these are architectural rewrites — each is a bounded, well-scoped fix to code that
already knows how to do the equivalent thing correctly somewhere else in the same repository
(`MaxBatchSize` for AddNodes, the Postgres `notifier`'s single shared connection, `Manager.Stats`
already computing the numbers `/metrics` needs). That's the most reassuring thing I can say about
this codebase: the fixes look like finishing work that was already half-done, not warning signs
of a design that doesn't hold together.

## Verdict

This is a carefully designed system with an unusually rigorous paper trail — the fencing
protocol, the atomicity contract, and the multi-instance test suite are the real thing, and I
would trust the core library's correctness under concurrent, competing, occasionally-crashing
workers more than most production schedulers I've reviewed. But the operational surface has not
been stress-tested the way the correctness surface has: nobody asked "what does a caller with a
huge number handed to `n` do to this," "what happens when the database is slow rather than
down," or "who's allowed to call this over the network" with the same rigor that went into "what
happens when two instances race for the same node." The result is a project I'd trust with my
data and not yet with my uptime — every blocker above is a few days of focused work in a
codebase that's clearly capable of doing it right, not evidence of a foundation that needs
rethinking.
