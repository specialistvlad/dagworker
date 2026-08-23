# Review: Public API and Go Idiom

Scope: the root package (`manager.go`, `claim.go`, `subscribe.go`, `store.go`, `typed.go`,
`errors.go`, `event.go`, `node.go`, `status.go`, `config.go`, `clock.go`, `identifier.go`,
`options_node.go`, `backoff.go`, `doc.go`). `go build ./...`, `go vet ./...`, `go test .` and
`golangci-lint run ./...` (the project's own strict, allowlist-based config) all pass clean on
this package — that's worth stating up front, and it means most of what follows is about
behavior and design, not style, because style is genuinely well policed here.

Three of the findings below (#1, #2, #6) are backed by a standalone reproduction built against
the real package (not the test doubles already in the repo), because the existing test suite
either doesn't exercise the path or actively masks the behavior. Repro code is inline.

---

## 1. BLOCKER — a doorbell that errors turns `Claim` into an unbounded busy loop

`claim.go:151-185`, the `waitForWork` helper:

```go
switch err := d.WaitForWork(wctx, scope, kinds); {
case err == nil, errors.Is(err, context.DeadlineExceeded):
    return ctx.Err()
case errors.Is(err, context.Canceled):
    return ctx.Err()
case errors.Is(err, ErrClosed):
    return ErrClosed
default:
    // A doorbell that fails is a degraded doorbell, not a broken claim. Log
    // it and let the caller poll.
    m.cfg.logger.WarnContext(ctx, "dagworker: doorbell failed, falling back to polling", ...)
    return ctx.Err()
}
```

Every branch — including `default` — returns essentially immediately. The intent is clearly "log
and fall back to the poll interval," but there is no poll interval anywhere in this function's
error branches: `wait := jitter(m.pollInterval())` (line 152) is used only to bound `wctx`, the
context handed to `WaitForWork` itself. If the doorbell implementation returns an error
*immediately* rather than blocking until `wctx` is done — which is exactly what "the doorbell is
broken" looks like (connection refused, auth failure, a bug in a third-party backend) — `wctx`
never expires, `WaitForWork` returns at once, the `default` case logs at Warn and returns
`ctx.Err()` (nil, since the caller's context is fine), and `Claim`'s outer loop (`claim.go:118-147`)
immediately calls `m.store.Claim` again. Nothing in this path ever waits.

I confirmed this is not a theoretical reading with a standalone repro (`go.mod` `replace`d onto
this module, not part of the repo):

```go
type faultStore struct {
    dw.Store
    failDoor atomic.Bool
    claims   atomic.Int64
}
func (f *faultStore) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
    if f.failDoor.Load() { return errInjected }
    return f.Store.(dw.Doorbell).WaitForWork(ctx, scope, kinds)
}
func (f *faultStore) Claim(ctx context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
    f.claims.Add(1)
    return f.Store.Claim(ctx, req)
}
// ... fs.failDoor.Store(true); m.Claim(ctx, "s") with a 1s timeout
```

```
Claim returned after 1.000326959s with err=context deadline exceeded, total store.Claim calls=1869176
```

**1.87 million `store.Claim` calls in one second**, on a pure in-memory backend with nothing to
claim. Against Redis or PostgreSQL this is not a CPU-spin curiosity, it's a network flood: every
one of those iterations is a would-be round trip, and every one also emits a `WarnContext` log
line, so the failure mode is CPU pegged on one core *and* the log sink saturated *and* the backend
hammered with claim RPCs, all while the process looks alive from the outside (`Claim` eventually
returns `context deadline exceeded`, which reads like ordinary backpressure, not a spin).

This is precisely the failure mode ADR-0033 was written to prevent — "a naive 'sleep and retry'
implementation becomes a busy-loop that burns CPU and hammers the storage backend" — and the
existing test, `TestClaimFallsBackWhenTheDoorbellFails` (`fault_test.go:188-212`), does not catch
it: it only asserts that the claim *eventually succeeds*, not that the fallback actually waits
between attempts. A spin loop finds the work just as fast as a well-behaved poll would, so the
test is green either way.

**Fix shape:** the `default` branch needs to actually wait out `wait` (or select on `wctx.Done()`)
before returning, the same as the non-doorbell branch below it (`claim.go:177-184`) already does.

---

## 2. MAJOR — `Typed[T]`'s "poison node" doesn't do what its doc comment says

`typed.go:52-57`:

```go
// A payload that fails to decode is a poison node: it would fail identically on
// every attempt, so it is failed immediately rather than retried, and the error
// is returned to the caller. Silently retrying a node that can never succeed is
// how a queue fills up with work nobody looks at.
```

The implementation (`typed.go:76-88`) calls `t.m.Nack(ctx, lease, decodeErr)` — the *ordinary*
failure path, subject to the scope's normal `RetryPolicy`. `Manager.Nack` is explicitly **not**
guaranteed terminal on the first call (`claim.go:220-232`; contrast with `Manager.Skip`, which is
documented as terminal on the first report specifically because it needs that guarantee,
`claim.go:234-238`). Under the library's own default `ScopeConfig` — `MaxAttempts: 3`
(`config.go:28`) — a payload that will never decode is retried twice more before finally
terminalizing, each time through the full claim → decode-fail → Nack → backoff cycle (default
backoff up to 5 minutes, `config.go:33`).

I verified this against a manager built with **no scope configuration at all** (the situation
every new user starts from):

```go
st := memory.New()
m, _ := dw.New(st)
_ = m.AddNode(ctx, "s", "bad", []byte(`{"depth":"not a number"}`))
tv := dw.NewTyped[job](m, "s")
for i := 0; i < 5; i++ {
    _, err := tv.TryClaim(ctx)
    n, _ := m.GetNode(ctx, "s", "bad")
    fmt.Printf("attempt %d: claimErr=%v status=%v attempt#=%d\n", i, err, n.Status, n.Attempt)
    ...
}
```

```
attempt 0: claimErr=...cannot unmarshal string into Go struct field job.depth... status=new attempt#=1
attempt 1: claimErr=dagworker: no ready node status=new attempt#=1
attempt 2: claimErr=dagworker: no ready node status=new attempt#=1
attempt 3: claimErr=dagworker: no ready node status=new attempt#=1
attempt 4: claimErr=dagworker: no ready node status=new attempt#=1
```

The node is `StatusNew` after the "immediate" failure — it went back into the retry queue, not to
`StatusError`. It will sit there through a full-jitter backoff and be reclaimed and re-decoded
(and fail identically) up to two more times before the scope's default policy finally lets it die.
That is not "failed immediately rather than retried"; it is exactly the retry-a-node-that-can-
never-succeed behavior the doc comment says this design exists to avoid.

The one test that exercises this path, `TestTypedRejectsAnUndecodablePayload`
(`typed_test.go:69-92`), passes only because it explicitly overrides the scope with
`dw.ScopeConfig{MaxAttempts: 1}` (`typed_test.go:74`) before claiming — which forces `Nack` to be
terminal on the first call for an unrelated reason (no attempts left), not because `decode()` did
anything to make it terminal. The test's own comment ("so it is failed at once rather than retried
forever") describes the doc's intent, not what happens under the library's actual defaults. Anyone
who copies this pattern without also copying that `MaxAttempts: 1` override — which is the natural
thing to do, since nothing in `Typed[T]`'s public surface suggests you need to — gets silent
retries of a payload that is guaranteed to keep failing.

**Fix shape:** `decode()` should report the failure through a path that is unconditionally
terminal, the way `Skip` is, rather than through `Nack`.

---

## 3. MAJOR — `Typed[T]`'s encode/decode errors escape the error taxonomy entirely

`errors.go:8-9` states the contract plainly: *"every error this package returns wraps exactly one
of [the sentinels]."* `typed.go` doesn't honor it. `AddNode` (line 41), `Ack` (line 96),
`GetNode` (line 117), and the shared `decode` helper (line 80) all wrap the raw
`encoding/json` error with `fmt.Errorf("...: %w", err)` and nothing else — no `ErrInvalidArgument`,
no dedicated sentinel, nothing a caller can `errors.Is` against.

Verified directly:

```go
_, err := tv.TryClaim(ctx) // undecodable payload, as in finding #2
for _, sentinel := range []error{dw.ErrInvalidArgument, dw.ErrInvalidConfig, dw.ErrUnsupported, dw.ErrNotFound} {
    fmt.Println(errors.Is(err, sentinel))
}
```
```
false
false
false
false
```

A caller building on `Typed[T]` who wants to branch on "this node's payload is corrupt, log and
move on" versus any other dagworker error has no supported way to do it — they'd have to reach
into `encoding/json`'s own error types (`*json.UnmarshalTypeError`, `*json.SyntaxError`), which is
exactly the internal-detail leakage the sentinel taxonomy exists to prevent everywhere else in
this package. This is a real gap in an otherwise well-kept taxonomy, not a style nit: it's the one
place the "every error wraps a sentinel" promise is broken, and it's broken in the convenience
layer most likely to be a new user's first contact with the library.

---

## 4. MAJOR — `AsWorker` / `WorkerID` is write-only; there is no way to ever read it back

`claim.go:30-35`:

```go
// AsWorker records who claimed the node, for observability. It has no bearing
// on correctness: the right to complete a node comes from the lease epoch, not
// from an identity anyone could assert.
func AsWorker(id string) ClaimOption { ... }
```

and `store.go:72-75`, `ClaimRequest.WorkerID`: *"identifies the claimant for observability."*

I traced `WorkerID` through the whole public surface and both non-trivial backends. It is written
once, on claim, and cleared on completion:

- `storage/memory/lease.go:180` (`r.worker = req.WorkerID`), cleared at lines 64, 235, 250.
- `storage/postgres/lease.go:64` (`UPDATE ... SET worker = $3 ...`), cleared at lines 189, 316, 325.

It is **never read back** anywhere — not into `Node`, not into `Inspection`, not into `Effect` or
`Event`. Grep the whole root package: `WorkerID` appears only at the two write sites
(`claim.go:34`, `store.go:75`). The public types simply have no field to put it in, so no backend
implementation *could* surface it without a breaking change to `Node`/`Inspection`, and both
backends I checked confirm they don't try.

This is the single most natural operational question a lease-based scheduler gets asked — "which
worker is holding node X" — and the API has a parameter and a doc comment promising exactly that,
with genuinely no path from claim to any read call that returns it. A caller who dutifully calls
`AsWorker("worker-7")` because the doc comment told them it's "for observability" gets nothing:
worse than not having the feature, because it looks like it works and there is no compiler or test
signal telling you otherwise.

**Fix shape (breaking):** add `ClaimedBy string` to `Node` and `Inspection`, or drop `AsWorker`
until there is somewhere for it to go. Shipping v1.0 with the current shape means this can only be
fixed later by adding a field to two structs that are otherwise clean, additive changes — not
technically breaking, but it cements a documented-but-inert option into the frozen v1 surface.

---

## 5. MAJOR — `ErrLeaseExpired` is a dead sentinel

`errors.go:50-52`:

```go
// ErrLeaseExpired means the lease deadline has passed. Distinct from
// [ErrLeaseMismatch]: the epoch still matches, but the grant is stale.
ErrLeaseExpired = errors.New("dagworker: lease expired")
```

`errors.go:11-13` says adding or removing a sentinel is a versioned, deliberate act — this is part
of the frozen taxonomy, not an internal detail. It is wired into both network adapters' error
tables (`adapters/grpc/errors.go:42`, `adapters/http/problem.go:76`), so it is treated as a real,
reachable outcome worth a gRPC code and an HTTP status. But grepping every backend
(`storage/memory`, `storage/redis`, `storage/postgres`) and the conformance suite
(`dagstoretest/`) for `ErrLeaseExpired` turns up nothing outside its own declaration and the two
adapter tables. It is never constructed, never returned, never exercised by a test.

The doc comment's claimed distinction from `ErrLeaseMismatch` ("the epoch still matches, but the
grant is stale") doesn't correspond to anything the reference backend actually checks: both
`Complete` and `Extend` in `storage/memory/lease.go` (lines 223, 286) compare only the epoch, never
the deadline independently of the epoch, so there is no code path in which the epoch still matches
*and* the deadline has passed that this design distinguishes from a plain epoch mismatch. A caller
who defensively branches on `errors.Is(err, ErrLeaseExpired)` per this doc comment is writing dead
code, forever, against every shipped backend.

**Fix shape:** either implement the distinction the doc comment promises (check the deadline
independently in `Complete`/`Extend` before falling back to the epoch check), or remove the
sentinel before it's frozen into 1.0 alongside two adapters that already reference it.

---

## 6. MAJOR — the "normative" contract disagrees with the shipped API in several concrete, checkable ways

The task brief for this review points at `docs/spec/01-contract.md` as normative and asks whether
the code does what it says. In several places, checkable by grep alone, it does not — and neither
direction has been reconciled by an ADR:

- **A whole event kind is missing from the contract's own events table.** `docs/spec/01-contract.md`
  §7.1 (lines 323-328) lists exactly two event kinds:
  ```
  EventTransition  a node's public Status changed. ...
  EventReady       a node became claimable. ...
  ```
  `EventCreated` (`event.go:13-22`) — a first-class member of the `EventKind` enum, with its own
  non-trivial semantics (a node born behind an already-failed predecessor starts *terminal*, not
  `StatusNew`) and its own required emission from `AddNodes` — is absent from this "normative"
  section entirely. A third-party implementer reading only the contract, as the review brief
  suggests trying, would not know this event kind exists, let alone that every node-creating write
  must produce it.

- **A promised error type doesn't exist.** §1.1 (line 35): *"Violations return
  `ErrInvalidIdentifier` wrapping the offending field name."* There is no `ErrInvalidIdentifier`
  anywhere in the codebase (`grep -rn ErrInvalidIdentifier` is empty). The actual mechanism is
  `*InvalidArgumentError` wrapping `ErrInvalidArgument` (`errors.go:118-134`), which is a perfectly
  good design — it's just not the one the contract documents.

- **Two capability facets in the contract's storage-port section don't exist; two facets the code
  has aren't in it.** §12 (lines 448-449): *"Optional facets, discovered by type assertion...:
  `Lister`, `DurableEventStream`, `ConditionalDeleter`, `BatchClaim`."* The shipped `Store`
  (`store.go:296-413`) has `Lister`, `DurableEventStream`, `Doorbell`, and `Collector`. There is no
  `ConditionalDeleter` or `BatchClaim` type anywhere in the module. `ADR-0016` (lines 97-104)
  designed both in detail, with their own capability bits (`CapConditionalDelete`,
  `CapBatchClaim`); `ADR-0042` (which lists ADR-0016 among the records it amends) documents eight
  other implementation deviations in careful detail but never mentions this one — the disappearance
  of two designed facets and the appearance of two undesigned ones is simply not written down
  anywhere. For a project whose whole discipline is "write the deviation down, don't leave it in a
  comment nobody reads" (ADR-0042's own words), this is the one architecturally significant change
  to the storage port's shape that fell through.

- **`WithPollInterval` moved from a `ClaimOption` to a `Manager`-level `Option` without the ADR
  being updated.** `docs/adr/0033-the-blocking-claim-wakeup-protocol.md:99-100` and again at
  line 197: *"`WithPollInterval` is a `ClaimOption`, not a `Manager`-level `Option` — different
  scopes served by the same `Manager` may reasonably want different poll floors."* The shipped
  function (`config.go:268-274`) is `func WithPollInterval(d time.Duration) Option` — a
  `Manager`-level option, exactly the shape the ADR argues against. The consequence the ADR
  predicted is real: one `Manager` serving both a low-traffic and a hot scope cannot give them
  different poll floors; there is one `pollInterval` for the whole `Manager` (`config.go:203-209`).

None of these are hard to fix individually, but each one means a reader who trusts the label
"normative" — which is exactly what this review's instructions told me to do — gets a wrong
answer. Given how unusually rigorous this project's ADR discipline is everywhere else (down to
recording that Redis durations round to the millisecond), this is a maintenance gap, not a
one-off slip: nothing currently forces the contract to be re-checked against the `Store` interface
when the interface changes.

---

## 7. MINOR — `Store.Scopes()` has no pagination, and the background loop is O(scope count)

`store.go:287-290`:

```go
// Scopes returns the scopes the store knows about. It exists so the
// background sweeper and the retention collector can find work without the
// caller enumerating scopes for them.
Scopes(ctx context.Context) ([]Scope, error)
```

No cursor, no limit — the signature can only ever return everything in one call. `Manager.maintain`
(`manager.go:403-429`) calls this every `SweepInterval` (default 5s) and then, in a single
goroutine, sequentially calls `Sweep` and `ScopeConfig`/`CollectTerminal` on **every scope
returned**:

```go
scopes, err := m.store.Scopes(ctx)
...
for _, scope := range scopes {
    ...
    m.sweepScope(ctx, scope)
    m.collectScope(ctx, scope)
}
```

`config.go:8-11` explicitly names "many small, short-lived scopes" as one of the two workload
shapes this library is designed to serve well, and `README.md:66` headlines *"Nothing is O(n)."*
But maintenance here is O(number of scopes) per cycle, single-threaded, with at least one and up
to two backend round trips per scope. A deployment that actually follows the library's own advice
— thousands of short-lived per-job scopes — turns every 5-second maintenance tick into thousands
of sequential round trips against Redis or PostgreSQL. This doesn't corrupt anything (sweeping is
also done inline on claim, so correctness never depends on this loop, per the doc at
`manager.go:397-402`), but it does mean the promised "dead worker noticed promptly" property
degrades as scope count grows, silently, with nothing to observe except a growing gap between
`SweepInterval` and actual reclaim latency.

Because `Scopes()` is part of the mandatory `Store` interface with this exact signature, fixing it
later (adding a cursor, or splitting "list scopes needing maintenance" from "list all scopes") is a
breaking change to every backend and every `Manager` version at once. Worth deciding before 1.0,
not after.

---

## 8. Naming and small-footgun findings

- **`Claim`'s batch form breaks the package's own pluralization convention.** Every other
  multi-item operation in this API is `Verb` / `Verbs`: `AddNode` / `AddNodes` (`manager.go:170,
  187`), `AddEdge` / `AddEdges` (`manager.go:204, 209`), `RemoveEdge` / `RemoveEdges`
  (`manager.go:222, 227`). Claiming breaks the pattern: `Claim` / `TryClaim` / `ClaimBatch`
  (`claim.go:67, 85, 118`). A developer who has just learned "the batch form is the plural" from
  the first three method pairs has no reason to guess `ClaimBatch` rather than `Claims` or
  `ClaimNodes`. Minor on its own, but naming is exactly the kind of thing that's free to fix now
  and impossible to fix for free after 1.0.

- **`WithRetry` and `WithMaxAttempts` compose unsafely.** `WithRetry` replaces the whole
  `RetryPolicy` struct (`options_node.go:56-62`: `s.Retry = RetryPolicy{...}`), while
  `WithMaxAttempts` mutates a single field (`options_node.go:64-68`: `s.Retry.MaxAttempts = n`).
  `AddNode(..., WithMaxAttempts(5), WithRetry(0, base, max))` silently discards the `5` — the field
  that was "inherit" in the `WithRetry(0, ...)` call wins because it runs last and overwrites the
  whole struct, not because either option documents that it beats the other. Contrast with
  `WithLabels` (`options_node.go:45-54`), which deliberately merges into any existing map for
  exactly this reason. `WithRetry` should merge non-zero fields the same way, or its doc comment
  should say plainly that combining it with the single-field setters is unsupported.

- **`SubscribeOptions.Overflow *OverflowPolicy`** (`event.go:168`) uses a pointer to a `uint8` enum
  purely to distinguish "not set" from "explicitly `OverflowDropOldest`" — defensible, since the
  manager-wide default might be `OverflowCloseSlow` and a caller needs to be able to override back
  down to `OverflowDropOldest` per-subscription, but it's the one pointer-to-enum in an API that
  otherwise (correctly, per `node.go:209-212`'s stated policy) avoids optional-pointer fields. A
  three-value enum (`OverflowUnset`/`OverflowDropOldest`/`OverflowCloseSlow`) would give the same
  expressiveness without a nil-dereference-shaped field in a public struct literal.

- **`ClaimBatch(ctx, scope, n, ...)` silently coerces `n <= 0` to `1`** (`claim.go:89`, via
  `max(n, 1)`), rather than treating a non-positive batch size as a caller error the way
  `WithLeaseTimeout`'s negative check does (`claim.go:58-60`). An accidentally-zero `n` (an
  uninitialized variable, a config value that failed to load) gets a working-but-wrong single-item
  claim instead of a loud `ErrInvalidArgument`.

---

## 9. `Typed[T]`: worth its weight, but incompletely typed and frozen with a gap

`Typed[T]` (`typed.go`) is a reasonable, honestly-scoped convenience: it's explicit that it's a
JSON wrapper over the byte-oriented primitive, not a replacement for it (`typed.go:9-17`), which
is the right call given the network adapters need to speak to non-Go processes. Two things about
its current shape will be awkward once frozen:

- **No batch operations.** `Manager` has `AddNodes` and `ClaimBatch`; `Typed[T]` has neither — only
  the single-node `AddNode`/`Claim`/`TryClaim`. A caller who wants typed batch insertion or typed
  batch claiming has to drop back to the raw `Manager` and hand-decode, which defeats the point of
  reaching for `Typed[T]` in the first place for exactly the workloads (many small nodes) where
  batching matters most.

- **`Ack`'s result is `any`, not a second type parameter.** `AddNode`, `Claim`, `TryClaim`, and
  `GetNode` are all strongly typed in the payload type `T`; `Ack(ctx, lease, result any)`
  (`typed.go:91`) is not. The type safety `Typed[T]` exists to provide covers what a worker
  consumes but not what it produces. That may be a deliberate scope decision (payload and result
  are conceptually different shapes), but if it isn't, the fix is `Typed[T, R]` — and adding a type
  parameter to an already-shipped generic type is a breaking change for every existing caller, not
  an additive one. Worth deciding deliberately now rather than discovering the asymmetry is
  unwanted after 1.0.

---

## 10. Is `Store` implementable by a third party without reading the memory backend?

Per the assignment: read only `store.go` and `docs/spec/01-contract.md`, then list what's still
unknown. Doing exactly that, here is what a diligent third-party implementer would not know going
in:

- **Whether `EventCreated` must ever be produced, and by what.** As covered in finding #6, the
  contract's events section doesn't mention this `EventKind` at all. The only way to learn it
  exists is to read `event.go` directly (not `store.go`, and not the contract) — which the exercise
  as posed doesn't include.

- **What effects, and how many, each Effect-returning method should emit.** `store.go`'s method
  docs for `AddNodes`, `AddEdges`, `RemoveEdges`, `RemoveNode`, and `Cancel` (lines 234, 239, 244,
  248, 254) each return a bare `([]Effect, error)` and describe the *status* change but not the
  *event* mapping: does inserting a node with zero unresolved deps emit one `Effect` (`EventCreated`,
  carrying its already-`New` status) or two (`EventCreated` then `EventReady`)? The reference
  backend emits two (`storage/memory/graph.go:213-221`: `EventCreated` for every fresh node, then a
  separate `settle` pass that appends `EventReady` for anything immediately claimable) — but nothing
  in `store.go` or the contract says a conforming backend must do the same rather than folding
  readiness into the creation event. Contrast this with `ClaimResult.Effects` and
  `CompleteResult.Effects` (`store.go:82-84`, `112-115`), which *are* specified precisely enough to
  implement from prose alone. The graph-mutation methods are not held to the same standard.

- **How `ClaimRequest.Kinds` with more than one entry is supposed to interact with priority
  ordering.** The contract's §4.2 describes selection as "eligible node from the `(scope, kind)`
  ready-set, honouring priority then FIFO" as if kind were singular. Neither document says how a
  claim naming two or more kinds should pick among their separate ready-sets — combined priority
  order across kinds, or exhaust one kind's ready-set before touching the next. This is a real
  behavioral degree of freedom a from-scratch implementation could resolve differently from the
  reference backend and still pass a shallow reading of both documents.

- **Priority-then-FIFO ordering isn't mentioned in `store.go` at all** — only in the contract
  (§4.2). This is fine under the letter of the exercise (both documents together do specify it),
  but `store.go` doesn't so much as point at `docs/spec/01-contract.md` from its own package doc or
  the `Store` interface's doc comment; a real third party discovering this package via
  `pkg.go.dev` (where `store.go`'s comments are all they'll ever see) has no signposted way to
  learn the contract document exists at all. The `Store` interface's own top-level doc
  (`store.go:171-213`, the "Atomicity / Clocks / Fencing / Conformance" sections) is genuinely
  good and should be the place that link lives.

On the positive side: the atomicity invariants (`store.go:177-193`), the clock-ownership rule
(`store.go:195-201`), and the fencing requirement (`store.go:203-208`) are stated precisely enough
to implement against without ever opening `storage/memory`, which is the hard 20% of this
exercise. `ScopeConfig.Resolved()` (`config.go:117-155`) and `Backoff()` (`backoff.go:18-45`) are
exported specifically so a third-party backend doesn't have to reimplement fallback resolution or
retry timing by hand and risk drifting from the in-memory backend's numbers — that's a real,
deliberate accommodation for exactly the audience this section is asking about, and it's the right
call.

---

## What's actually good here

Worth being specific about, since the brief asks for earned praise, not just problems:

- **The `Status`/`Reason`/`Phase` split** (`status.go`) is the rare case of a taxonomy that
  actually resists the temptation to add a fifth status for every new terminal cause. The
  `Reason`-by-`Status`/`Attempt` table (`status.go:78-84`) is normative, matches the contract
  (§2.2) exactly, and is enforced by `MarshalText`/`UnmarshalText` round-tripping through a closed
  switch rather than relying on the integer value staying stable.
- **Context discipline is real, not just claimed.** No exported type in the reviewed files stores
  a `context.Context` in a struct field; `Manager`'s own background context is deliberately
  reduced to just the `cancel` function (`manager.go:33-40`) with a comment explaining exactly why.
  `Close` genuinely waits for every started goroutine before returning (`manager.go:113-142`).
- **The lint configuration is unusually serious** (`.golangci.yml`) — an explicit allowlist rather
  than a denylist, `exhaustive` on the status/reason switches with
  `default-signifies-exhaustive: false` (the setting that actually makes it useful),
  `paralleltest`/`tparallel`/`thelper` for test discipline, and a `depguard` rule enforcing the
  "core module has no network import" claim from the README at lint time, not just at review time.
  It reports zero issues on this package, which for a ruleset this strict is a genuine signal.
- **`Backoff`'s full-jitter implementation** (`backoff.go:18-45`) is careful about the one thing
  that's easy to get wrong — shifting rather than multiplying so a large attempt count can't
  overflow into a negative duration — and is exported precisely so every backend is forced to
  reproduce the identical schedule rather than inventing its own.

---

## Verdict

The public API's shape — Manager as a thin, validating facade; Store owning every atomic
operation; a closed four-value Status with reasons carrying the detail; options that can't be
constructed outside the package — is coherent and a competent Go developer would guess right about
most of it most of the time. The lint discipline and the context/shutdown hygiene are genuinely
better than average. But three of the findings above are not style complaints: a doorbell error
turning `Claim` into a 1.87-million-calls-per-second busy loop is a production incident waiting for
a Redis blip, `Typed[T]`'s flagship "poison node" guarantee is false under the library's own
default configuration and the one test covering it hides that with a non-default override, and a
documented observability field (`AsWorker`) has no read path anywhere in the shipped types. Add a
dead sentinel wired into two adapters and a "normative" contract that has drifted from the code it
governs in checkable ways, and the honest read is: the design is sound, but this is not yet the
kind of finished that survives contact with a real fleet, and at least two of these (the busy loop,
the poison-node retry) should block a 1.0 tag until fixed rather than be filed for a point release.
