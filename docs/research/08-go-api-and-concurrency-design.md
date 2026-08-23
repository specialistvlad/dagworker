# Go API Design and Concurrency Patterns for dag-worker-go

Scope: how to shape the **public Go API** of an embeddable DAG-of-work-items manager, and how
to build its **concurrency machinery** (event fan-out, goroutine lifecycle, in-memory storage
primitives, deterministic tests, observability) so that it survives being embedded in someone
else's process, called from many goroutines, and maintained for years under the Go 1
compatibility promise. Every recommendation below is checked against the canonical Go API-design
literature and against three production systems that already solved "many producers, many slow
subscribers, must not deadlock, must survive reconnects": Kubernetes `client-go`
(informers/workqueue), the NATS Go client, and etcd's watch API.

---

## 1. Sources consulted, at a glance

| Source | What it's authoritative on | URL |
|---|---|---|
| Rob Pike, "Self-referential functions and the design of options" (2014) | Origin of the functional-options pattern | [commandcenter.blogspot.com](https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html) |
| Dave Cheney, "Functional options for friendly APIs" (2014) | The now-standard `func(*T) error`-free options shape | [dave.cheney.net](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) |
| Dave Cheney, "Practical Go" | Package design, "accept interfaces, return structs," error handling, concurrency ownership | [dave.cheney.net/practical-go](https://dave.cheney.net/practical-go/presentations/qcon-china.html) |
| Go Code Review Comments (wiki) | Interfaces belong to the consumer, error string style, goroutine lifetimes | [github.com/golang/go/wiki/CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments) |
| Go Proverbs (Rob Pike, Gopherfest 2015) | Design aphorisms used as tie-breakers throughout this doc | [go-proverbs.github.io](https://go-proverbs.github.io/) |
| Effective Go | Canonical interface/channel/error idiom | [go.dev/doc/effective_go](https://go.dev/doc/effective_go) |
| Uber Go Style Guide | Concrete, opinionated rules: options-as-interface, channel size ≤ 1, no goroutines in `init`, no embedded mutexes | [github.com/uber-go/guide](https://github.com/uber-go/guide/blob/master/style.md) |
| Google Go Style Guide — Style Decisions & Best Practices | "Accept interfaces, return concrete types," context rules, options-struct vs. variadic-options decision matrix | [google.github.io/styleguide/go/decisions](https://google.github.io/styleguide/go/decisions.html), [best-practices](https://google.github.io/styleguide/go/best-practices.html) |
| Go blog, "Working with Errors in Go 1.13" | `errors.Is`/`As`, `%w`, "wrapping an error makes that error part of your API" | [go.dev/blog/go1.13-errors](https://go.dev/blog/go1.13-errors) |
| `errors.Join` (Go 1.20) | Multi-error aggregation semantics | [pkg.go.dev/errors#Join](https://pkg.go.dev/errors#Join) |
| `context` package + Go blog on context-in-structs | Cancellation propagation rules, `AfterFunc`, `Cause` | [pkg.go.dev/context](https://pkg.go.dev/context), [go.dev/blog/context-and-structs](https://go.dev/blog/context-and-structs) |
| `golang.org/x/sync/errgroup` | Goroutine-group error propagation and cancellation | [pkg.go.dev/.../errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup) |
| `sync.WaitGroup.Go` (Go 1.25) | New leak-resistant goroutine-launch primitive | [pkg.go.dev/sync#WaitGroup.Go](https://pkg.go.dev/sync#WaitGroup.Go) |
| `testing/synctest` (Go 1.24 experimental → 1.25 GA) | Fake-time bubbles for deterministic timeout tests | [go.dev/blog/synctest](https://go.dev/blog/synctest) |
| Kubernetes `client-go` workqueue + informers | The reference architecture for decoupling event delivery from slow processing | [client-go/util/workqueue](https://pkg.go.dev/k8s.io/client-go/util/workqueue), [queue.go](https://github.com/kubernetes/client-go/blob/master/util/workqueue/queue.go), [default_rate_limiters.go](https://github.com/kubernetes/client-go/blob/master/util/workqueue/default_rate_limiters.go) |
| NATS Go client slow-consumer handling | Bounded pending buffers + explicit `ErrSlowConsumer`, never silently unbounded | [docs.nats.io/slow_consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers), [nats.go source](https://github.com/nats-io/nats.go/blob/main/nats.go) |
| etcd watch API | Resume-by-revision semantics, compaction errors, per-stream buffering | [etcd.io/docs/v3.6/learning/api](https://etcd.io/docs/v3.6/learning/api/), [watch.go](https://github.com/etcd-io/etcd/blob/main/client/v3/watch.go) |
| Go blog, "When To Use Generics" | Containers/algorithms vs. behavior — the generics/interfaces boundary | [go.dev/blog/when-generics](https://go.dev/blog/when-generics) |
| Go blog, "structured logging with slog" | Frontend/backend split, no logger-in-context | [go.dev/blog/slog](https://go.dev/blog/slog) |
| OpenTelemetry Go docs | "Libraries depend on the API only, never the SDK" | [opentelemetry.io/.../go/instrumentation](https://opentelemetry.io/docs/languages/go/instrumentation/) |
| Prometheus / OTel naming conventions | Base units, `_total` suffix, UCUM units | [prometheus.io/docs/practices/naming](https://prometheus.io/docs/practices/naming/), [OTel naming](https://opentelemetry.io/docs/specs/semconv/general/naming/) |
| Go 1 compatibility promise & module versioning | What "v1" commits you to, and how v2+ is spelled | [go.dev/doc/go1compat](https://go.dev/doc/go1compat), [go.dev/blog/v2-go-modules](https://go.dev/blog/v2-go-modules), [research.swtch.com/vgo-import](https://research.swtch.com/vgo-import) |

---

## 2. The Go API design canon, applied to this library

### 2.1 Go Proverbs as load-bearing constraints

Rob Pike's [Go Proverbs](https://go-proverbs.github.io/) are not decoration; four of them directly
decide contested points in this design:

- **"The bigger the interface, the weaker the abstraction."** The pluggable-storage requirement
  (in-memory / Redis / Memcached / PostgreSQL) is a trap for a single fat `Storage` interface
  with forty methods, because every backend must then implement all forty, and testing requires
  forty-method fakes. Split it into small interfaces the way `io.Reader`/`io.Writer` are split —
  `NodeStore`, `EdgeStore`, `LeaseStore`, `EventLog` — and let a backend's top-level type satisfy
  a composed `Store` interface built from them. This also lets a backend that only supports part
  of the feature set (e.g., an event log that doesn't support historical replay) be typed
  precisely instead of lying about a method it can't implement well.
- **"Channels orchestrate; mutexes serialize."** The in-memory backend's node/edge maps are
  *data* — protect them with sharded mutexes (§10), not channels. The event fan-out to
  subscribers and the ready-queue hand-off to workers are *coordination* — use channels there,
  not a shared mutable "pending list" guarded by a lock that every subscriber goroutine polls.
- **"A little copying is better than a little dependency."** The `Clock` abstraction (§11) should
  be a two-or-three-method interface *owned by this module*, not a required dependency on
  `benbjohnson/clock` or `clockwork` — even though the design sketch in §11.2 is lifted from both.
  A one-line adapter lets users plug either library in if they already depend on one.
- **"Design the architecture, name the components, document the details."** This is the
  justification for keeping the *public* status vocabulary to `New` / `InProgress` / `Success` /
  `Error` while an internal state machine (queued-but-blocked-on-deps, leased, lease-expired,
  retry-scheduled, cancelled) does the real work — the public names are the architecture as seen
  from outside the library; the rest is detail that must not leak into the API surface or every
  future internal refactor becomes a breaking change.

### 2.2 Effective Go and Code Review Comments — the baseline

[Effective Go](https://go.dev/doc/effective_go) supplies the idioms this design builds on:
single-method interfaces named by the `-er` convention (`Reader`, `Closer`), returning
`error` values instead of raising exceptions, and using goroutines/channels instead of
callback registries for concurrent coordination. [Go Code Review
Comments](https://github.com/golang/go/wiki/CodeReviewComments) (now substantially folded into
the Google style guide, see below) adds two rules with direct bearing here:

- *Interfaces belong in the package that consumes values of that type, not the package that
  produces them.* Consequence: `dagworker` should **not** export a `Node` interface for hosts to
  implement. It should export concrete `Node`/`Event`/`Claim` structs, and let hosts define their
  *own* narrow interfaces (e.g., a `Worker` interface with one `Process(ctx, Claim) Result`
  method) that they pass to a `Run` helper — the consumer of that interface is the host's code,
  so the host owns the interface, mirroring the split between `io.Reader` (defined in `io`,
  consumed everywhere) and, e.g., a driver-specific `Scanner`.
- *Goroutine lifetimes must be stated, not implied.* Every internal goroutine this library starts
  (dispatcher, lease reaper, per-subscription pump) must have a documented owner and a
  documented shutdown signal — this is elevated to a hard contract in §9.

### 2.3 Dave Cheney's Practical Go

[Practical Go](https://dave.cheney.net/practical-go/presentations/qcon-china.html) contributes
three decisions directly:

1. **"Accept interfaces, return structs."** `Manager.Claim` returns a concrete `*Claim`, not a
   `Claimer` interface; `New` accepts a `Store` interface. This gives callers full access to
   whatever concrete fields/methods a `*Claim` carries instead of an artificially narrowed view.
2. **"Leave concurrency to the caller."** The library must not spin up a fixed worker pool that
   calls user code — that decision (how many workers, which goroutine model, whether to use a
   pool library) belongs to the host program, per the brief's own framing ("the host program owns
   the actual workers"). The library's job stops at handing out claims and receiving acks;
   Cheney's "do the work yourself, or delegate — never assume you should spawn a goroutine on the
   caller's behalf" argument is exactly why `Claim`/`Subscribe` are pull/blocking calls, not
   callback-registration calls that silently start goroutines.
3. **"Never start a goroutine without knowing how it will stop."** Directly reused in §9's
   goroutine-ownership contract.

### 2.4 Uber Go Style Guide

The [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) is the most
prescriptive of the three and settles several implementation-level questions:

- **Functional options as an interface, not a raw `func(*T)`.** Uber's canonical shape wraps the
  function in an unexported `apply(*options)` method on an exported `Option` interface (see the
  exact code in §4). This is stricter than Pike/Cheney's raw closures and buys forward
  compatibility: the concrete option type stays unexported, so users can never construct one that
  doesn't call `apply`, and the library is free to add fields to the private `options` struct
  without a signature change.
- **Channel size ≤ 1, everything else needs written justification.** *"Channels should usually
  have a size of one or be unbuffered. Any other size must be subject to a high level of
  scrutiny."* This directly bears on the subscriber-buffer design in §8: the buffer size is a
  deliberately-justified, user-configurable exception to this rule, not a magic constant chosen
  by feel.
- **No goroutines in `init()`, and every spawned goroutine gets a `stop`/`done` pair or a
  `WaitGroup`.** Feeds directly into §9.
- **Never embed `sync.Mutex`** (even in unexported structs) because the `Lock`/`Unlock` methods
  become part of the promoted method set and an external caller holding a `*Manager` could call
  `mgr.Lock()` by accident if `Manager` ever became embeddable. Every mutex in this design is a
  named field (`mu sync.RWMutex`), never embedded.
- **Error variables/types for anything the caller must be able to detect programmatically**, versus
  plain `fmt.Errorf`/`errors.New` for messages nobody branches on. This is the backbone of §6.

### 2.5 Google Go Style Guide

The [Google Go Style Guide](https://google.github.io/styleguide/go/decisions.html) — split into
core style, [Style Decisions](https://google.github.io/styleguide/go/decisions.html), and [Best
Practices](https://google.github.io/styleguide/go/best-practices.html) — refines the
options-pattern decision with an explicit two-column decision table absent from Pike/Cheney:

> Use an **options struct** when: all or most callers must set most fields, options are shared
> between call sites, or the option set is large and mostly non-optional. Use **variadic
> functional options** when: most callers pass zero options, individual options are used
> infrequently, and the option set may grow indefinitely. — paraphrased from [Best
> Practices](https://google.github.io/styleguide/go/best-practices.html)

It also states a rule this design leans on hard: **"Contexts are never included in option
structs"** ([decisions#contexts](https://google.github.io/styleguide/go/decisions.html)) — a
`context.Context` is per-call, not per-construction, so it can never legally appear as a
`With...` option on `New`; it only ever appears as the first parameter of a method. And on
interfaces: *"Functions should take interfaces as arguments but return concrete types"* — the
same rule as Cheney's, arrived at independently, which is a strong signal it's not a style
preference but a genuine Go idiom.

---

## 3. Versioning: the Go 1 promise, semantic import versioning, and this library's v0→v1 path

The [Go 1 compatibility
promise](https://go.dev/doc/go1compat) guarantees *source* compatibility, not binary
compatibility, for the lifetime of a major version: "programs written to the Go 1 specification
will continue to compile and run correctly, unchanged." It explicitly carves out unkeyed struct
literals, `unsafe`, dot-imports, and behavior programs "incorrectly depend on" as fair game for
breakage. The module system's [import compatibility
rule](https://go.dev/blog/versioning-proposal) generalizes this per-package: *"If an old package
and a new package have the same import path, the new package must be backwards compatible with
the old package."* [Semantic import
versioning](https://research.swtch.com/vgo-import) is the mechanism that reconciles this rule
with SemVer's allowance for breaking `v2+` releases: a v2 (or later) major version must live at a
different import path (`.../v2`), so that a large program can depend on both `v1` and `v2` of the
same module transitively without a diamond conflict, and can upgrade call sites incrementally
([go.dev/blog/v2-go-modules](https://go.dev/blog/v2-go-modules)).

Concretely for dag-worker-go:

1. **Start at v0** (module path `github.com/<org>/dag-worker-go`, no version suffix, tags
   `v0.x.y`). Go's own tooling treats v0 as "anything may change release to release" — this is the
   license to get the `Store` interface, the event-envelope shape, and the error taxonomy wrong
   twice before committing.
2. **Freeze the wire-visible and store-visible shapes before tagging v1.** Because storage is
   pluggable and multiple *instances* share it, the on-storage encoding of a node record and the
   event envelope are effectively a second API surface — a schema migration is far more expensive
   than a Go API break, because it must be applied to every running process across a fleet
   simultaneously. Version the on-storage schema explicitly (a `schema_version` field per scope)
   from v0.1 on, independent of the Go module version, so that it can evolve without forcing a
   major Go version bump.
3. **Adopt keyed struct literals as a public-API rule from day one** (`Node{ID: id, Scope:
   scope}`, never `Node{id, scope}`) precisely because Go 1's own compatibility promise excludes
   unkeyed literals — this lets the library add fields to `Node`, `Event`, and `Claim` post-v1
   without it counting as a breaking change for any caller who followed the convention (which
   should be enforced by a linter rule, `fieldalignment`/`composites` style, not just documented).
4. **Reserve `/v2` for the day the `Store` interface itself must change shape** — e.g., if a future
   backend requires a fundamentally different transaction model. Until then, growing the `Store`
   interface is done by *adding* an optional, capability-detected sub-interface (`type
   BatchStore interface { Store; AddNodesBatch(...) }`) that backends can implement and the
   manager type-asserts for, rather than changing the required method set — the same pattern the
   standard library uses for `io.ReaderFrom`, `io.WriterTo`, and `http.Pusher`.

---

## 4. Constructing the Manager: functional options vs. a Config struct

### 4.1 The two camps, precisely

Rob Pike's original 2014 post, ["Self-referential functions and the design of
options"](https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html),
frames the problem as: how do you let a package export an extensible set of named,
independently-documented settings without either (a) an constructor with a dozen positional
arguments that breaks every time one is added, or (b) forcing every caller to build and zero out
a settings struct just to change one field? His answer is a function that is *itself* the option
and, when invoked, returns another option that would restore the previous value:

```go
type option func(*Foo) option

func Verbosity(v int) option {
    return func(f *Foo) option {
        prev := f.verbosity
        f.verbosity = v
        return Verbosity(prev)
    }
}
```

This buys `prev := foo.Option(Verbosity(3)); defer foo.Option(prev)` — genuinely elegant for a
long-lived, mutable, in-process object like his `log.Logger`, but Pike himself flags the cost: the
previous value is now hidden inside a closure, so extracting the raw value back out "needs a
little more magic." [Dave Cheney's 2014 talk](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis)
strips the self-referential/undo half out and keeps only the "options are functions applied at
construction time" half, because most Go types (his running example is an HTTP `Server`) are
configured once at construction, not tweaked and rolled back at runtime:

```go
func NewServer(addr string, options ...func(*Server)) *Server {
    srv := &Server{addr: addr, timeout: defaultTimeout}
    for _, opt := range options {
        opt(srv)
    }
    return srv
}

func Timeout(d time.Duration) func(*Server) {
    return func(s *Server) { s.timeout = d }
}
```

Cheney's stated wins over a config struct: no caller is forced to construct and zero out an empty
struct to get defaults; there is no ambiguity about whether a zero-valued field means "unset, use
default" or "explicitly set to zero" (a config struct's zero value is silently overloaded);
callers cannot hold a reference into the server's internal config and mutate it after
construction, because the closures execute once, during `New`, and their captured values are
copied into private fields.

**The counterargument, taken seriously.** A plain `Config` struct is simpler to read (`go doc`
shows every field and its type in one place instead of scattered `With*` functions), trivially
serializable from JSON/YAML/env for a host that wants to expose manager settings as its own
config file, and — most importantly for testing — trivially comparable and constructible in table
tests: `cfg := Config{DefaultLeaseTimeout: time.Second}; m := New(store, cfg)` needs no helper
functions, whereas asserting *which* options were applied to a functional-options constructor
requires either exposing an unexported `options` struct for tests in the same package or
re-deriving state through the manager's public getters. The Google Style Guide's decision table
(§2.5) formalizes exactly this tradeoff: an options struct wins when *most* callers must set
*most* fields; variadic options win when most callers pass none.

### 4.2 Decision for dag-worker-go

`New(store Store, opts ...Option)` — functional options, Uber's `Option`-interface variant, not
raw closures, and not a config struct. Reasoning, weighed against §4.1:

- The overwhelming majority of embedders will call `dagworker.New(store)` with zero options and
  get sane defaults (in-process default lease timeout, no-op logger, no-op meter/tracer
  providers, default subscriber buffer size) — this is exactly Google's "most callers pass no
  options" trigger for the variadic form, not the struct form.
  The **required** parameter (`store Store`) is a positional argument, never an option, per Cheney's
  "don't make the common case pay for the rare case" and per Google's explicit rule that a
  required dependency never belongs in an options struct or a `With...` option.
- Options here are genuinely add-over-time and independently documentable
  (`WithDefaultLeaseTimeout`, `WithClock`, `WithLogger`, `WithMeterProvider`,
  `WithTracerProvider`, `WithSubscriberBufferSize`, `WithOverflowPolicy`, `WithScopeShardCount` for
  the in-memory backend) — precisely the shape functional options were designed for, and precisely
  the shape that would make a `Config` struct's zero-value ambiguity a real bug magnet (is
  `Config{}.SubscriberBufferSize == 0` "unbuffered channel" or "use the library default of 256"?
  functional options sidestep the question entirely by only running the default-assignment code
  the library controls).
- We reject Pike's self-referential/undo form outright: nothing about a `Manager`'s configuration
  is meant to be flipped at runtime and rolled back — lease timeout defaults, clock, and telemetry
  providers are construction-time-only, so the added complexity of returning a restorer option
  buys nothing here.
- We accept Cheney's opaque-closure downside as the price of forward compatibility and follow
  Uber's variant to soften the specific testability complaint from §4.1: wrap the function in an
  unexported `apply` method on an exported `Option` interface so unit tests inside the module can
  still construct an `options` struct directly and call `.apply()` on each option to assert
  outcomes, without exposing a public config struct that becomes part of the compatibility surface
  in its own right (a struct's fields are individually load-bearing under the Go 1 promise;
  functions are not).

```go
package dagworker

import (
    "log/slog"
    "time"
)

// Option configures a Manager at construction time. The concrete type behind
// Option is unexported: callers can only obtain one from a With* function in
// this package, which keeps the option set open for extension without ever
// changing New's signature — see https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
// and the Option-as-interface variant in the Uber Go Style Guide:
// https://github.com/uber-go/guide/blob/master/style.md#functional-options
type Option interface {
    apply(*config)
}

type config struct {
    defaultLeaseTimeout time.Duration
    clock               Clock
    logger              *slog.Logger
    meterProvider       MeterProvider // narrow subset of otel/metric.MeterProvider, see §12
    tracerProvider      TracerProvider
    subscriberBuffer    int
    overflow            OverflowPolicy
    shardCount          int
}

func defaultConfig() config {
    return config{
        defaultLeaseTimeout: 30 * time.Second,
        clock:               realClock{},
        logger:              slog.New(discardHandler{}),
        meterProvider:       noopMeterProvider{},
        tracerProvider:      noopTracerProvider{},
        subscriberBuffer:    256,
        overflow:            OverflowDropOldestAndMarkGap,
        shardCount:          64,
    }
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// WithDefaultLeaseTimeout sets the lease timeout used when a worker claims a
// node without specifying ClaimOption WithLeaseTimeout. It has no effect on
// leases already outstanding.
func WithDefaultLeaseTimeout(d time.Duration) Option {
    return optionFunc(func(c *config) { c.defaultLeaseTimeout = d })
}

// WithClock overrides the manager's time source. Intended for tests; see §11.
func WithClock(clk Clock) Option {
    return optionFunc(func(c *config) { c.clock = clk })
}

// WithLogger sets the *slog.Logger the manager writes structured diagnostic
// events to. The zero value of Manager logs nothing (see §12.1); passing nil
// here is equivalent to not calling WithLogger.
func WithLogger(l *slog.Logger) Option {
    return optionFunc(func(c *config) {
        if l != nil {
            c.logger = l
        }
    })
}

// WithSubscriberBufferSize sets the per-subscription channel buffer used by
// Subscribe (see §8). Must be > 0; New returns an error otherwise.
func WithSubscriberBufferSize(n int) Option {
    return optionFunc(func(c *config) { c.subscriberBuffer = n })
}

// New constructs a Manager backed by store. store is a required, positional
// dependency — never a functional option — per
// https://google.github.io/styleguide/go/decisions#contexts and the sibling
// rule that required collaborators are constructor arguments, not options.
func New(store Store, opts ...Option) (*Manager, error) {
    cfg := defaultConfig()
    for _, opt := range opts {
        opt.apply(&cfg)
    }
    if store == nil {
        return nil, fmt.Errorf("dagworker: New: %w", ErrNilStore)
    }
    if cfg.subscriberBuffer <= 0 {
        return nil, fmt.Errorf("dagworker: WithSubscriberBufferSize: %w", ErrInvalidConfig)
    }
    return newManager(store, cfg), nil
}
```

This mirrors gRPC-Go's own `DialOption`/`CallOption` types, which use exactly this
opaque-interface-plus-unexported-implementation shape at far larger scale — `grpc.DialOption` is
documented only as `interface { /* contains filtered or unexported methods */ }`, and every
`grpc.WithXxx` constructor is the sole legal source of one
([pkg.go.dev/google.golang.org/grpc#DialOption](https://pkg.go.dev/google.golang.org/grpc#DialOption)),
which is the strongest evidence this pattern scales to a library with dozens of options added
over a decade without a breaking change.

---

## 5. Context propagation rules

The `context` package documentation is unambiguous and this design follows it to the letter:
*"Do not store Contexts inside a struct type; instead, pass a Context explicitly to each function
that needs it. The Context should be the first parameter, typically named ctx"*
([pkg.go.dev/context](https://pkg.go.dev/context)). The [Go blog's dedicated post on this exact
question](https://go.dev/blog/context-and-structs) gives the reasoning this project should quote
in its own CONTRIBUTING doc: a context stored on a struct (a) obscures how long it's valid for,
(b) prevents any one method call from having its own deadline distinct from the object's, and (c)
makes it ambiguous which of an object's methods the context even applies to. **`Manager` therefore
never stores a `context.Context` field.** Every method that can block, do I/O against a storage
backend, or wait on a channel takes `ctx context.Context` as its first parameter:

```go
func (m *Manager) AddNode(ctx context.Context, scope Scope, id NodeID, payload []byte, deps ...NodeID) error
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)
func (m *Manager) Ack(ctx context.Context, lease Lease, result Result) error
func (m *Manager) Subscribe(ctx context.Context, opts SubscribeOptions) (*Subscription, error)
```

The one documented exception the Go blog itself allows — carrying a context on a struct only for
backward-compatible retrofits of a pre-existing API (`Client.Call()` keeps working,
`Client.CallContext(ctx)` is added alongside it) — does not apply to a greenfield v0 library and
is explicitly not adopted here.

**Cancelling in-flight claims.** A `Claim` call blocks (subject to a caller-supplied `ctx`) until a
node in the requested scope becomes ready or `ctx` is done. This needs cooperative cancellation
inside the storage layer, not just at the manager's own select statement, because a
network-backed store's "find and lease a ready node" operation is itself a blocking/polling RPC.
The pattern used throughout — for `Claim`, for `Subscribe`'s initial catch-up read, and for a
storage backend's internal retry loop — is `select` on both the operation's own completion channel
and `ctx.Done()`:

```go
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error) {
    cfg := m.claimDefaults(opts)
    for {
        claim, err := m.store.TryClaim(ctx, scope, cfg.leaseTimeout)
        switch {
        case err == nil:
            return claim, nil
        case errors.Is(err, ErrScopeEmpty):
            select {
            case <-m.readySignal(scope):
                continue // a node just became ready; retry immediately
            case <-ctx.Done():
                return nil, fmt.Errorf("dagworker: Claim: %w", context.Cause(ctx))
            }
        default:
            return nil, fmt.Errorf("dagworker: Claim: %w", err)
        }
    }
}
```

Two `context` additions matter specifically for the lease-timeout mechanism (§9.3):
[`context.AfterFunc`](https://pkg.go.dev/context#AfterFunc) (Go 1.21) — *"arranges to call f in
its own goroutine after ctx is canceled ... the returned stop function does not wait for f to
complete"* — is the primitive the lease reaper is built on, and
[`context.WithTimeoutCause`/`WithDeadlineCause`/`Cause`](https://pkg.go.dev/context#WithTimeoutCause)
(Go 1.21) let a canceled claim's context distinguish *why* it ended: `context.Canceled` (caller
gave up), `context.DeadlineExceeded` (caller's own timeout), or a library-supplied
`ErrLeaseExpired` cause attached via `WithDeadlineCause` — so `errors.Is(err, ErrLeaseExpired)`
works even though the underlying mechanism is a plain context deadline.

---

## 6. Errors: a public taxonomy, not an afterthought

### 6.1 The Go 1.13 model, and why it's the only viable one for a library

[Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors) formalized what libraries like
`os` had done informally for years: an error type that holds another error implements
`Unwrap() error`, forming a chain; `errors.Is(err, target)` walks that chain doing `==` (or a
custom `Is` method) at each link; `errors.As(err, &target)` walks it doing a type assertion at
each link; and `fmt.Errorf("...: %w", err)` is the one call that both formats a message *and*
makes the wrapped error visible to `Is`/`As` — `%v` formats the same string but severs the chain.
The post's single most important sentence for a library author: *"wrapping an error makes that
error part of your API."* [`errors.Join`](https://pkg.go.dev/errors#Join) (Go 1.20) extends the
same chain-walking to a *list* of causes via `Unwrap() []error`, so `errors.Is`/`As` do a
depth-first search across all of them — the case this project needs it for is `Manager.Close`
returning every internal goroutine's shutdown error as one value instead of discarding all but
the first.

The [Uber style guide's decision
matrix](https://github.com/uber-go/guide/blob/master/style.md#error-types) is the cleanest
statement of when to reach for which tool, and this project adopts it verbatim:

| Caller needs to match the error? | Message is static? | Use |
|---|---|---|
| No | — | `errors.New("...")` or `fmt.Errorf("...")` inline |
| Yes | Yes | package-level `var ErrXxx = errors.New("...")` |
| Yes | No (carries data) | a custom `*XxxError` type implementing `error` and `Unwrap`/`Is` |

The [Google style guide](https://google.github.io/styleguide/go/decisions.html) adds the
complementary framing that matters when a `CycleError` must carry the offending path: *"Error
values in Go typically have a component intended for human eyes and a component intended for
semantic control flow"* — the human string goes in `Error()`, the semantic payload goes in typed,
exported fields, and tests should assert on the latter, never on the former (error-string
assertions break on every wording tweak and are explicitly discouraged by Google's guide for this
reason).

### 6.2 The dag-worker-go error taxonomy

```go
package dagworker

import "errors"

// Sentinel errors. Callers match with errors.Is, never with ==, because
// every error returned across a storage-backend boundary is wrapped at least
// once (with %w) to add scope/node context — see
// https://go.dev/blog/go1.13-errors.
var (
    // ErrNotFound is returned when a referenced node, edge, or lease does
    // not exist in the given scope.
    ErrNotFound = errors.New("dagworker: not found")

    // ErrConflict is returned by a mutating call that lost an optimistic
    // concurrency race against another instance sharing the same storage
    // (see the cross-instance coordination research doc for the underlying
    // CAS/lease protocol).
    ErrConflict = errors.New("dagworker: conflict")

    // ErrCycle is the sentinel wrapped by CycleError (below); match against
    // this, not against CycleError directly, unless you need the path.
    ErrCycle = errors.New("dagworker: dependency cycle")

    // ErrLeaseExpired is the Cause (context.Cause) of a claim's context
    // once its lease timeout fires without an Ack; also returned by Ack
    // itself if called after expiry.
    ErrLeaseExpired = errors.New("dagworker: lease expired")

    // ErrLeaseMismatch is returned by Ack/Extend when the supplied Lease
    // token does not match the node's current lease (another worker or
    // instance has since re-claimed it after expiry).
    ErrLeaseMismatch = errors.New("dagworker: lease token mismatch")

    // ErrScopeEmpty is returned by a non-blocking claim attempt when no
    // node in the scope is currently ready. Claim (blocking) never returns
    // this to callers; it retries internally until ctx is done.
    ErrScopeEmpty = errors.New("dagworker: no ready node in scope")

    // ErrClosed is returned by any Manager method called after Close has
    // been invoked.
    ErrClosed = errors.New("dagworker: manager closed")

    // ErrNilStore and ErrInvalidConfig are New-time construction errors.
    ErrNilStore      = errors.New("dagworker: store must not be nil")
    ErrInvalidConfig = errors.New("dagworker: invalid option value")

    // ErrSubscriberLagged is delivered as Subscription.Err() (never as a
    // panic or a silently-dropped channel close) when the overflow policy
    // is OverflowCloseSlow and the subscriber could not keep up — see §8.5.
    ErrSubscriberLagged = errors.New("dagworker: subscriber lagged and was disconnected")

    // ErrResumeTooOld is returned by Subscribe when the caller's resume
    // token references a revision the event log has already compacted —
    // the etcd precedent, see §8.4. The caller must resync via List then
    // re-subscribe from the fresh token.
    ErrResumeTooOld = errors.New("dagworker: resume token older than retained history")
)

// CycleError reports a dependency cycle detected while adding an edge. It
// wraps ErrCycle so callers can use either errors.Is(err, ErrCycle) for a
// coarse check or errors.As(err, &cycleErr) to inspect Path.
type CycleError struct {
    Scope Scope
    Path  []NodeID // the cycle, in edge order, first node repeated last
}

func (e *CycleError) Error() string {
    return fmt.Sprintf("dagworker: cycle in scope %q: %s", e.Scope, joinNodeIDs(e.Path))
}

func (e *CycleError) Unwrap() error { return ErrCycle }

// LeaseError reports a lease-related failure with the specific lease token
// and node involved, for structured logging at the call site.
type LeaseError struct {
    NodeID NodeID
    Token  string
    Err    error // ErrLeaseExpired or ErrLeaseMismatch
}

func (e *LeaseError) Error() string {
    return fmt.Sprintf("dagworker: lease %s on node %s: %v", e.Token, e.NodeID, e.Err)
}

func (e *LeaseError) Unwrap() error { return e.Err }
```

Every storage backend's own errors (a Redis `EXEC` failure, a Postgres serialization failure, a
`context.DeadlineExceeded` from a network call) are wrapped, never returned bare, at the boundary
between `store.XxxStore` and `Manager`: `fmt.Errorf("dagworker: claim: %w", err)`. This keeps
`errors.Is(err, context.DeadlineExceeded)` working for a caller who wants to distinguish "the
store timed out" from "the lease expired," while the *message* still carries the dag-worker-go
operation name — matching Uber's rule against redundant "failed to" phrases stacking up
(`"claim: tryClaim: redis: EXEC: %w"`, not `"failed to claim: failed to run tryClaim: failed to
exec in redis: %w"`). Panics are reserved, per both Effective Go's and Uber's guidance, for
programmer errors caught at construction time only (`New` returning `ErrNilStore` rather than
panicking is itself a deliberate choice — a library should prefer an error return even at
construction, reserving `panic` for truly unrecoverable states, matching Uber's "panic only for
irrecoverable states or during initialization" and only when there is no error return available at
all, e.g., inside a `var _ = ...` package-level check).

---

## 7. Generics: where they earn their place, and where they don't

[The Go blog's "When To Use
Generics"](https://go.dev/blog/when-generics) draws the line this project should hold: type
parameters pay off for **containers and algorithms** that make no assumption about the element
type beyond what a constraint states — a generic linked list, a generic `Map`/`Filter`/`Reduce`
over slices — and they actively hurt when the real requirement is *behavior*: *"If all you need to
do with a value of some type is call a method on that value, use an interface type, not a type
parameter."* The [Google style
guide](https://google.github.io/styleguide/go/decisions.html#generics) sharpens this into two
explicit warnings this project takes as hard rules: don't generalize a type that currently has
exactly one concrete instantiation ("if there is only one type being instantiated in practice,
start by making your code work on that type without generics at all"), and never use generics to
build an error-handling DSL or a framework.

**Where generics do *not* belong in dag-worker-go: the `Store` interface and the wire/storage
representation of a node payload.** The moment `Store` becomes `Store[T any]`, every backend
implementation (Redis, Memcached, Postgres) is forced to either serialize `T` generically — which
means reflection-based JSON or gob encoding hidden behind a type parameter, buying nothing over
`[]byte` — or become itself generic over `T`, which means a `RedisStore[T]` can never be
constructed without knowing `T` at the call site, which is impossible for a library that must let
*independent processes* (potentially built from different `go.mod`s, at different versions) attach
to the same shared storage and interoperate. Cross-process shared storage means the payload is
necessarily a byte-oriented wire format at the storage boundary; the `Payload` field on `Node` and
`Claim` is `json.RawMessage` (or `[]byte` with a documented encoding, see the serialization
research doc), full stop, and `Store` is not generic:

```go
type Node struct {
    ID      NodeID
    Scope   Scope
    Status  Status
    Payload json.RawMessage
    // ...
}

type Store interface {
    NodeStore
    EdgeStore
    LeaseStore
}
```

**Where generics genuinely help: a thin, optional, in-process-only typed convenience layer that a
single embedder can use if every producer and consumer of a given scope's payloads is *in the same
binary* and agrees on a Go type.** This is exactly the container/algorithm case the Go blog
endorses — it is a generic wrapper around an untyped API, not a generic reshaping of the API
itself, so it can be deleted from the module without touching `Store` or the wire format:

```go
// Typed adapts a Manager to a single Go payload type T for one scope,
// entirely in-process. It exists because most embedders have exactly one
// payload type per scope and do not want json.RawMessage littering call
// sites — see https://go.dev/blog/when-generics ("use type parameters for
// containers, not for behavior"); Typed contains no behavior of its own,
// only encode/decode around the untyped Manager.
type Typed[T any] struct {
    m     *Manager
    scope Scope
}

func NewTyped[T any](m *Manager, scope Scope) Typed[T] {
    return Typed[T]{m: m, scope: scope}
}

func (t Typed[T]) AddNode(ctx context.Context, id NodeID, payload T, deps ...NodeID) error {
    raw, err := json.Marshal(payload)
    if err != nil {
        return fmt.Errorf("dagworker: Typed.AddNode: encode payload: %w", err)
    }
    return t.m.AddNode(ctx, t.scope, id, raw, deps...)
}

func (t Typed[T]) Claim(ctx context.Context, opts ...ClaimOption) (TypedClaim[T], error) {
    c, err := t.m.Claim(ctx, t.scope, opts...)
    if err != nil {
        return TypedClaim[T]{}, err
    }
    var payload T
    if err := json.Unmarshal(c.Payload, &payload); err != nil {
        return TypedClaim[T]{}, fmt.Errorf("dagworker: Typed.Claim: decode payload: %w", err)
    }
    return TypedClaim[T]{Claim: c, Payload: payload}, nil
}

type TypedClaim[T any] struct {
    Claim
    Payload T
}
```

This also satisfies the Google guide's documentation bar for exported generic APIs ("motivating
runnable examples") trivially, because the motivating example is one line:
`type Job struct{...}; jobs := dagworker.NewTyped[Job](mgr, "ingest")`.

---

## 8. Event delivery to subscribers, without deadlocking or leaking

This is the hardest concurrency-design problem in the library, because it has three producers a
subscriber cannot be allowed to block (the storage-mutation path, the lease reaper, and — in a
multi-instance deployment — a poller/change-feed reading another instance's writes) and an
unbounded, untrusted number of consumers (anyone who calls `Subscribe`) each running at their own
unpredictable pace. Three production systems have already solved variants of exactly this problem
in Go, and this design borrows from all three.

### 8.1 The never-block-the-producer rule

Every one of the reference systems below enforces the same invariant, phrased slightly
differently: **the code path that observes a state change must never be the code path that also
does slow, unbounded, or externally-blocking work with that change.** A producer that writes
directly into a subscriber-owned unbounded channel, or that calls a subscriber's callback inline
and waits for it to return, ties the producer's forward progress to the slowest subscriber —
turning one stuck consumer into a global outage. The three concrete techniques used to break this
coupling are (a) a bounded buffer per subscriber with an *explicit, chosen* overflow policy, never
an unbounded channel "to be safe," (b) decoupling detection from processing via an intermediate
work queue so the detector never runs user code at all, and (c) a resumable position marker so a
disconconnected/reset consumer can catch up from where it left off instead of the producer having
to remember infinite history for it.

### 8.2 Case study: Kubernetes client-go informers + workqueue

[`client-go`'s shared informers](https://pkg.go.dev/k8s.io/client-go/util/workqueue) already hit
the failure mode directly: an informer's `ResourceEventHandler.OnUpdate` callbacks run
**synchronously and sequentially** on the informer's single delivery goroutine, so a slow handler
delays every later event for every other handler on that informer, and — because `OnUpdate` has no
return value — a failed handler has no retry path at all. The fix client-go itself recommends is
to make the handler do *nothing but enqueue*: push the object's key onto a
[`workqueue.TypedRateLimitingInterface[T]`](https://pkg.go.dev/k8s.io/client-go/util/workqueue) and
return immediately; a separate pool of worker goroutines pulls from the queue at its own pace.

```go
// TypedInterface — the core queue contract every k8s controller is built on.
// https://pkg.go.dev/k8s.io/client-go/util/workqueue
type TypedInterface[T comparable] interface {
    Add(item T)
    Len() int
    Get() (item T, shutdown bool)
    Done(item T)
    ShutDown()
    ShuttingDown() bool
}

type TypedRateLimitingInterface[T comparable] interface {
    TypedDelayingInterface[T]
    AddRateLimited(item T)
    Forget(item T)
    NumRequeues(item T) int
}
```

Three properties of this design generalize directly to dag-worker-go's dispatch of "ready" nodes
to external workers:

1. **`Add` de-duplicates.** The queue is a set with insertion order, not a plain FIFO — if the same
   key is added twice before a worker calls `Get`, it is processed once. A `Claim` call for scope
   `S` should be backed by the same idea: many redundant "node became ready" signals for the same
   node coalesce into one ready-to-claim marker, not a growing backlog of duplicate notifications.
2. **`Done` + re-add-if-dirty.** If an item is re-marked dirty *while* a worker is processing it
   (`Done` hasn't been called yet), the queue remembers and re-delivers it after `Done`, instead of
   losing the update or processing stale data. This is the shape a lease-timeout re-queue should
   take: if a node's lease expires while another `AddEdge` touches an unrelated part of the same
   scope, the retry must not be dropped.
3. **[`ItemExponentialFailureRateLimiter`](https://github.com/kubernetes/client-go/blob/master/util/workqueue/default_rate_limiters.go)
   is per-item, not global**, and client-go's `DefaultControllerRateLimiter` layers it under a
   global token-bucket cap (10 qps / burst 100 by default) — one hot, endlessly-failing item backs
   off on its own schedule without starving unrelated items of their own first attempt. This is
   the model for retry/backoff on a node whose worker keeps timing out: back off *that node*
   exponentially (5ms→10ms→...→cap), not the whole scope.

The controller pattern client-go teaches — [detection populates a queue; a separate worker pool
drains it](https://github.com/kubernetes/client-go/blob/master/examples/workqueue/main.go) — maps
onto dag-worker-go as: the internal "which nodes just became ready" detector (triggered by
`AddNode`/`AddEdge`/`Ack` completing a dependency) only ever pushes a `NodeID` into an internal,
bounded, per-scope ready-set; it is `Claim` (called from the *worker's* goroutine, at the worker's
own pace) that pulls from it. The detector thread is never the thing waiting on a slow worker.

### 8.3 Case study: NATS Go client slow-consumer detection

The [NATS Go client](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers) takes
the opposite stance from "buffer forever": every asynchronous subscription has a **hard cap** on
pending messages and bytes (default 500,000 messages / 64 MiB per subscription,
[docs.nats.io](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)), and the
moment a subscription's internal buffer would exceed either cap, the client does not block the
connection's read loop — it **drops the message, marks the subscription
`SubscriptionSlowConsumer`, and delivers `nats.ErrSlowConsumer` to the connection's async error
callback**, a side channel the application must register to find out
([nats.go source](https://github.com/nats-io/nats.go/blob/main/nats.go),
[Synadia's writeup](https://www.synadia.com/insights/checks/nats-slow-consumers)). Two lessons
generalize:

- **Bound, don't queue forever, and detect the overflow *locally* before the *server* has to.** The
  NATS docs are explicit that "it is better to catch a slow consumer locally in the client rather
  than allow the server to detect this condition" — the cheapest place to enforce backpressure is
  the first hop, not several network calls downstream.
  - **Never let overflow be silent.** An overflowing subscription doesn't just quietly stop
  updating — it transitions to an observable error state (`SubscriptionSlowConsumer`) that the
  caller can poll (`sub.Pending()` against `sub.PendingLimits()`) or be pushed via callback.
  Silent staleness is worse than an explicit disconnect, because a silently-stalled subscriber
  looks healthy to every monitoring signal except "did the numbers stop moving."

### 8.4 Case study: etcd's watch API and resume revisions

[etcd's watch API](https://etcd.io/docs/v3.6/learning/api/) solves the *reconnection* half of the
problem that NATS's model doesn't address: NATS's pending-buffer cap protects the producer, but
says nothing about how a disconnected consumer picks back up without replaying its entire history
or silently missing events in the gap. etcd's answer is a **monotonic revision number** attached
to every mutation and to every watch event: a client opens a watch with an optional
`start_revision`; if omitted, the stream starts from "now." On disconnect, [the client library
resumes by re-opening the watch at `start_revision = last_received_revision +
1`](https://github.com/etcd-io/etcd/blob/main/client/v3/watch.go) — the client-side `watch.go`
literally tracks `nextRev` per substream and threads it through
`newWatchClient`'s reconnect path so every re-established watcher resumes exactly where it left
off, not from "now" (which would silently drop the gap) and not from "the beginning" (unbounded
replay). The one failure mode this can't paper over: if the requested `start_revision` has already
been [compacted](https://etcd.io/docs/v3.6/learning/api/) out of etcd's retained history, the
server returns `ErrCompacted` (`compact_revision` set in the response) and the client's only
correct move is a full resync (list current state, get a fresh revision, re-watch from there) —
there is no way to serve a resume request for history that no longer exists, and etcd does not
pretend otherwise.

Additionally, etcd's watch stream supports **`progress_notify`**: a periodic empty `WatchResponse`
carrying only an updated revision, sent even when nothing changed, specifically so a client can
still advance its resume checkpoint during a quiet period and doesn't have to replay a large gap
if the *next* real event arrives long after the last real one did.

### 8.5 Design for dag-worker-go: bounded channel + resume revision + chosen overflow policy

Synthesizing §8.1–8.4 into one `Subscribe` API:

```go
// Rev is a per-scope, strictly monotonically increasing sequence number
// assigned by the store to every recorded status transition and every
// "ready for work" signal. It is dag-worker-go's equivalent of etcd's mod
// revision (https://etcd.io/docs/v3.6/learning/api/) and exists so a
// reconnecting subscriber can resume exactly where it left off instead of
// either missing events or replaying from the beginning.
type Rev uint64

type EventKind int

const (
    EventTransition EventKind = iota // a node's public Status changed
    EventReady                       // a node must now be taken for processing
)

type Event struct {
    Rev     Rev
    Scope   Scope
    NodeID  NodeID
    Kind    EventKind
    From, To Status // populated for EventTransition; zero value otherwise
    At      time.Time
}

type OverflowPolicy int

const (
    // OverflowDropOldestAndMarkGap evicts the oldest buffered event to make
    // room for the newest, and marks Subscription.Lagged() true so the
    // caller can detect (and, e.g., trigger a resync) rather than silently
    // consume a stream with holes in it. Default policy: matches NATS's
    // "bound the buffer, never block the producer" stance
    // (https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)
    // while still trying to deliver *something* recent.
    OverflowDropOldestAndMarkGap OverflowPolicy = iota

    // OverflowBlock applies backpressure to the producer's dispatch path
    // for this one subscriber's slot only (never to storage mutation
    // itself — see the isolation note below). Use only for a subscriber
    // whose processing latency is bounded and trusted, e.g., an in-process
    // metrics counter.
    OverflowBlock

    // OverflowCloseSlow disconnects the subscriber outright: Events() is
    // closed and Err() returns ErrSubscriberLagged. Mirrors NATS marking a
    // subscription SubscriptionSlowConsumer
    // (https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers)
    // — an explicit, observable failure instead of silent staleness.
    OverflowCloseSlow
)

type SubscribeOptions struct {
    Scope      Scope // zero value ("") subscribes to every scope
    From       Rev   // resume token; 0 means "start from now"
    BufferSize int   // 0 means "use the manager default from WithSubscriberBufferSize"
    Overflow   OverflowPolicy
}

type Subscription struct {
    events chan Event
    errc   chan error
    cancel context.CancelFunc
    done   chan struct{}
}

// Events returns the channel of delivered events. It is closed when the
// Subscription's context is done, when Close is called, or (under
// OverflowCloseSlow) when the subscriber lagged. Callers must keep draining
// it — see the workqueue pattern in §8.2, this channel plays the role of
// the queue and the caller's own goroutine plays the worker.
func (s *Subscription) Events() <-chan Event { return s.events }

// Err returns the reason Events() was closed, if any. ErrResumeTooOld
// (§8.4's compaction case) and ErrSubscriberLagged (§8.3's slow-consumer
// case) are the two sentinel causes; a nil Err after closure means a clean
// shutdown via Close or context cancellation.
func (s *Subscription) Err() error

func (s *Subscription) Close() error
```

Internally, per-subscription delivery is a **single dedicated goroutine per subscriber** that owns
a bounded `chan Event` and applies the chosen `OverflowPolicy` — the fan-out point (where a
committed mutation becomes an `Event`) never itself blocks on any one subscriber's channel; it
posts to each subscriber's *inbound* lock-free ring buffer (or, more simply in v0, tries a
non-blocking `select { case sub.in <- ev: default: sub.handleOverflow(ev) }`) and returns
immediately, exactly matching the "detection populates a queue; a separate goroutine drains it"
separation from §8.2. This is what makes `OverflowBlock` safe to offer at all: the *block* only
ever back-pressures the one per-subscriber pump goroutine competing for `sub.in`, never the shared
mutation path that produced the event, so one blocking subscriber cannot stall `AddNode`/`Ack` for
every other caller. **`OverflowBlock` must therefore be documented as inherently unsafe against an
unbounded number of concurrent mutators contending on that one subscriber's slot** — it trades
"never drop an event for this subscriber" for "a badly-behaved slow subscriber can measurably slow
down the fan-out goroutine's own loop," which is why it defaults off.

Resumption follows etcd exactly: `SubscribeOptions.From` is a `Rev`; if it references a revision
still within the event log's retention window, the subscription's first delivered events are the
backlog since `From`, then live events, with no gap and no duplicate; if `From` is older than the
oldest retained revision, `Subscribe` returns `ErrResumeTooOld` immediately (never silently starts
from "now," which would hide missed transitions) and the caller is expected to re-derive its state
via a `List`/snapshot call and re-subscribe from the fresh `Rev` it returns — the same "410 Gone,
go relist" contract Kubernetes watches use for the identical reason.

### 8.6 The callback alternative

A callback-based subscription API (`Handle(func(Event))`) is offered as a thin convenience layer
on top of the channel primitive, never as an independent implementation, because a callback
invoked *inline on the fan-out goroutine* reintroduces exactly the coupling §8.1 forbids — the
producer would again be blocked on arbitrary user code. The only safe callback shape spawns its
own drain goroutine internally and is, structurally, `Subscribe` plus a `for ev := range
sub.Events() { handler(ev) }` loop the library runs on the caller's behalf:

```go
// Handle is sugar over Subscribe for callers who prefer a callback to a
// channel. It starts one internal goroutine that ranges over a Subscription
// and invokes fn for each Event; fn must not block indefinitely — it runs
// under whatever OverflowPolicy opts specifies, exactly as if the caller
// had ranged over Events() themselves. The returned func stops the
// goroutine and blocks until it has exited (see the Close contract, §9.5).
func (m *Manager) Handle(ctx context.Context, opts SubscribeOptions, fn func(Event)) (stop func(), err error) {
    sub, err := m.Subscribe(ctx, opts)
    if err != nil {
        return nil, err
    }
    done := make(chan struct{})
    go func() {
        defer close(done)
        for ev := range sub.Events() {
            fn(ev)
        }
    }()
    return func() { sub.Close(); <-done }, nil
}
```

---

## 9. Goroutine lifecycle and shutdown

### 9.1 The ownership rule

Every goroutine this library ever starts is owned by exactly one `Manager` (or one `Subscription`,
which is itself owned by the `Manager` that created it) and has a single documented way to stop:
either its owner's context is canceled, or its owner's `Close`/`Cancel` method is called. This is
the rule Cheney's Practical Go and the Uber style guide both converge on ("never start a goroutine
without knowing how it will stop" / "every goroutine must have a predictable exit path"), and it
is enforced structurally, not just by convention, via the `Close` contract in §9.5. Concretely, a
`Manager` running against the in-memory backend owns exactly three categories of long-lived
goroutine: (1) the fan-out dispatcher (§8.5), (2) the lease-reaper (§9.3), and (3) one pump
goroutine per live `Subscription`. A `Manager` running against a networked backend additionally
owns a change-feed poller per subscribed scope. None of these are started lazily on first use from
an arbitrary caller's goroutine — they are all started once, inside `New`/`newManager`, so their
lifetime is anchored to the `Manager`'s own lifetime from the start.

### 9.2 errgroup for coordinated internal goroutines

[`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) is the right
primitive for the *internal* goroutine set a `Manager` owns, because it gives three things a bare
`sync.WaitGroup` doesn't: a derived `context.Context` that is canceled the instant any one
goroutine returns a non-nil error (so a crashed dispatcher immediately signals the lease-reaper and
every subscription pump to stop, rather than leaving them running against a half-dead manager),
`Wait()` returning the *first* real error instead of forcing manual aggregation, and (via
`SetLimit`) an optional cap on concurrently-running internal goroutines that is useful for
per-scope poller fan-out against a networked backend so a `Manager` watching 10,000 scopes doesn't
open 10,000 concurrent change-feed connections.

```go
func newManager(store Store, cfg config) *Manager {
    ctx, cancel := context.WithCancel(context.Background())
    g, gctx := errgroup.WithContext(ctx)
    m := &Manager{store: store, cfg: cfg, shutdown: cancel, g: g}
    g.Go(func() error { return m.runDispatcher(gctx) })
    g.Go(func() error { return m.runLeaseReaper(gctx) })
    return m
}

func (m *Manager) Close(ctx context.Context) error {
    m.closeOnce.Do(func() {
        m.shutdown() // cancel the internal errgroup context
        close(m.closed)
    })
    done := make(chan error, 1)
    go func() { done <- m.g.Wait() }()
    select {
    case err := <-done:
        return err
    case <-ctx.Done():
        return fmt.Errorf("dagworker: Close: internal goroutines did not exit: %w", ctx.Err())
    }
}
```

Note `Close` itself takes a `ctx` — this is the one deliberate, narrow exception to "internal
goroutines are bounded by the manager's own lifetime, not a caller's": a caller shutting down its
whole process needs a bounded wait, not an indefinite block, if a storage backend's network call
is wedged; the internal goroutines are still told to stop via the errgroup's own canceled context
regardless of whether `Close`'s caller-supplied `ctx` expires first.

### 9.3 context.AfterFunc for lease timeouts

The lease-timeout mechanism — "if a worker does not answer within a timeout, the node is marked
error-with-timeout" — is a direct application of [`context.AfterFunc`](https://pkg.go.dev/context#AfterFunc)
(Go 1.21): *"AfterFunc arranges to call f in its own goroutine after ctx is canceled ... the
returned stop function does not wait for f to complete."* Each claim gets its own
`context.WithDeadlineCause(ctx, expiresAt, ErrLeaseExpired)`, and `AfterFunc` schedules the
timeout-handling closure — mark the node `Error` with a timeout cause, requeue for retry per the
policy in §8.2's `ItemExponentialFailureRateLimiter` precedent, and free the lease — to run exactly
once, automatically, without the library needing a separate polling goroutine per outstanding
lease (which would not scale to the million-node target):

```go
func (m *Manager) beginLease(nodeID NodeID, timeout time.Duration) (Lease, func() bool) {
    token := newLeaseToken()
    deadline := m.clock.Now().Add(timeout)
    leaseCtx, cancel := context.WithDeadlineCause(m.rootCtx, deadline, ErrLeaseExpired)
    stop := context.AfterFunc(leaseCtx, func() {
        defer cancel()
        m.onLeaseTimeout(nodeID, token) // marks Error-with-timeout, requeues per backoff
    })
    // stop() is called by Ack/Extend on the happy path; if the ack races
    // the timeout, AfterFunc's own doc guarantees f runs at most once and
    // stop() reports whether it *prevented* that run — see
    // https://pkg.go.dev/context#AfterFunc.
    return Lease{Token: token, ExpiresAt: deadline}, stop
}
```

Because `AfterFunc`'s `f` runs in its own goroutine and the doc explicitly warns `stop` "does not
wait for f to complete," `onLeaseTimeout` and `Ack` must be written to tolerate running
concurrently with each other and agree on a single winner — this is naturally expressed as the same
optimistic-CAS-on-lease-token mechanism the storage layer already needs for cross-instance safety
(see the storage-backend research doc), not a new mutex.

### 9.4 sync.WaitGroup.Go (Go 1.25)

[Go 1.25 added `WaitGroup.Go`](https://pkg.go.dev/sync#WaitGroup.Go): `func (wg *WaitGroup) Go(f
func())` — *"Go calls f in a new goroutine and adds that task to the WaitGroup. When f returns, the
task is removed."* This collapses the historically error-prone `wg.Add(1); go func() { defer
wg.Done(); f() }()` triplet (a very common source of leaked/miscounted goroutines when the `Add`
and `go` are accidentally separated by a conditional) into one call, and this project's target
minimum Go version should be **1.25** specifically to get this API in the toolbox for the simpler,
non-error-propagating cases where `errgroup` is overkill — chiefly the per-subscription pump
goroutines from §8.5, which have no error to propagate (a pump's only failure mode is "the
subscriber lagged," which is communicated via `Subscription.Err()`, not a return value):

```go
func (m *Manager) newSubscription(opts SubscribeOptions) *Subscription {
    sub := &Subscription{events: make(chan Event, opts.BufferSize)}
    m.subWG.Go(func() { m.pumpSubscription(sub, opts) }) // sync.WaitGroup.Go, Go 1.25
    return sub
}

// Close (§9.5) does m.subWG.Wait() after canceling every subscription's
// context, guaranteeing every pump goroutine has exited before returning.
```

### 9.5 The Close contract

`Manager.Close` is a hard promise, not a best-effort cleanup: **it returns only after every
goroutine the `Manager` started has exited**, mirroring the pattern both the Uber style guide and
Practical Go independently converge on ("expose an object managing the goroutine's lifetime, with
a `Close`/`Stop` method that signals the goroutine and *waits* for it," not one that merely
signals). This is why §9.2's `Close` blocks on `g.Wait()` and §9.4's pump goroutines are tracked in
a `WaitGroup` that `Close` also joins. Every `Subscription.Close()` call similarly blocks until its
one pump goroutine has exited — never a fire-and-forget `close(sub.stopCh)` that returns before the
goroutine has actually stopped touching `sub.events`, because a caller that immediately reuses or
garbage-collects state referenced by a not-yet-stopped goroutine is exactly the class of race the
"every goroutine has an owner and a shutdown path that is *awaited*" rule exists to prevent.

---

## 10. Concurrency primitives for the in-memory backend

At the 1,000,000-node scale target with O(1)/O(log n) operations required, the in-memory backend's
data-structure choice is not a stylistic question — it is the difference between a benchmark that
scales linearly with core count and one that flatlines past 8 cores from lock contention.

### 10.1 Sharded/striped maps vs. sync.Map

A single `map[NodeID]*Node` behind one `sync.RWMutex` serializes every write across the *entire*
node set regardless of scope, which directly violates the "multiple instances/goroutines must make
progress concurrently" requirement once write-heavy workloads (bulk `AddNode` during DAG
construction) run alongside read-heavy ones (status polling). The standard fix — used by
essentially every high-throughput Go in-memory store — is a **sharded (striped) map**: hash the key
to one of N independent `map + RWMutex` shards, so unrelated keys almost never contend:

```go
const numShards = 256 // power of two; see false-sharing note in §10.5

type shardedMap[K comparable, V any] struct {
    shards [numShards]shard[K, V]
}

type shard[K comparable, V any] struct {
    mu sync.RWMutex
    m  map[K]V
}

func (s *shardedMap[K, V]) shardFor(k K) *shard[K, V] {
    h := hashOf(k) // e.g. maphash.Comparable, or fnv over []byte(id)
    return &s.shards[h&(numShards-1)]
}

func (s *shardedMap[K, V]) Load(k K) (V, bool) {
    sh := s.shardFor(k)
    sh.mu.RLock()
    defer sh.mu.RUnlock()
    v, ok := sh.m[k]
    return v, ok
}

func (s *shardedMap[K, V]) Store(k K, v V) {
    sh := s.shardFor(k)
    sh.mu.Lock()
    defer sh.mu.Unlock()
    sh.m[k] = v
}
```

The natural sharding key for this project is the **scope** first, then the node ID within a scope's
own shard set — this both bounds lock contention and gives "delete a scope" an O(shard-count)
bulk-drop instead of an O(n) walk, and it lines up with the requirement that scopes are the unit of
namespacing.

`sync.Map`'s own doc is explicit about when it — rather than a sharded mutex map — is the right
choice: *"the Map type is optimized for two common use cases: (1) when the entry for a given key is
only ever written once but read many times, as in caches that only grow, or (2) when multiple
goroutines read, write, and overwrite entries for disjoint sets of keys"* ([pkg.go.dev/sync#Map](https://pkg.go.dev/sync#Map)).
Neither describes dag-worker-go's node map well: nodes are mutated repeatedly (every status
transition rewrites the same key) by potentially-overlapping goroutines (multiple workers claiming
from the same scope), which is `sync.Map`'s explicitly-documented *worst* case, not its best one. A
sharded `RWMutex` map is the correct default; `sync.Map` is reserved for the one place its access
pattern actually matches its design — the read-mostly, write-once **scope registry** itself (a
scope, once created, is looked up far more often than it is created), and possibly a
pointer-stable "current storage config snapshot" cache.

### 10.2 The Go 1.24 HashTrieMap rewrite of sync.Map

Since Go 1.24, `sync.Map`'s internal implementation was replaced with `HashTrieMap`, the same
concurrent hash-trie originally built for the `unique` package — [tracked in
golang/go#70683](https://github.com/golang/go/issues/70683) and shipped in
[`internal/sync/hashtriemap.go`](https://go.dev/src/internal/sync/hashtriemap.go). It is a 16-way
branching trie where reads are fully lock-free (atomic pointer traversal down the trie) and writes
take a **per-node** mutex affecting only a small subtree, rather than the old implementation's
single amortized "dirty map" promoted wholesale under one mutex. Reported effects: large wins on
disjoint-key-set write contention (`MapLoadAndDeleteCollision` +68.8%, `MapAdversarialDelete`
+77.6%) and a mild regression on the pure read-hit case, because the previous generation's
Swiss-Tables-flavored fast path was slightly faster for reads that never miss. **Practical
takeaway for this project:** on Go ≥1.24 the gap between "hand-rolled sharded map" and `sync.Map`
narrows substantially for the specific disjoint-write-heavy pattern a multi-scope node store
exhibits, which is worth re-benchmarking at the 1M-node target *after* the sharded-map baseline is
built — but it does not change the §10.1 recommendation, because `sync.Map`'s API itself (no
atomic multi-key transactions, `LoadOrStore`/`CompareAndSwap` only per single key) is still a worse
fit than a hand-rolled shard for operations like "atomically move a node from the blocked set to
the ready set," which routinely touch more than one key.

### 10.3 RWMutex contention at high core counts

A plain `sync.RWMutex`'s `RLock` still performs an atomic increment on a shared counter on every
acquisition, so even a pure-read workload with zero actual contention on the *data* pays
cache-coherency traffic on that one counter across every core touching it — this is the standard,
well-documented reason a single global `RWMutex` stops scaling well past roughly 8–16 cores under
read-heavy load, independent of anything about the protected data itself. Sharding (§10.1) is the
direct fix, because it turns one hot counter into N independent, per-shard counters that different
cores rarely touch simultaneously. For the very hottest single piece of state in the system — e.g.,
a per-scope "is anything ready" flag checked on every `Claim` retry loop — prefer `atomic.Bool` /
`atomic.Int64` over an `RWMutex` entirely; a mutex is the wrong tool once the critical section is a
single word.

### 10.4 atomic.Pointer and copy-on-write snapshots

[`atomic.Pointer[T]`](https://pkg.go.dev/sync/atomic#Pointer) — *"an atomic pointer of type *T
[with] Load, Store, Swap, CompareAndSwap"* — is the right primitive for state that is read
constantly by every hot path but changed rarely: the resolved `config` a `Manager` was constructed
with, a scope's cached shard-count/topology, or (most usefully here) a **per-scope
dependents-index snapshot** used to fan out "node X completed, who's now ready" without holding a
lock during the fan-out computation itself. The pattern is copy-on-write: a writer builds an
entirely new, immutable index and atomically swaps the pointer; every concurrent reader either sees
the whole old index or the whole new one, never a torn read, and readers take **zero locks**:

```go
type dependentsIndex map[NodeID][]NodeID // node -> nodes that depend on it

type scopeState struct {
    dependents atomic.Pointer[dependentsIndex]
}

func (s *scopeState) Dependents(id NodeID) []NodeID {
    idx := s.dependents.Load() // lock-free
    return (*idx)[id]
}

func (s *scopeState) rebuildDependents(newIdx dependentsIndex) {
    s.dependents.Store(&newIdx) // old readers keep their snapshot until GC'd
}
```

This only pays off when reads vastly outnumber writes and the rebuilt structure is cheap enough to
reconstruct wholesale — exactly the shape of a dependents index that changes only on `AddEdge`,
read on every single status transition.

### 10.5 False sharing and cache-line padding

Independent atomic counters or independent mutexes that happen to sit within the same 64-byte CPU
cache line silently serialize each other anyway, because the cache-coherency protocol invalidates
the whole line on any core's write to any part of it — the classic **false sharing** failure mode
([100 Go Mistakes #92](https://100go.co/92-false-sharing/), with the standard fix being to insert
padding so each hot field owns a full cache line):

```go
type paddedCounter struct {
    n atomic.Int64
    _ [64 - 8]byte // pad to a full 64-byte cache line
}

type shardStats struct {
    shards [numShards]paddedCounter // per-shard op counters, no false sharing across shards
}
```

This matters specifically for the `shardedMap`'s per-shard hit/miss counters (if exposed as
metrics, §12.3) and for any array-of-atomics indexed by worker ID or shard ID — the `shard[K,
V]` struct itself in §10.1 is large enough (a full `sync.RWMutex` plus a map header) that it
already exceeds 64 bytes and does not need explicit padding, but a **bare array of `atomic.Int64`
counters, one per shard**, absolutely does, or every counter update from every shard bounces the
same handful of cache lines between every core touching any shard.

---

## 11. Determinism for tests: the Clock interface and testing/synctest

### 11.1 Why an injected Clock is mandatory, not optional

Every part of this design that involves time — lease timeouts (§9.3), exponential backoff (§8.2),
the `progress_notify`-style keepalive on long-idle subscriptions (§8.4) — is untestable at
production timeout scale (seconds to minutes) without either genuinely sleeping in tests (slow,
flaky under CI load, and directly the problem [`testing/synctest`](https://go.dev/blog/synctest)
exists to solve) or abstracting `time.Now`/`time.After`/`time.AfterFunc` behind an interface the
test can control. This project defines its own minimal `Clock` interface — per the "a little
copying is better than a little dependency" proverb from §2.1 — rather than requiring
`benbjohnson/clock` or `clockwork` as a `go.mod` dependency of the *library itself*:

```go
type Clock interface {
    Now() time.Time
    NewTimer(d time.Duration) Timer
    AfterFunc(d time.Duration, f func()) Timer
}

type Timer interface {
    C() <-chan time.Time
    Stop() bool
    Reset(d time.Duration) bool
}
```

`realClock{}` (the default, §4.2) is a two-line wrapper over the `time` package; a `fakeClock` for
tests is provided in an internal `dagworkertest` (or `clocktest`) subpackage so downstream users
who embed dag-worker-go and want to write their own timeout-adjacent tests get the same fake
without pulling in an external clock dependency transitively.

### 11.2 benbjohnson/clock vs. clockwork — which shape to copy

Both libraries solve the same problem and this project's own `Clock` interface (§11.1) is
deliberately compatible with either being adapted underneath it:

| | [benbjohnson/clock](https://github.com/benbjohnson/clock) | [jonboulle/clockwork](https://github.com/jonboulle/clockwork) |
|---|---|---|
| Core types | `Clock` interface + `Mock` implementing it, wrapping nearly all of `time`'s surface (`Now`, `Since`, `After`, `AfterFunc`, `Sleep`, `Tick`, `Ticker`, `Timer`) | `Clock` interface (real) + `FakeClock` (test), narrower surface centered on `Now`/`Sleep`/`After`/`NewTimer` |
| Advancing fake time | `mock.Add(d)` fires any due timers synchronously | `fake.Advance(d)` / `fake.BlockUntil(n)` to wait for N goroutines to be blocked on the clock before advancing — avoids race between "goroutine hasn't called Sleep yet" and "test advances time" |
| Maintenance | Per [pkg.go.dev](https://pkg.go.dev/github.com/benbjohnson/clock), original author stepped back; module now community-maintained | Actively maintained, used inside Kubernetes-adjacent tooling |
| Fit for dag-worker-go | Broader surface than needed | `BlockUntil`'s "wait for N goroutines parked on the clock" idiom is the closer analogue to `synctest.Wait`'s quiescence check (§11.3) and is the one worth copying into this project's own fake |

**Recommendation:** shape the internal fake clock's synchronization primitive after clockwork's
`BlockUntil`/`Advance` pair specifically because it is the one piece of either library that
correctly solves the race `mock.Add` alone does not — a test that calls `fake.Advance(timeout)`
before the code under test has actually reached its `NewTimer` call advances time too early and the
timer never fires. `BlockUntil(n)` blocks the test goroutine until `n` separate goroutines are
registered as waiting on the fake clock, giving a race-free synchronization point.

### 11.3 testing/synctest: fake time and goroutine quiescence, for free

[`testing/synctest`](https://go.dev/blog/synctest) (experimental behind `GOEXPERIMENT=synctest` in
Go 1.24, promoted to a stable, on-by-default package with a `synctest.Test` helper in Go 1.25)
solves a problem an injected `Clock` interface alone cannot: **asserting a negative** ("this timer
must *not* have fired yet") without an arbitrary real-time sleep-and-hope. `synctest.Run(f)`
executes `f` inside an isolated "bubble" with a virtual clock that automatically fast-forwards
whenever *every* goroutine in the bubble is durably blocked (parked on a channel, timer, or mutex —
anything synctest can prove is not about to make progress on its own); `synctest.Wait()`
explicitly blocks the calling goroutine until every other goroutine in its bubble reaches such a
durably-blocked state, i.e., quiescence. Crucially, this means a test can use the **real**
`context.WithTimeout`/`time.Timer` — not an injected fake — and still run in microseconds:

```go
func TestLeaseTimeout(t *testing.T) {
    synctest.Run(func() {
        const timeout = 30 * time.Second
        ctx, cancel := context.WithTimeout(context.Background(), timeout)
        defer cancel()

        m := newTestManager(t) // uses the *real* time package, no fake Clock needed here
        lease, _ := m.beginLease("node-1", timeout)

        time.Sleep(timeout - time.Nanosecond)
        synctest.Wait() // let every timer/goroutine in the bubble settle
        if got, _ := m.Status(ctx, "s", "node-1"); got != StatusInProgress {
            t.Fatalf("before deadline: got %v, want InProgress", got)
        }

        time.Sleep(time.Nanosecond) // crosses the deadline
        synctest.Wait()
        if got, _ := m.Status(ctx, "s", "node-1"); got != StatusError {
            t.Fatalf("after deadline: got %v, want Error (timeout)", got)
        }
        _ = lease
    })
}
```

**Practical rule for this project:** the injected `Clock` interface (§11.1) and `synctest` are
complementary, not competing — `Clock` is required regardless, because a *production* deployment
benefits from it too (e.g., a host that wants a monotonic-but-adjustable clock for testing its own
integration against dag-worker-go), and `Clock`-based fakes remain the right tool for
**cross-goroutine-boundary** scenarios `synctest`'s bubble model handles awkwardly today — most
notably anything that talks to a real external process (a Redis/Postgres container in an
integration test), since `synctest` bubbles do not extend across a real network call or a
separate OS thread performing blocking I/O outside the bubble's tracked goroutines. Use `synctest`
for unit tests of pure in-process timeout/backoff logic (§9.3, §8.2's backoff scheduling); use the
injected `Clock` for anything that must also work when Redis or Postgres is the backend under test.

---

## 12. Observability: slog, OpenTelemetry, and metric naming

### 12.1 log/slog: never force output, never smuggle a logger through context

[`log/slog`](https://go.dev/blog/slog)'s architecture splits a `Logger` (the frontend user code
calls) from a `Handler` interface (the backend that formats and writes) specifically so that a
library and its embedder can agree on the frontend API without the library dictating output
format, destination, or even whether anything is written at all. Two rules follow directly for
dag-worker-go: **accept a `*slog.Logger` as a construction option (`WithLogger`, §4.2), defaulting
to one backed by a handler that discards everything**, so a caller who never calls `WithLogger`
pays zero I/O cost and sees zero unsolicited output on their stdout/stderr — a library that logs to
stderr by default regardless of the host's own logging setup is a well-known anti-pattern this
design explicitly avoids. Second, and just as important: **never place a `*slog.Logger` (or
anything else) into a `context.Context` value for the library's own internal use** — the
[slog blog post itself explains the design deliberately avoided a context-carried
logger](https://go.dev/blog/slog), because it "smuggles in an implicit dependency, making the code
harder to understand"; this is the same argument as §5's context-in-struct rule and dag-worker-go
follows it symmetrically for loggers, tracers, and meters alike — all three are `WithXxx`
construction options, not context values, and all three are threaded explicitly through the
`Manager`'s own fields.

### 12.2 OpenTelemetry for a library: API only, never the SDK

The OpenTelemetry Go docs state the rule for any instrumented library without qualification: *"If
you're instrumenting a library, only install the OpenTelemetry API package for your language ...
your library will not emit telemetry on its own — it will only emit telemetry when it is part of
an app that uses the OpenTelemetry SDK"*
([opentelemetry.io/docs/languages/go/instrumentation](https://opentelemetry.io/docs/languages/go/instrumentation/)).
Concretely: dag-worker-go's `go.mod` depends on `go.opentelemetry.io/otel/trace` and
`go.opentelemetry.io/otel/metric` (the API modules) but never on
`go.opentelemetry.io/otel/sdk` or any exporter — those are the *host* application's dependencies to
choose. `WithTracerProvider`/`WithMeterProvider` options (§4.2) accept the API-level
`trace.TracerProvider`/`metric.MeterProvider` interfaces, default to
`otel.GetTracerProvider()`/`otel.GetMeterProvider()` (which are no-ops until the host configures an
SDK, exactly the graceful-no-telemetry-by-default behavior the docs describe), and the library
acquires its own tracer/meter with an instrumentation-scope name matching its module path, per the
documented convention (`tp.Tracer("github.com/<org>/dag-worker-go")`), so a host running many
instrumented libraries can distinguish dag-worker-go's spans/metrics from everyone else's in one
trace.

### 12.3 Metric naming: base units, `_total`, and a scope-aware label

The two naming conventions this project should hold itself to — [Prometheus's naming
guide](https://prometheus.io/docs/practices/naming/) and [OpenTelemetry's semantic-conventions
naming spec](https://opentelemetry.io/docs/specs/semconv/general/naming/) — agree on the load-bearing
points even though they differ on syntax details (Prometheus's exporter-side `snake_case` vs.
OTel's instrument-side `dot.case`, reconciled automatically by the Prometheus OTel exporter):
always use **base units** (seconds, bytes — never milliseconds or kilobytes, because a bare number
is ambiguous about scale otherwise), a counter's name reflects that it only accumulates (`_total`
suffix in Prometheus terms; OTel leaves the unit off the *name* since it's carried on the
instrument's `Unit` field instead), and units use UCUM codes at the OTel API level (`s`, `By`)
rather than prefixed variants (`ms`, `MiBy`) "unless there is good technical reason to." Applied to
dag-worker-go's own instrumentation surface:

| Metric | Instrument | Unit | Labels/attributes |
|---|---|---|---|
| `dagworker.claims.total` | Counter | `1` | `scope`, `outcome` (`claimed`\|`empty`\|`error`) |
| `dagworker.claim.duration` | Histogram | `s` | `scope` |
| `dagworker.lease.timeouts.total` | Counter | `1` | `scope` |
| `dagworker.nodes.ready` | UpDownCounter (or async gauge) | `1` | `scope` |
| `dagworker.subscribers.active` | UpDownCounter | `1` | — |
| `dagworker.subscriber.lagged.total` | Counter | `1` | `scope` |
| `dagworker.store.op.duration` | Histogram | `s` | `scope`, `op`, `backend` |

Every instrument is created once, lazily, off the injected `MeterProvider`, and every metric name
is prefixed with the module's own instrumentation scope (`dagworker.`) precisely so it composes
cleanly inside a host process that also instruments its own workers and other libraries under the
same `MeterProvider` — matching Prometheus's "prefix by application/library name to indicate
ownership" rule and OTel's namespaced-instrumentation-scope model simultaneously.

---

## 13. The consolidated public API surface

Pulling every decision above into one coherent sketch of the package's exported surface (elided
bodies where already shown above):

```go
package dagworker // import "github.com/<org>/dag-worker-go"

// --- identifiers & minimal public status vocabulary --------------------

type NodeID string
type Scope string
type Rev uint64

type Status string

const (
    StatusNew        Status = "new"
    StatusInProgress Status = "in_progress"
    StatusSuccess    Status = "success"
    StatusError      Status = "error"
)

// --- core types ----------------------------------------------------------

type Node struct {
    ID      NodeID
    Scope   Scope
    Status  Status
    Payload json.RawMessage
    Rev     Rev
}

type Claim struct {
    Node
    Lease Lease
}

type Lease struct {
    Token     string
    ExpiresAt time.Time
}

type Result struct {
    OK      bool
    Payload json.RawMessage // optional result data attached on success
    Cause   error           // optional structured failure reason on !OK
}

// --- construction ----------------------------------------------------------

type Manager struct{ /* unexported */ }

func New(store Store, opts ...Option) (*Manager, error)

type Option interface{ apply(*config) }

func WithDefaultLeaseTimeout(d time.Duration) Option
func WithClock(c Clock) Option
func WithLogger(l *slog.Logger) Option
func WithTracerProvider(tp trace.TracerProvider) Option
func WithMeterProvider(mp metric.MeterProvider) Option
func WithSubscriberBufferSize(n int) Option
func WithOverflowPolicy(p OverflowPolicy) Option
func WithScopeShardCount(n int) Option // in-memory backend tuning

// --- DAG mutation ----------------------------------------------------------

func (m *Manager) AddNode(ctx context.Context, scope Scope, id NodeID, payload json.RawMessage, deps ...NodeID) error
func (m *Manager) AddEdge(ctx context.Context, scope Scope, from, to NodeID) error
func (m *Manager) RemoveNode(ctx context.Context, scope Scope, id NodeID) error
func (m *Manager) Node(ctx context.Context, scope Scope, id NodeID) (Node, error)

// --- worker-facing claim/ack protocol ---------------------------------------

type ClaimOption interface{ applyClaim(*claimConfig) }

func WithLeaseTimeout(d time.Duration) ClaimOption

func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error)
func (m *Manager) Ack(ctx context.Context, lease Lease, result Result) error
func (m *Manager) Extend(ctx context.Context, lease Lease, by time.Duration) error

// --- events ------------------------------------------------------------

func (m *Manager) Subscribe(ctx context.Context, opts SubscribeOptions) (*Subscription, error)
func (m *Manager) Handle(ctx context.Context, opts SubscribeOptions, fn func(Event)) (stop func(), err error)

// --- lifecycle ------------------------------------------------------------

func (m *Manager) Close(ctx context.Context) error

// --- pluggable storage (implemented by the in-memory default and by the
// Redis/Memcached/Postgres backends; see the storage-backend research docs
// for the concrete implementations and the required consistency guarantees) --

type Store interface {
    NodeStore
    EdgeStore
    LeaseStore
}

// --- optional typed convenience layer (§7) ---------------------------------

type Typed[T any] struct{ /* unexported */ }

func NewTyped[T any](m *Manager, scope Scope) Typed[T]
```

---

## Recommendations for dag-worker-go

1. **Ship `New(store Store, opts ...Option)`** with Uber's opaque-`Option`-interface variant
   (§4.2), not a `Config` struct and not Pike's self-referential/undo variant — the config surface
   is add-over-time and most callers want zero options, which is precisely Google's own trigger
   condition for variadic options over a struct.
2. **Never store a `context.Context` on `Manager`, `Subscription`, or any internal type.** Every
   blocking method takes `ctx` as its first parameter; per-claim cancellation is implemented with
   `context.WithDeadlineCause` + `context.AfterFunc`, not a hand-rolled timer goroutine per lease.
3. **Adopt the full sentinel-error-plus-typed-error taxonomy in §6.2 before v0.1, not after** —
   `ErrNotFound`/`ErrConflict`/`ErrCycle`/`ErrLeaseExpired`/`ErrLeaseMismatch`/`ErrClosed`/
   `ErrSubscriberLagged`/`ErrResumeTooOld` — because per §6.1, "wrapping an error makes it part of
   your API," and this taxonomy *is* the load-bearing part of the API multi-instance callers will
   branch on.
4. **Keep `Store` non-generic and byte-oriented at the payload boundary** (`json.RawMessage`);
   offer generics only as the optional, in-process-only `Typed[T]` convenience wrapper in §7 — a
   generic `Store[T]` cannot work across independently-versioned processes sharing one Redis/
   Postgres instance.
5. **Build `Subscribe` on the three-part synthesis in §8.5**: a bounded per-subscriber channel, a
   `Rev`-based resume token (etcd's model), and an explicit, caller-chosen `OverflowPolicy`
   defaulting to drop-oldest-and-mark-gap (NATS's model) — never an unbounded channel, and never a
   fan-out that invokes subscriber code inline on the producer's own goroutine.
6. **Route "node became ready" detection through an internal, de-duplicating ready-set per scope**
   modeled directly on `client-go`'s workqueue (§8.2), so a burst of redundant readiness signals for
   the same node coalesces to one, and a worker's `Claim` pulls from that set rather than the
   detection path calling into worker-facing code directly.
7. **Require Go ≥1.25** as the minimum supported version, specifically to get `sync.WaitGroup.Go`
   (§9.4) and a GA `testing/synctest` (§11.3) into the toolchain from day one rather than retrofitting
   both later — this is a deliberate, documented tradeoff against the wider compatibility a lower
   floor would give, justified because this is a new library with no legacy user base to protect.
8. **Guarantee `Close` blocks until every goroutine the `Manager` ever started has exited** (§9.5),
   built on `errgroup` for the internally-coordinated set (dispatcher, lease-reaper) and
   `sync.WaitGroup` for the independently-failing set (subscription pumps); this contract should be
   asserted by a dedicated goroutine-leak test (`goleak` or an equivalent) in CI on every backend.
9. **Default the in-memory backend to a scope-first sharded `RWMutex` map (§10.1), not `sync.Map`**
   — re-benchmark against `sync.Map`'s Go 1.24 `HashTrieMap` backend (§10.2) once the sharded
   baseline exists, since the gap has genuinely narrowed, but do not default to `sync.Map` given its
   own documentation explicitly describes dag-worker-go's actual access pattern (repeated writes to
   overlapping keys) as its worst case.
10. **Define an internal, minimal `Clock` interface (§11.1)** rather than depending on
    `benbjohnson/clock` or `clockwork` in `go.mod`; shape its fake's synchronization primitive after
    clockwork's `BlockUntil`/`Advance` pair, and use `testing/synctest` for pure in-process
    timeout/backoff unit tests while keeping the injected `Clock` for anything touching a real
    external storage backend.
11. **Depend on the OpenTelemetry API modules only, never the SDK** (§12.2), default every
    provider option to the global no-op providers, and default `WithLogger` to a discarding
    `*slog.Logger` — a library that emits unsolicited stdout/stderr output or forces a tracing
    backend on its embedder is a defect, not a feature, per both the slog and OTel design docs.
12. **Freeze on keyed struct literals as a public-API convention starting at v0.1**, enforced by a
    linter, precisely because the Go 1 compatibility promise (§3) excludes unkeyed literals from
    its guarantee — this is what lets `Node`, `Event`, and `Claim` grow fields post-v1 without that
    counting as a breaking change.

## Open questions

- **Exact overflow-policy default.** §8.5 defaults `Subscribe` to `OverflowDropOldestAndMarkGap`;
  is silently dropping *any* transition event ever acceptable for a library whose stated purpose is
  reliable work dispatch, or should the default instead be `OverflowCloseSlow` (fail loud, force
  the caller to resync) even at the cost of surprising a first-time user who didn't expect their
  subscription to ever be disconnected? This likely needs a per-event-*kind* answer:
  `EventReady` (a worker must act) arguably should never silently drop, while `EventTransition`
  (an observability signal) more plausibly can.
- **How far does the ready-set de-duplication (§8.2/recommendation 6) interact with the
  at-least-once "worker did not answer" retry path?** client-go's workqueue guarantees a dirty item
  is redelivered exactly once after the in-flight processing finishes; dag-worker-go's equivalent
  needs the same guarantee to hold *across process restarts* of the instance that owned the
  in-flight claim, which pushes this from a pure Go-concurrency question into the storage-backend
  design (lease-ownership handoff) covered in a different research doc — the two need to agree on
  one shared state machine.
- **Should `Handle` (the callback convenience API, §8.6) exist in v1 at all**, or is offering only
  the channel-based `Subscribe` simpler to reason about and sufficient, given that `Handle` is a
  five-line wrapper any caller can write themselves? Shipping it adds one more thing to keep stable
  under the Go 1 promise for marginal convenience.
- **Is a per-item exponential-backoff limiter (à la `ItemExponentialFailureRateLimiter`, §8.2)
  in-scope for the library itself, or is "how to handle a node whose worker keeps timing out" a
  host-program policy decision** the library should expose hooks for (a pluggable `RetryPolicy`
  option) rather than hard-code a default for? The brief doesn't specify a retry-on-timeout
  requirement beyond "marked error-with-timeout," which argues for leaving retry/backoff entirely
  to the host, but every reference system studied here bakes in a sane default rather than making
  it a required host decision.
- **`testing/synctest`'s bubble model and a networked storage backend's real goroutines (e.g., a
  Redis connection pool's background reader) may not compose cleanly** — needs a concrete spike
  against `miniredis` or a real `redis://` container to confirm `synctest.Run` either correctly
  detects those goroutines as bubble-external (and ignores them for quiescence) or needs the
  network-backed tests to be excluded from `synctest`-based suites entirely, per the tentative
  guidance in §11.3.
- **Minimum Go version of 1.25 (recommendation 7) is aggressive for a brand-new open-source library
  that wants adoption** — worth revisiting once real users are evaluated, since some enterprise
  Go shops lag current stable by 1–2 versions; the fallback if 1.25 turns out to gate adoption is
  1.23+ with `errgroup`/hand-rolled `WaitGroup.Add/Done` and `GOEXPERIMENT=synctest`-gated tests
  excluded from the default `go test` run.
