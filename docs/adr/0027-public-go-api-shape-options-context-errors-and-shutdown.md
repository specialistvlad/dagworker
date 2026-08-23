# ADR-0027: Public Go API shape: options, context, errors, and shutdown

- **Status:** Accepted
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Backing research:** docs/research/08-go-api-and-concurrency-design.md §2, §4–§6, §9

## Context

dag-worker-go is a library meant to be embedded in a host process for years, not a service with an
independent release cadence the maintainers fully control — every public signature decided now is a
compatibility promise under the Go 1 promise the moment it ships. Four decisions compound to make
or break that promise: how the `Manager` is constructed and configured, how `context.Context` is
threaded through it, how errors are shaped for callers to branch on, and how goroutines it starts
are guaranteed to stop. Getting any one of these wrong is either a breaking change later or a
silent correctness/leak bug for every embedder.

**Construction.** Go's own canon is split: Rob Pike's self-referential functional options solve
runtime reconfiguration-with-undo, which nothing about a `Manager`'s configuration needs (lease
timeout defaults, clock, logger, and buffer sizes are construction-time-only); Dave Cheney's
simpler "apply once at construction" variant is the right half to keep. The Google Style Guide's own
decision table says an options struct wins when most callers must set most fields, and variadic
options win when most callers pass none — and the overwhelming majority of embedders will call
`dagworker.New(store)` with zero options (08 §4.1–§4.2). A plain `Config` struct's zero-value
ambiguity is a specific, real bug magnet here: `Config{}.SubscriberBufferSize == 0` cannot
distinguish "explicitly unbuffered" from "use the library default," a distinction functional
options sidestep entirely because only the library's own default-assignment code runs unless a
caller explicitly overrides it.

**Context.** The `context` package documentation is unambiguous: "Do not store Contexts inside a
struct type; instead, pass a Context explicitly to each function that needs it" (08 §5), because a
stored context obscures its own validity window and invites exactly the kind of subtle bug a
long-lived embedded library cannot afford to reintroduce release after release.

**Errors.** A library embedded in many hosts needs callers to branch on *why* a call failed without
parsing error strings — the Go 1.13 `errors.Is`/`errors.As`/`%w` model exists precisely so that
"the human string goes in `Error()`, the semantic payload goes in typed, exported values" (08
§6.1), and every error crossing a storage-backend boundary must be wrapped, never returned bare,
because callers match with `errors.Is`, never `==`.

**Shutdown.** A `Manager` against the in-memory backend alone owns at least three categories of
long-lived goroutine (the fan-out dispatcher, the lease timeout machinery, one pump per live
`Subscription`); against a networked backend it additionally owns a per-scope listener. Cheney's
Practical Go and the Uber style guide converge on the same rule from opposite directions: "never
start a goroutine without knowing how it will stop," enforced structurally via a `Close` that
*waits*, not one that merely signals (08 §9.1, §9.5).

## Decision

**Construction — Uber's opaque-`Option`-interface functional options, required `store` positional,
never in the options list:**

```go
type Option interface{ apply(*config) }

type optionFunc func(*config)
func (f optionFunc) apply(c *config) { f(c) }

func WithDefaultLeaseTimeout(d time.Duration) Option {
    return optionFunc(func(c *config) { c.defaultLeaseTimeout = d })
}
func WithClock(clk Clock) Option           // test seam — synctest-friendly, ADR-0029
func WithLogger(l *slog.Logger) Option     // nil is a no-op, never a panic
func WithSubscriberBufferSize(n int) Option
func WithOverflowPolicy(p OverflowPolicy) Option
func WithPayloadCap(n int) Option
func WithRetention(r RetentionPolicy) Option

// store is a required, positional dependency — never a functional option,
// per Google's rule that a required collaborator is a constructor argument.
func New(store Store, opts ...Option) (*Manager, error) {
    cfg := defaultConfig()
    for _, opt := range opts {
        opt.apply(&cfg)
    }
    if store == nil {
        return nil, fmt.Errorf("dagworker: New: %w", ErrNilStore)
    }
    if cfg.subscriberBuffer <= 0 {
        return nil, fmt.Errorf("dagworker: New: %w", ErrInvalidConfig)
    }
    return newManager(store, cfg), nil
}
```

`Option`'s concrete type is unexported; every legal instance comes from a `With*` function in this
package, which keeps the option set open for extension without ever changing `New`'s signature —
the same shape `grpc.DialOption` uses at far larger scale.

**Context — first parameter, everywhere, no exceptions, never stored:**

Every `Manager` and `Subscription` method that can block or do I/O takes `ctx context.Context` as
its first parameter. No `Manager` field is ever a `context.Context`. The one narrow, deliberate
exception to "internal goroutines are bounded by the Manager's own lifetime, not a caller's" is
`Close(ctx context.Context) error` itself: a caller shutting down its process needs a *bounded*
wait if a storage backend's network call is wedged, so `Close` accepts a `ctx` purely to bound how
long it will wait for its own internal goroutines — those goroutines are still told to stop via the
`Manager`'s internally-owned cancellation, regardless of whether `Close`'s caller-supplied `ctx`
expires first.

**Errors — sentinel set plus one typed error, matching the taxonomy already fixed in the public API
surface:**

```go
var (
    ErrNotFound, ErrConflict, ErrIDConflict, ErrCycle, ErrCrossScopeEdge,
    ErrAlreadyTerminal, ErrNodeInFlight, ErrLeaseExpired, ErrLeaseMismatch,
    ErrScopeEmpty, ErrScopeSealed, ErrClosed, ErrPayloadTooLarge,
    ErrSubscriberLagged, ErrCursorExpired, ErrNilStore, ErrInvalidConfig error
    // — one var block, one dagworker: prefix, documented individually.
)

type CycleError struct {
    Scope Scope
    Path  []NodeID
}
func (e *CycleError) Error() string { return fmt.Sprintf("dagworker: cycle in scope %q", e.Scope) }
func (e *CycleError) Unwrap() error { return ErrCycle }
```

Every error returned across the storage-port boundary is wrapped with `%w` at least once to add
scope/node context before crossing into the public API; callers are documented to use `errors.Is`
against the sentinels (never `==`, never string comparison) and `errors.As(&CycleError{})` when
they need `Path`. No error is ever returned that is not one of these sentinels, one typed error
wrapping one of these sentinels, or a bare wrapped `ctx.Err()`.

**Shutdown — `Close` blocks until every goroutine the `Manager` started has exited:**

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
    m.closeOnce.Do(func() { m.shutdown(); close(m.closed) })
    done := make(chan error, 1)
    go func() { done <- m.g.Wait() }() // subWG.Wait() joined here too
    select {
    case err := <-done:
        return err
    case <-ctx.Done():
        return fmt.Errorf("dagworker: Close: internal goroutines did not exit: %w", ctx.Err())
    }
}
```

Coordinated internal goroutines (dispatcher, lease reaper, per-scope listeners) run under one
`errgroup.Group` so any one's non-nil error cancels the shared `gctx` for all of them immediately.
Per-subscription pump goroutines, which have no error to propagate (a pump's only failure mode is
"the subscriber lagged," communicated via `Subscription.Err()` — ADR-0022), are tracked with
`sync.WaitGroup.Go` (Go ≥1.25, ADR-0029) instead of `errgroup`, joined by the same `Close`. Every
lease timeout is a `context.AfterFunc(leaseCtx, ...)` (Go 1.21) attached to a per-claim
`context.WithDeadlineCause`, so no polling goroutine per outstanding lease is ever needed — this is
what lets the design scale to the mandated 1,000,000-node target without a goroutine-per-lease
tax. No `Manager` method — `AddNode`, `Claim`, `Subscribe`, or otherwise — ever spawns a goroutine
on the caller's behalf beyond these already-accounted-for, `Close`-awaited internals (ADR-0033's
blocking `Claim` explicitly spawns none at all).

## Consequences

### Positive

- Zero-option construction (`dagworker.New(store)`) is the common case and reads as such; the
  option set can grow for a decade without a breaking change to `New`'s signature, matching
  `grpc.DialOption`'s own long track record.
- `ctx` discipline means no `Manager` or `Subscription` value can outlive its intended validity
  window silently — every I/O-capable call states its own deadline/cancellation explicitly.
- A caller can write one `errors.Is` check per failure mode across the entire library regardless of
  which storage backend is behind it, because every backend-specific error is normalized to this
  taxonomy before crossing the public boundary.
- `Close`'s hard "wait for everything" contract means a host can restart or redeploy without ever
  wondering whether a `Manager`'s background work is still touching shared state after `Close`
  returns — the exact guarantee an embedded, long-lived library must give.

### Negative

- The opaque `Option` interface costs testability compared to a plain struct: unit tests inside the
  module must reach for an unexported `options`-struct-plus-`.apply()` pattern to assert which
  options were applied, rather than comparing struct literals directly (08 §4.1's own
  counterargument, accepted as the price of the compatibility guarantee).
- A large, closed sentinel-error set is itself a compatibility surface — adding a genuinely new
  error condition later means either reusing an imprecise existing sentinel or accepting that a new
  one is a minor-version addition callers' `errors.Is` switches must be updated to handle.
- `Close`'s bounded-wait-via-`ctx` exception is the one place this library's own "no stored,
  ambient context" discipline bends slightly, and it must be documented clearly enough that
  embedders do not generalize the exception to any other method.

### Neutral

- This ADR fixes the *shape* of options/context/errors/shutdown; it does not enumerate every
  `With*` option or every sentinel the mature library will eventually carry (ADR-0026, ADR-0029/§9
  and sibling ADRs add specific ones over time within this shape).

## Alternatives considered

**A plain exported `Config` struct passed to `New`.** Rejected per Google's own decision table:
most callers here pass zero options, and a struct's zero value cannot distinguish "unset, use
default" from "explicitly zero" for fields like `SubscriberBufferSize` — a real, not theoretical,
bug magnet that functional options avoid by construction (08 §4.1).

**Pike's original self-referential/undo functional options** (`type option func(*Foo) option`).
Rejected: nothing about `Manager` configuration is meant to be changed and rolled back at runtime —
every option here is construction-time-only — so the added complexity of an undo-capable option
buys nothing (08 §4.1).

**A logger/tracer/context-value smuggled through `context.Context`.** Rejected per the Go blog's
own dedicated post on this exact temptation: values threaded through `context` for convenience
defeat static analysis of what a function actually depends on, and this library's `WithLogger`
option already gives every embedder an explicit, discoverable way to configure logging without it
(08 §5, §12.1).

**Fire-and-forget `Close` (signal, don't wait).** Rejected per Cheney/Uber's converged guidance (08
§9.1, §9.5): a `Close` that returns before its goroutines have actually stopped touching shared
state is exactly the race a library embedded for years cannot introduce — a caller that immediately
reuses or garbage-collects state a not-yet-stopped goroutine still touches is a bug this design
must foreclose structurally, not by convention.

## References

- docs/research/08-go-api-and-concurrency-design.md §2, §4.1–§4.2, §5, §6.1–§6.2, §9.1–§9.5
- docs/research/00-synthesis.md §3 (ADR-27 seed), §4 (public API surface)
- Pike, "Self-referential functions and the design of options" — https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html
- Cheney, "Functional options for friendly APIs" — https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
- Go blog, "Contexts and structs" — https://go.dev/blog/context-and-structs
- `context.AfterFunc` — https://pkg.go.dev/context#AfterFunc
- `sync.WaitGroup.Go` — https://pkg.go.dev/sync#WaitGroup.Go
