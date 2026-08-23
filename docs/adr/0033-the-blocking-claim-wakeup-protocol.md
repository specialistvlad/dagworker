# ADR-0033: The blocking Claim wakeup protocol

- **Status:** Accepted, amended by ADR-0044
- **Date:** 2026-08-22
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** —
- **Amended by:** ADR-0044 §3 — the counting-signal doorbell described below was not built. All three backends broadcast; the reasoning and the trade are recorded there.
- **Backing research:** docs/research/10-event-bus-and-delivery-semantics.md §7.2, §9; docs/research/03-leases-heartbeats-timeouts.md §2.1–§2.2; docs/research/04-postgres-backend.md §3; docs/research/05-redis-backend.md §10; docs/research/08-go-api-and-concurrency-design.md §9

## Context

`Manager.Claim` is specified in the public API (ADR-0027) as blocking "subject to ctx until at
least one ready node is available or ctx is done," in contrast to `TryClaim`, which returns
`ErrScopeEmpty` immediately. The synthesis left the wakeup mechanism implicit. AMD-4 corrects that:
an unspecified blocking implementation is not a detail to leave to whoever writes the code first,
it is a correctness-and-liveness hazard with two failure modes waiting on either side of it — a
naive "sleep and retry" implementation becomes a busy-loop that burns CPU and hammers the storage
backend at whatever poll interval was hand-picked, while a naive "wait on a signal" implementation
without a fallback can hang forever the instant a signal is dropped, which every backend surveyed
admits can happen: Redis pub/sub is fire-and-forget and loses everything sent while a subscriber is
disconnected (05 §10); Postgres `NOTIFY` "does not survive disconnect, and is not itself durable" —
if the listening session is not connected and consuming, the event is gone, full stop (04 §3).

The design must additionally close a well-known race that both `sync.Cond`-based designs and
Linux's `futex(2)` were built to avoid: if a waiter checks "is anything ready?", gets "no," and only
*then* registers to be woken, a producer's signal delivered in the gap between the check and the
registration is lost, and the waiter blocks until the next unrelated event or the poll fallback
saves it — silently degrading every blocking `Claim` to poll-interval latency under exactly the
concurrency conditions where low latency matters most (many workers, few ready nodes).

Finally, per ADR-0019, `Claim` never trusts the doorbell as a source of truth — it is purely a
liveness optimization telling a blocked caller "something may have changed, go re-check storage."
This means the wakeup protocol's own correctness bar is low (a spurious or lost wakeup can never
cause an incorrect claim, only a slower one), but its *liveness* bar is exactly what this ADR must
guarantee precisely, because nothing else in the design will.

## Decision

`Claim` is: **immediate non-blocking attempt → register on the per-`(scope, kind)` doorbell →
second non-blocking attempt → block on {doorbell, jittered poll, ctx} → repeat.** The registration
happens strictly before the second attempt, closing the missed-wakeup gap: any signal delivered
during or after the second attempt is observed by an already-registered waiter, never by one that
registers afterward.

```go
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (*Claim, error) {
	cfg := resolveClaimOptions(m.cfg, opts)

	// 1. Immediate non-blocking attempt — the fast path. Most calls to Claim
	// against a scope with steady work return here without touching the
	// doorbell or a timer at all.
	if c, err := m.tryClaimOnce(ctx, scope, cfg); err == nil {
		return c, nil
	} else if !errors.Is(err, ErrScopeEmpty) {
		return nil, err
	}

	for {
		// 2. Register BEFORE the second attempt. A Complete/Sweep call that
		// readies a node between our first attempt (above, or the previous
		// loop iteration's attempt below) and this Register call is caught
		// by attempt #3 immediately below, not lost.
		waiter := m.doorbell.Register(scope, cfg.Kind)

		// 3. Second (or Nth) attempt, now that a signal cannot be missed.
		if c, err := m.tryClaimOnce(ctx, scope, cfg); err == nil {
			waiter.Cancel()
			return c, nil
		} else if !errors.Is(err, ErrScopeEmpty) {
			waiter.Cancel()
			return nil, err
		}

		// 4. Block on: a real signal, the jittered poll fallback, or ctx.
		delay := fullJitter(cfg.pollBase, cfg.pollCap) // [pollBase, pollCap]
		timerC, stopTimer := m.clock.NewTimer(delay)
		select {
		case <-waiter.C(): // real doorbell signal (or a stale/spurious one)
		case <-timerC: // poll fallback — covers a lost/never-arriving signal
		case <-ctx.Done():
			stopTimer()
			waiter.Cancel()
			return nil, ctx.Err()
		}
		stopTimer()
		waiter.Cancel() // always unregister before looping — never accumulate
	}
}
```

**No goroutine is ever spawned by a `Claim` call.** `waiter := m.doorbell.Register(...)` is a pure
data-structure operation (append a channel to a per-`(scope, kind)` slice under a striped mutex,
matching the sharding discipline of ADR-0028) returning a value with a `C() <-chan struct{}` and a
`Cancel()` that removes it — the entire blocking loop runs in the calling goroutine's own stack via
`select`. `ctx.Done()` unwinds by calling `stopTimer()` and `waiter.Cancel()` and returning; both
are O(1), neither can block, so cancellation is always prompt and leaves nothing behind. This
matches ADR-0027 §9's ownership rule: a caller's own goroutine, not one the library started on its
behalf, is what's blocked, and it owns its own exit path.

**Poll interval bounds.** `pollBase` defaults to `100ms`, `pollCap` defaults to `3s`, both settable
via `ClaimOption` (`WithPollInterval(base, cap time.Duration)`) for hosts that know their
backend's doorbell reliability characteristics. Each cycle redraws a Full Jitter delay
(`random(0, min(pollCap, pollBase))`, reusing ADR-0012's formula at a fixed exponent rather than a
growing one — this is a liveness floor, not a failing-dependency backoff, so there is no reason to
grow the interval the longer a caller waits) independently per blocked caller, so N blocked callers
never synchronize their poll attempts against each other.

**Thundering herd.** When many `Claim` calls are blocked on the same `(scope, kind)` and exactly one
node becomes ready, waking every registered waiter would cause N−1 wasted round trips to the
storage backend for the single node that exactly one of them will actually win — expensive at
scale on Redis/Postgres, where each wasted attempt is a real network round trip and a real
(losing) atomic claim attempt. The doorbell is therefore a **counting signal, not a broadcast**:

```go
// Signal wakes at most n waiters — n being the number of nodes the write
// that just committed actually readied (the len of Complete/Sweep's
// returned []NodeRef, AMD-2) — never every registered waiter regardless of
// how many nodes just became ready.
func (d *doorbell) Signal(scope Scope, kind string, n int) {
	d.mu.Lock()
	q := d.waiters[key{scope, kind}]
	for i := 0; i < n && len(q) > 0; i++ {
		q[0].fire() // non-blocking send on a cap-1 channel; never panics if already fired
		q = q[1:]
	}
	d.waiters[key{scope, kind}] = q
	d.mu.Unlock()
}
```

`Complete` and `Sweep` (AMD-2) call `Signal(scope, kind, len(readied))` once per backend commit,
grouped by each readied node's `Kind`. A woken waiter that loses the race anyway (a concurrent
`TryClaim`, or — in a multi-instance deployment — a different process instance won the atomic
claim first) simply re-enters the loop: it re-registers and re-attempts, costing at most one extra
round trip, never a stampede, and the doorbell's own FIFO ordering means the *next* waiter in line
gets the next `Signal`, so no single blocked caller is starved indefinitely under sustained
contention.

**Per-backend doorbell mechanics** (all sit behind the identical `doorbell.Register`/`Signal`
internal interface above — only how `Signal` gets invoked across process boundaries differs):

- **In-memory.** The doorbell is the construct shown above, called directly under the same shard
  lock as the `Complete`/`Sweep` write. Wakeup latency is sub-microsecond; the poll fallback is a
  pure correctness safety net here and never fires in practice.
- **Redis.** A per-`(scope, kind)` pub/sub channel (`PUBLISH scope:{s}:ready:{kind} <n>`), fired by
  the same Lua Function that performs the fenced `Complete`/`Sweep` write (05 §10, 10 §5.4), fanned
  into `doorbell.Signal` by one `SUBSCRIBE`-holding goroutine per `Manager` (a `Manager`-lifetime
  goroutine per ADR-0027 §9, not a per-`Claim` one). Pub/sub loses everything sent while
  disconnected (05 §10), which is exactly why the poll fallback is load-bearing, not cosmetic, here.
- **PostgreSQL.** `LISTEN scope_{s}_ready_{kind}`, `NOTIFY`'d by the same trigger that performs the
  durable event write (04 §3), fanned into `doorbell.Signal` by one `pgx`-pool listener goroutine per
  `Manager`. `NOTIFY` "does not survive disconnect, and is not itself durable" (04 §3): a dropped
  listener connection silently loses every notification queued during the gap, with no error
  surfaced to waiting `Claim` calls. Two required mitigations: (a) on reconnect, the listener treats
  the gap as "possibly missed" and wakes every currently registered waiter for the scopes/kinds it
  was listening to, rather than resuming silently; (b) the jittered poll is the real liveness bound
  regardless — `pollCap` is the hard ceiling on how long a Postgres-backed `Claim` can be stuck
  behind a lost `NOTIFY`, independent of how long reconnection itself takes.

## Consequences

### Positive

- Liveness is bounded and provable: worst-case wake latency for any blocked `Claim` is `pollCap`
  (default 3s), regardless of backend or doorbell failure mode, because the poll fallback never
  depends on the doorbell having worked.
- The missed-wakeup race is closed by construction (register-before-second-attempt), not by a
  documented caveat — every `Claim` implementation that follows this ADR is correct by the shape
  of the code, not by careful discipline the reviewer has to remember to check for.
- Zero goroutines per blocked caller, at any scale: 100,000 concurrently blocked `Claim` calls cost
  100,000 blocked goroutines the *caller* already owns (their own call stacks) plus O(1) doorbell
  registry entries each — no library-side goroutine pool to size or leak.
- The counting-signal doorbell bounds the thundering-herd cost to O(readied nodes), not O(blocked
  waiters), which is the only version of this design that scales to the "many small scopes with
  many idle blocked workers" shape AMD-6 requires the library to serve well.

### Negative

- A large population of blocked callers against a genuinely idle scope still generates a steady
  background poll load — at `pollCap = 3s` and 10,000 idle blocked workers, that is on the order of
  ~3,300 non-blocking `Claim` attempts per second even when the doorbell works perfectly, purely
  from Full Jitter's spread. This is a deliberate, tunable cost (`WithPollInterval`), sized by the
  operator for their idle-worker count, not an oversight.
- Three backends means three distinct fan-in mechanisms (direct call, pub/sub-fed goroutine,
  `LISTEN`-fed goroutine) sharing one internal `doorbell` contract — more moving parts under the
  conformance suite (ADR-0018) than a single, backend-agnostic polling-only design would have had.
- The Postgres reconnect-wakes-everyone mitigation trades a small burst of redundant `Claim`
  attempts for closing the silent-gap window faster than `pollCap` alone would; the alternative
  (relying on `pollCap` alone after every reconnect) costs a full extra `pollCap` of latency per
  blocked caller on every connection blip, which compounds badly if reconnects are frequent.

### Neutral

- This ADR governs only the wakeup *mechanism*. Which nodes are eligible, how the atomic claim
  itself is performed, and what `ClaimOptions{Kind, LeaseTimeout, MaxNodes}` mean are ADR-0007's
  and ADR-0019's territory; this ADR assumes `tryClaimOnce` (== `TryClaim`'s non-blocking body) is
  already correct and only adds the loop, registration, and fallback around it.
- `WithPollInterval` is a `ClaimOption`, not a `Manager`-level `Option` — different scopes served by
  the same `Manager` may reasonably want different poll floors (a low-traffic scope vs. a hot one),
  consistent with AMD-6's per-scope-configuration stance.

## Alternatives considered

**Pure poll, no doorbell at all.** Rejected: forces every blocked `Claim` to pay the full
`pollCap` latency on every wakeup, all the time, rather than only during the (rare, bounded) gaps a
doorbell can have — throwing away the common case's near-zero latency for implementation
simplicity the doorbell design above does not actually cost much to build.

**Pure doorbell, no poll fallback ("wait on signal, trust it").** Rejected outright per AMD-4 and
per 04 §3 / 05 §10's own documented loss behavior: Postgres `NOTIFY` and Redis pub/sub both
provably lose notifications across a disconnect, with no delivery guarantee and no error signaled
to the waiter — a design with no fallback can hang a blocked `Claim` indefinitely on exactly the
kind of transient network blip this library must tolerate by design (it is meant to run for years
embedded in a host process).

**Broadcast-wake-all on every readiness signal** (a `sync.Cond.Broadcast()`-shaped design, or a
Redis/Postgres doorbell that always wakes every listener regardless of how many nodes just became
ready). Rejected: this is precisely the thundering-herd cost this ADR's counting-signal design
exists to avoid — under high contention (many blocked workers, low throughput of newly-ready
nodes), broadcast wastes O(waiters) round trips per event instead of O(readied nodes), and that
waste is paid on every single completion in the scope, not just occasionally.

**Exponentially growing poll interval** (classic client backoff shape, reusing ADR-0012's growing
formula instead of a fixed jittered band). Rejected: ADR-0012's growth exists to protect a
struggling dependency from a retry storm after a failure; here there is no failing dependency to
protect — the doorbell not having fired yet says nothing about whether the storage backend is
healthy — so growing the interval would only slow down recovery from a doorbell miss the longer a
caller has already been waiting, which is the wrong direction for a liveness fallback.

## References

- docs/research/10-event-bus-and-delivery-semantics.md §7.2, §9.1–§9.2
- docs/research/03-leases-heartbeats-timeouts.md §2.1–§2.2 (SQS/Pub-Sub long-poll precedent)
- docs/research/04-postgres-backend.md §3 (`LISTEN`/`NOTIFY` disconnect-loses-everything)
- docs/research/05-redis-backend.md §10 (pub/sub fire-and-forget, loss-on-disconnect)
- docs/research/08-go-api-and-concurrency-design.md §9.1–§9.3 (goroutine ownership, `context.AfterFunc`)
- docs/research/00-synthesis.md AMD-4 (owner amendment), §4 (`Claim`/`TryClaim`)
- PostgreSQL `NOTIFY` docs — https://www.postgresql.org/docs/current/sql-notify.html
- Redis keyspace notifications / pub-sub loss — https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/
