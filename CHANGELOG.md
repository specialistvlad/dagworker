# Changelog

All notable changes are recorded here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below 1.0, a minor bump may contain a breaking change; the
entry will say so.

## [Unreleased]

### Added

- PostgreSQL backend (`storage/postgres`): `SELECT ... FOR UPDATE SKIP LOCKED`
  claim in one statement, `clock_timestamp()` deadlines, a durable event outbox
  with `LISTEN`/`NOTIFY` as a latency hint, and autovacuum and fillfactor
  settings shipped in the migration rather than left as advice.
- Redis backend (`storage/redis`): one Lua script per mutating operation, a
  `{scope}` hash tag on every key so multi-key scripts stay Cluster-legal,
  priority-then-FIFO ready ZSETs, a deadline ZSET for sweeping, and Streams for
  the durable event feed.
- `test/e2e`: the whole stack under test against all three backends, including
  two instances competing for one graph with no coordinator, and an instance
  killed mid-job whose work the survivor recovers.
- `test/perf`: complexity guards and benchmarks, verified at 1,000,000 nodes.
- gRPC adapter (`adapters/grpc`): Temporal-shaped unary long-poll dispatch, an
  etcd-shaped Watch stream with a resume cursor, committed generated code, buf
  lint and breaking-change configuration, and a reference client that owns the
  heartbeat loop.
- HTTP/JSON adapter (`adapters/http`): Consul-style blocking claim, Server-Sent
  Events whose `id:` is the resume cursor, RFC 9457 problem details, keyset
  pagination, and a hand-written OpenAPI 3.1 document.
- `cmd/dagworkerd`: the optional daemon that hosts both adapters, with layered
  configuration, secrets as file paths, a separate admin listener for health,
  readiness, metrics and flag-gated pprof, and an ordered graceful shutdown
  that leaves in-flight leases alone so a rolling restart does not revoke every
  lease in the fleet.
- Strict `golangci-lint` v2 configuration; every module reports zero issues.

### Changed

- `Manager.Nack` returns an `AttemptResult` saying whether the failure became
  another attempt and when it is due. It previously computed that and discarded
  it, forcing callers into a second, unfenced read that can describe a different
  worker's attempt.

### Fixed

- The conformance suite made four assumptions that held only for a backend
  driving a fake clock: an unset `MinLeaseTimeout` silently inflating every
  test lease to the one-second floor, `SetScopeConfig` discarding the baseline
  because it replaces the whole struct, a one-nanosecond retry backoff that is
  not representable on a backend storing milliseconds, and a reclaim test that
  over-specified when a reclaimed node becomes claimable again.
- `Subscription.finish` could close the event channel while `offer` was sending
  on it. Closing a channel another goroutine sends to panics.
- `memory.WithClock(nil)` stored a nil clock and panicked on the first lease.
- `AddNodes` settled nodes in map-iteration order, making equal-priority FIFO
  ordering random.
- `WaitForWork` parked forever on a scope that did not exist yet, sleeping
  through the work it was waiting for.
- PostgreSQL updated the scope's aggregate counter row once per node, so a
  500-node batch made 500 tuple versions of one row and paid O(N^2) in chain
  traversal. Deltas are now accumulated and written once per transaction.
- PostgreSQL bound `phase` as a parameter in the sweep and doorbell queries, so
  once the planner switched from a custom plan to a generic one it could no
  longer prove the partial indexes applied and fell back to sequential scans.
  Claim went from 11.3ms to 1.83ms at 20,000 nodes.
- PostgreSQL had no index for retention collection (added in migration 0002).

## [0.0.1] - 2026-08-23

The first tagged version. A pre-release anchor so the backend modules can
require a real version of the core module rather than carrying a replace
directive; not intended for use.

### Added

- The frozen public vocabulary: a four-value `Status`, a closed `Reason`, and an
  internal `Phase` that carries no compatibility promise.
- `Store`, the storage port, with its atomicity and clock-ownership contract,
  plus the optional `Lister`, `Doorbell`, `DurableEventStream` and `Collector`
  facets discovered by type assertion.
- `Manager`: graph mutation, the blocking-claim wakeup protocol, fenced
  `Ack`/`Nack`/`Skip`/`Extend`, subscriptions with a non-blocking fan-out, and
  the maintenance loops.
- The in-memory reference backend, with Pearce–Kelly incremental topological
  ordering for constant-time cycle rejection in the common case.
- `dagstoretest.RunConformance`, the suite that defines correct backend
  behaviour, plus `FakeClock`.
- `test/perf`: complexity guards asserting that no operation's cost grows with
  the size of the graph, verified at a million nodes.
- 41 architecture decision records and 15 primary-source research dossiers.

[Unreleased]: https://github.com/specialistvlad/dagworker/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/specialistvlad/dagworker/releases/tag/v0.0.1
