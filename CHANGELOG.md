# Changelog

All notable changes are recorded here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is below 1.0, a minor bump may contain a breaking change; the
entry will say so.

## [Unreleased]

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
