# ADR-0044: Design decisions that were not implemented, and why

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0014, ADR-0016, ADR-0033, ADR-0040; `docs/spec/01-contract.md`
- **Backing research:** none new — this ADR records the gap between the earlier ADRs and the code

## Context

An adversarial review of this repository found that the design documents
repeatedly assert things about the shipped code that are not true. Not
disagreements about interpretation: an ADR requiring a metric that does not
exist, a contract naming interfaces and error sentinels that were never
written, a testing strategy describing four tools none of which are in `go.mod`.

Every one of those is worse than having written nothing. A reader trusts an ADR
precisely because it is specific, so a specific claim that is false spends that
trust on a lie, and the reader who checks one and finds it wrong has no reason
to believe the forty that are right.

Three responses were possible: implement the missing things, delete the claims,
or record the gap. Deleting is dishonest about the project's own history — the
reasoning in those ADRs is often still good, and the fact that it did not
survive contact with the implementation is itself the interesting part. So the
claims stay where they are, each ADR is marked amended, and this document is
the single place that says what is actually true.

## Decision

**Each item below is a claim in an earlier document that the code does not
implement.** Where a claim was fixed rather than recorded, it is not here — it
is in the commit that fixed it.

### 1. `topo_fastpath_hit_ratio`, and metrics generally (contract §6.2)

**Claimed:** the library MUST export a `topo_fastpath_hit_ratio` metric.

**Reality:** the library exports no metrics and has no metrics interface.

**Why it stays unbuilt:** a metrics interface in the core module is the same
argument as a logger in the core module, and this project already answered it —
the host owns observability, the library owns behaviour. What the host can
already read is `ScopeStats`, which is a store-level count rather than a
counter this library maintains, so it cannot drift. A metrics facet, alongside
`Lister` and `Collector`, is the right shape if one is added; it is a v0.5
question, not something to bolt onto the topological sort in particular.

### 2. `PartitionAssigner` (ADR-0014, contract §8)

**Claimed:** an internal `PartitionAssigner` interface exists from the first
commit with a trivial `P=1` implementation, so the v0.5 upgrade to jump
consistent hash + HRW is an internal refactor.

**Reality:** no such interface exists.

**Why it stays unbuilt:** what ADR-0014 was protecting is intact, and it is not
the interface. **No public signature mentions a partition**, so introducing one
later is still an internal change — which was the entire guarantee. The
placeholder would have been an abstraction with exactly one implementation and
no second caller to justify its shape, and a v0.5 that actually needs
partitioning will discover the right shape from the real second implementation,
not from a `P=1` stub written a year earlier. ADR-0039 rejects speculative
interfaces by name.

### 3. The counting-signal doorbell (ADR-0033)

**Claimed:** the doorbell is a counting signal, not a broadcast, so waking N
blocked claimers for one readied node costs O(readied nodes) rather than
O(waiters).

**Reality:** all three backends broadcast. Memory closes and replaces a
`chan struct{}`; Redis publishes to a channel; PostgreSQL uses `NOTIFY`. Every
blocked claimer in the scope wakes and races.

**Why it stays unbuilt:** the ADR's cost analysis is right and its conclusion
did not survive the storage port. A counting signal must be *shared* by every
Manager instance on the same storage to mean anything, and neither Redis pub/sub
nor `LISTEN/NOTIFY` has counting semantics — implementing one means a second
piece of coordinated state next to the ready set, kept consistent with it, with
its own reclaim path when a woken claimer dies before claiming. That is a
distributed counter guarding an operation that is already atomic and already
idempotent under a lost race.

The realised cost is also smaller than the ADR assumed, because a wasted wakeup
is one `Claim` against a backend that answers "nothing ready" from an index
lookup, not a scan. Where it is not smaller — a very wide fleet on one scope —
the mitigation available today is a bounded claim batch (`MaxClaimBatch`), so
one wakeup does more work. This is a real deviation and a real trade, not an
oversight; it is recorded here so a future contributor measuring a thundering
herd knows the counting design was considered and why it was dropped.

### 4. The testing strategy (ADR-0040)

**Claimed:** `testing/synctest` for timeout tests, `pgregory.net/rapid` for
model-based property tests, a seeded chaos harness at `internal/chaos`, and a
Porcupine linearizability check.

**Reality:** none of the four exist. `go.mod` has no test dependencies at all.

**What was built instead**, and what each of the four was for:

| Planned | Shipped | Verdict |
|---|---|---|
| `synctest` for deadline tests | `dagstoretest.FakeClock`, an injected `Clock` every backend honours | **Better.** `synctest` controls one process's timers; a lease deadline is evaluated by *the storage's* clock (ADR-0008), which is a different clock in two of three backends. An injectable clock tests the thing that actually decides. |
| `rapid` model-based properties | an 81-case conformance suite every backend runs | **Different, and mostly sufficient.** The conformance suite is enumerated rather than generated, so it cannot find an interleaving nobody thought of — a real loss, honestly stated. It does run identically against three backends, which is the property that has actually caught bugs. |
| `internal/chaos` seeded fault harness | `faultStore`, a fault-injecting `Store` wrapper in `fault_test.go` | **Same idea, less machinery.** It injects errors at chosen call sites rather than randomly at a seed. Less thorough; no external dependency and no reproducibility contract to maintain. |
| Porcupine linearizability | nothing | **Unmet.** The concurrency claims rest on `-race -shuffle` under parallel tests and on the conformance suite's own concurrent cases. That is weaker than a linearizability check and this ADR does not pretend otherwise. |

ADR-0040's own "Consequences" section anticipated this: it names the four-tier
apparatus as "real upfront cost" and flags Porcupine as deferrable. Three of
four were deferred and one was replaced with something better suited to a
storage-agnostic library.

### 5. Facets that were named but not built (contract §12)

**Claimed:** optional facets `Lister`, `DurableEventStream`, `ConditionalDeleter`,
`BatchClaim`, declined via an `ErrCapability` sentinel.

**Reality:** the facets are `Lister`, `DurableEventStream`, `Doorbell`, and
`Collector`; declining is `ErrUnsupported`.

**Why:** conditional deletion turned out to be expressible through
`RemoveNode`'s cascade policy, and batch claiming is mandatory rather than
optional — `Claim` takes a `Max` and every backend implements it, so a facet
for it would have had no non-implementer. `Doorbell` and `Collector` were
discovered during implementation and are genuinely optional: a backend can
correctly decline either.

### 6. `ErrLeaseExpired`

**Claimed** (by its own doc comment): distinct from a lease mismatch.

**Reality:** nothing returns it. A deadline is enforced at reclaim time, not on
the acknowledgement — a worker that finishes late, before anyone has taken the
node away, has its result accepted. Once reclaimed, the epoch has moved and the
same acknowledgement is `ErrLeaseMismatch`.

That behaviour is deliberate: the deadline exists to make a dead worker's node
claimable again, not to discard work that was genuinely done. It is now pinned
by `T-LATE-ACK-IS-ACCEPTED-UNTIL-RECLAIMED` so it is a decision rather than an
accident, and the sentinel is kept because both adapters map it and a backend
that does enforce the deadline directly has somewhere to report it.

### 7. The PostgreSQL `leases` table (contract §12.1)

**Claimed:** an `UNLOGGED` `leases` table, so a revocable grant is not
WAL-durable.

**Reality:** there is no `leases` table. A lease is three columns on the node
row (`epoch`, `deadline`, `worker`), which is what makes a claim one write
rather than two rows in one transaction (ADR-0007). Those columns are as
durable as the node, which is a cost the design accepted in exchange for
atomicity.

## Consequences

- **Every remaining specific claim in the contract and the ADRs has been checked
  against the code.** This list is what did not survive.
- **Three of these are now covered by tests** rather than only by prose:
  the late-ack behaviour, the facet set (via `CapabilityReporter`), and the
  lease holder's observability.
- **Two are open work with a named owner in this document**: a metrics facet,
  and a linearizability check. Neither blocks v1.0; both should be revisited
  before anyone claims this library is verified rather than tested.
- **The counting-signal doorbell is closed, not deferred.** A future
  contributor who measures a thundering herd should read §3 first: the design
  was considered against three real backends and dropped for reasons that a
  benchmark alone will not surface.
