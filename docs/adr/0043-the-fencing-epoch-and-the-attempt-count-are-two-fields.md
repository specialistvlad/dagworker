# ADR-0043: The fencing epoch and the attempt count are two fields

- **Status:** Accepted
- **Date:** 2026-08-23
- **Deciders:** Vladyslav Kazantsev (project owner)
- **Amends:** ADR-0011 (supersedes its central claim), ADR-0006 (adds the epoch floor)
- **Backing research:** docs/research/03-leases-heartbeats-timeouts.md §3.4 (Kleppmann); docs/research/12-dag-semantics-and-state-machine.md §3.4

## Context

ADR-0011 fused two counters into one integer and argued the economy was free:
a node's `attempt` was also its `lease_epoch`, so the retry counter and the
fencing token were the same field. Every backend implemented it that way.

The economy was not free. It holds only while a node identifier is used once.
ADR-0025 makes node identity caller-supplied, and ADR-0036 makes removal a
first-class operation, so a caller may legitimately delete a node and add
another under the same `NodeID` — a re-run of a pipeline step, a scope reused
across builds. Nothing forbids it and nothing should.

The two counters want opposite things across that boundary:

- The **epoch** must never go backwards for an identifier, ever, because a
  worker that has been unreachable across the deletion still holds a lease
  naming `(scope, node, epoch)`. If the recycled node starts again at zero it
  eventually re-issues that epoch, the fenced CAS in `Complete` matches, and a
  worker that was fenced out writes its result over a node it never claimed.
  That is precisely Kleppmann's GC-pause scenario (03 §3.4) — the one ADR-0006
  exists to prevent — reached through the front door.
- The **attempt count** must restart at zero, because to the caller the
  recycled identifier is a new node. Carrying the old count forward means a
  node whose predecessor happened to fail twice is terminal on its first
  failure, with `MaxAttempts` silently already spent.

Fused, there is no value that satisfies both. Seeding the epoch high to close
the fencing hole breaks retries; restarting it to keep retries working leaves
the fencing hole open. `T-FENCE-SURVIVES-RECREATE` in the conformance suite
reproduces the hole on all three backends against the pre-amendment code.

## Decision

**The epoch and the attempt count are separate fields.**

- `epoch` is monotonic **per identifier within a scope, across deletions**. Each
  scope carries an **epoch floor**: deleting a node raises the floor past that
  node's epoch, and a newly created node's epoch starts at the floor. A
  recycled identifier therefore resumes strictly above every epoch its previous
  generations ever held, and a stale lease from a deleted generation can never
  match.
- `attempt` counts claims of **this** node and starts at zero for a newly
  created one, recycled or not. It is what `RetryPolicy.MaxAttempts` is
  compared against and what `AttemptResult.Attempt` reports.
- A claim increments both, independently.

The epoch floor is per scope rather than per identifier deliberately. Per
identifier would need a tombstone that outlives the node it describes, which is
unbounded state whose retention policy nobody can set correctly; a scope-wide
floor costs one integer per scope and is only ever *more* conservative — it
skips epochs, which is free, since nothing about a fencing token requires
density.

### Public surface

`Node.Attempt` keeps its name and meaning as the attempt count, and stops
claiming to be the epoch. The epoch a node's current lease was granted at is
`Inspection.LeaseEpoch`, added by this ADR because it was previously
unreachable without holding the lease — the HTTP adapter's `GET .../leases/{id}`
was reading `Node.Attempt` in its place, correct only until the first recycled
identifier. `Lease.Epoch` is unchanged: a holder always had it.

### Per backend

| Backend | Epoch floor lives in | Set on delete | Read on create |
|---|---|---|---|
| memory | `scope.epochFloor uint64` | `release()` | `create()` |
| postgres | `dagw.scopes.epoch_floor` (migration `0003`) | `deleteNodeRow` | `insertNodeSQL` |
| redis | `EpochFloor` on the scope stats hash | `deleteNodeKeys` via `retireEpoch` | node-creation script |

## Consequences

- **A fenced write is now safe across the full identifier lifecycle**, not only
  within one generation of it. This is the guarantee ADR-0006 was written to
  provide and did not actually provide.
- **Epochs are sparse.** A scope that deletes a node at epoch 900 issues epoch
  901 to the next node created anywhere in that scope. Nothing reads an epoch
  as a count, and `AttemptResult.Attempt` — the number a human sees — is the
  attempt, so the sparseness is invisible outside the fencing comparison.
- **`Outcome.Attempt` no longer equals `Claim.LeaseEpoch`**, which ADR-0011
  asserted. They still agree for any node that has never been deleted, so no
  existing caller observes a change; the equality is simply no longer a promise.
- **One extra integer per scope**, written only on deletion. No hot path grew a
  read: the epoch floor is consulted on node creation, which already writes the
  scope row on every backend.
- **ADR-0011 stands except for the fusion.** Its actual subject — that a retry
  is a new attempt on the same node, never a resurrection of a terminal one,
  and that the CAS fences on the epoch rather than on the status — is unchanged
  and is what the conformance suite still pins.
