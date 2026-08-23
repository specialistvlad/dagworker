// Package dagworker manages a dynamic directed acyclic graph of work items and
// hands ready items to external workers.
//
// The library owns the graph, the readiness computation, the lease and timeout
// protocol, and the pluggable storage. The host program owns the workers.
//
// # Model
//
// A [Node] is one unit of work, identified by a [Scope] and a [NodeID]. An edge
// from A to B means B must not run until A resolves. Nodes and edges may be
// added while the graph is running; an edge that would create a cycle is
// rejected at insert time with [ErrCycle].
//
// A node's public [Status] is one of exactly four values — [StatusNew],
// [StatusInProgress], [StatusSuccess], [StatusError] — and that vocabulary is
// frozen. Why a node reached a terminal status is a separate closed [Reason]:
// a timeout is an error with [ReasonTimeout], never a fifth status.
//
// # Claiming work
//
// A worker calls [Manager.Claim] to take a ready node. The claim grants a
// lease: a deadline plus a monotonically increasing epoch that acts as a
// fencing token. The worker reports the outcome with [Manager.Ack] or
// [Manager.Nack], presenting the lease it was handed. A worker that does not
// answer before the deadline loses the node, which is failed with
// [ReasonTimeout] or re-queued for another attempt. Long work extends its own
// deadline with [Manager.Extend].
//
// Every write after a claim is a compare-and-swap on the lease epoch, so a
// worker that was merely paused — not dead — cannot corrupt state after being
// superseded. Delivery is at-least-once; the accepted effect is at-most-once
// per lease epoch. Exactly-once delivery to an external process is not
// possible and is not offered.
//
// # Observing
//
// [Manager.Subscribe] returns a stream of status transitions. It is an
// observation feed, deliberately separate from claiming work: correctness never
// depends on an event arriving. Readiness is always re-derivable by querying
// storage, so a missed or duplicated signal costs latency, never correctness.
//
// # Storage
//
// The in-memory backend is the default and is shared by every worker in the
// process. Redis and PostgreSQL backends live in their own modules so importing
// this package pulls in no database driver. Multiple Manager instances in
// different processes may share one backend; work is distributed by pull-based
// competition on the backend's own atomic claim primitive.
//
// # Scopes
//
// A [Scope] is a namespace and the unit of isolation, configuration, completion
// and retention. Scopes are created implicitly on first write. An edge never
// crosses a scope boundary.
//
// # Trust
//
// Workers are assumed cooperative: operated by the same team as the Manager
// instances. The fencing token is a plain integer, so a malicious worker could
// forge one and steal a node it does not hold. It cannot corrupt graph
// structure, cross a scope boundary, or exceed the payload cap.
package dagworker
