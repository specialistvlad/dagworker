# 13 — The gRPC Worker Protocol

Status: research dossier. Scope: the wire protocol between a remote worker (Python, Node,
Rust, Java, or Go) and `dagworkerd`, the optional daemon that embeds the `dagworker` core
library and exposes it over gRPC and HTTP/JSON. The core library (`github.com/specialistvlad/dagworker`)
imports none of this; `dagworkerd` and a generated client SDK are the only things that do.

The central question: **how does a remote worker get told "here is a node, you have N
seconds"?** Three shapes exist in production systems. This dossier studies all three with
real proto and real failure modes, picks one (a hybrid, but a narrow one), and specifies the
full protocol.

---

## Decision, up front

**Primary dispatch is a unary long-poll RPC (`ClaimNode`), exactly Temporal's
`PollActivityTaskQueue` shape.** Heartbeat, Complete, and Fail are unary RPCs keyed by an
opaque, server-issued `task_token` — again exactly Temporal's shape. A worker's *capacity* is
expressed the same way Temporal expresses it: by how many concurrent `ClaimNode` calls it has
in flight, not by an explicit credit message. **Status/availability notification is a
separate, independent bidirectional stream (`Watch`)** modeled on etcd's `Watch` RPC, used
only to wake idle pollers and to give operators/schedulers a live event feed — it never
carries the lease itself. No general-purpose "push tasks down a stream" RPC exists in this
design; §3 explains why that shape is the wrong tool for *this* problem even though it is the
right tool for others (xDS, CRI's exec/attach).

This is the hybrid the assignment permits, but it is a narrow one: exactly two RPC shapes
(long-poll unary, bidi-stream-for-events), not three, and no bespoke credit protocol — HTTP/2
already gives dagworker one for free (§4).

---

## 1. Long poll — the primary dispatch mechanism

### 1.1 Temporal: the single best piece of prior art here

Temporal's `WorkflowService` is a single gRPC service with ~80 RPCs; the ones that matter for
worker dispatch are five, and their shapes are worth internalizing field-by-field
([`workflowservice/v1/service.proto`](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/service.proto)):

```
rpc PollWorkflowTaskQueue (PollWorkflowTaskQueueRequest) returns (PollWorkflowTaskQueueResponse)
rpc PollActivityTaskQueue (PollActivityTaskQueueRequest) returns (PollActivityTaskQueueResponse)
rpc RecordActivityTaskHeartbeat (RecordActivityTaskHeartbeatRequest) returns (RecordActivityTaskHeartbeatResponse)
rpc RespondActivityTaskCompleted (RespondActivityTaskCompletedRequest) returns (RespondActivityTaskCompletedResponse)
rpc RespondActivityTaskFailed (RespondActivityTaskFailedRequest) returns (RespondActivityTaskFailedResponse)
```

`PollActivityTaskQueue` is a **unary RPC that blocks server-side** until a task exists or an
internal poll timeout elapses; the worker's SDK runs a pool of goroutines each holding one
call open, so *N concurrent poll calls = N units of consumption capacity* — there is no
separate "I have capacity for K" message, because the poll call itself *is* the capacity
signal. Not exposed over HTTP, gRPC-only
([source](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/service.proto)).

The response carries a **task token** — the single artifact worth studying hardest:

```protobuf
message PollActivityTaskQueueResponse {
  bytes task_token = 1;                                    // "A unique identifier for this task"
  ...
  google.protobuf.Duration schedule_to_close_timeout = 14; // first scheduled -> final result, total budget
  google.protobuf.Duration start_to_close_timeout = 15;    // this attempt's budget
  google.protobuf.Duration heartbeat_timeout = 16;         // window within which a heartbeat is required
}
```
([source](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/request_response.proto))

`task_token` is **opaque bytes to the client** but internally encodes enough for the server to
validate every later call against it: which workflow/activity/attempt it belongs to, and
implicitly a fencing generation. Every one of `RecordActivityTaskHeartbeat`,
`RespondActivityTaskCompleted`, and `RespondActivityTaskFailed` takes the *same*
`task_token` as field 1 and nothing else identifies the unit of work
([source](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/request_response.proto)).
That is precisely a fencing token in the classic distributed-systems sense (Martin Kleppmann's
"How to do distributed locking" pattern): whoever presents the *current* token wins; a stale
token from a timed-out attempt is rejected. Temporal's server documents the rejection
explicitly: `RespondActivityTaskCompleted` and `RespondActivityTaskFailed` **fail with
`NotFound` if the task token is invalid due to timeout, prior completion, or non-existence**
([source](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/service.proto)) —
collapsing three distinct causes (expired lease, already-acked, garbage token) into one gRPC
code is a deliberate simplification worth copying (§9) precisely *because* task tokens are
meant to be single-use and short-lived, not because `NotFound` is the only defensible choice.

Three independent timeout knobs ride alongside the token
([Temporal docs](https://docs.temporal.io/encyclopedia/detecting-activity-failures)):
`ScheduleToStartTimeout` (queue wait), `StartToCloseTimeout` (this attempt), `ScheduleToCloseTimeout`
(all attempts combined), plus `HeartbeatTimeout` as a *liveness* check independent of the
others. This is the direct ancestor of dagworker's "lease timeout" plus "no ack before the
deadline ⇒ timeout failure, reclaimable" rule from the project brief — Temporal just names the
lease components separately instead of folding them into one number, and dagworker should too
(§6 keeps `lease_expires_at` and the `ClaimNode` RPC deadline strictly separate for the same
reason).

`RecordActivityTaskHeartbeat`'s response carries `cancel_requested: bool`
([source](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/request_response.proto)) —
the heartbeat is a full duplex channel, not just a liveness ping: the server piggybacks
cancellation onto the very same call the worker was already making. dagworker's `ExtendLease`
response reuses this exact idea (§5).

### 1.2 Nomad: the same shape over HTTP, with an explicit ceiling

Nomad's blocking queries are the same long-poll idea over plain HTTP: a client sends
`?index=<X>&wait=<duration>`, the server hangs the response until the resource's index moves
past `X` or `wait` elapses (**capped at 10 minutes**, defaulting to 5), and every response
carries `X-Nomad-Index` for the next call
([Nomad HTTP API docs](https://developer.hashicorp.com/nomad/api-docs#blocking-queries)).
Nomad's own docs warn that a blocking call returning is **"no guarantee of a change"** — the
timeout and an idempotent write both look identical to the client, so callers must always
re-diff on the returned state rather than assume "it returned, so something happened." This is
the same shape dagworker's `ClaimNode` must have: a timed-out poll returns a normal, successful
`ClaimNodeResponse` with no lease set, not an error — errors are for exceptional cases,
"nothing was ready yet" is not exceptional.

### 1.3 Buildbarn: long poll with a *server-scheduled* resync — the closest sibling design

Buildbarn's worker↔scheduler protocol is a single RPC, `Synchronize`, and it is the most
direct precedent for dagworker of the three because it dispatches arbitrary opaque work units
(build actions) rather than a fixed workflow/activity model:

```protobuf
service OperationQueue {
  rpc Synchronize(SynchronizeRequest) returns (SynchronizeResponse);
}
```
([source](https://github.com/buildbarn/bb-remote-execution/blob/master/pkg/proto/remoteworker/remoteworker.proto))

A worker calls `Synchronize` repeatedly, reporting its `CurrentState` (idle, or executing a
specific action digest at a specific phase — fetching inputs / running / uploading outputs)
and receiving a `DesiredState` back (stay idle, or here is an action to run). The genuinely
interesting piece: the response also carries `next_synchronization_at`, an absolute timestamp
telling the worker exactly when to call back next — **the scheduler controls the polling
cadence, not a client-side constant**. The call is documented to be non-blocking while
executing (the worker is just checking in) but the scheduler is explicitly allowed to **"let
the call hang until more work is available"** when the worker reports idle
([source](https://github.com/buildbarn/bb-remote-execution/blob/master/pkg/proto/remoteworker/remoteworker.proto)).
This is a strict superset of Temporal's model: same long-poll-when-idle behavior, plus a
server-driven backoff/resync schedule that a fixed client-side poll-timeout constant can't
express (useful if dagworkerd ever wants to tell a worker "stop hammering me, come back in
30s" without returning an error). §11 recommends dagworker's `ClaimNodeResponse` carry an
optional `retry_after` for exactly this, borrowed from Buildbarn rather than invented fresh.

### 1.4 What long poll is *not* good at

All three systems above accept the same cost: a blocked RPC occupies one HTTP/2 stream (cheap,
~tens of bytes of local state) but, more importantly, occupies one *goroutine/thread and one
logical connection slot* on both sides for the poll's duration, and interacts badly with
infrastructure that assumes short-lived unary calls (idle-connection-killing L4 load balancers,
§10; aggressive client keepalive misconfiguration, §7). None of that is disqualifying — it is
exactly why §7 and §10 exist — but it is why long poll alone is not the whole answer for
*event* delivery: a client watching for DAG-wide transitions (not "give me one claimable node")
would need dozens of outstanding long polls (one per interesting node) or one enormous
multiplexed poll that reinvents streaming badly. That job is handed to `Watch` (§3, §6)
instead.

---

## 2. Server-streaming dispatch — the flow-control trap

The naive alternative: `rpc StreamNodes(StreamNodesRequest) returns (stream Node)`, server
pushes ready nodes down the stream as fast as they exist. This looks efficient — no poll
round-trips — and it is exactly the shape REv2's `Execute` uses, but for a different problem:

```protobuf
rpc Execute(ExecuteRequest) returns (stream google.longrunning.Operation) { ... }
rpc WaitExecution(WaitExecutionRequest) returns (stream google.longrunning.Operation) { ... }
```
([source](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto))

Note what's actually being streamed: `Execute` submits **one** action and streams `Operation`
*progress updates for that one action* (queued → executing → complete) back to whichever
client is watching; it is a progress feed, not a task-dispatch feed. Crucially,
`WaitExecution` lets a **disconnected client reattach** to an in-progress operation by name and
receive the current status immediately followed by the rest of the stream
([source](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)) —
that reattach-by-durable-name idea is worth keeping (dagworker's `GetNode` plus `Watch` with a
`node_id_filter` gives the same capability), but note this is a **1:1 stream per unit of
work**, never a fan-out queue of many different units pushed to one consumer — REv2 does not
use server-streaming to solve "dispatch arbitrary tasks to a pool of workers," and neither
should dagworker.

The actual problem with a genuine fan-out push stream (server picks N ready nodes and streams
them at whatever worker happens to be connected) is **flow control at the wrong layer**. gRPC
sits on HTTP/2, and HTTP/2 flow control (RFC 7540 §6.9, `WINDOW_UPDATE` frames) governs *bytes
in flight per stream and per connection* — it stops the sender from writing more DATA frames
than the receiver's TCP-adjacent buffer has advertised room for. It has no concept of "the
receiver is a worker currently running 3 CPU-bound jobs and can't start a 4th" — that is
application-level capacity, and HTTP/2 windows cannot express it. A server-streaming dispatch
RPC therefore needs its own, bespoke, application-level signal for "stop sending, I'm full,"
and a way to un-stall once capacity frees up. gRPC gives you exactly one tool to build that
signal with on a pure server-stream: the receiver can slow down how fast it calls `Recv()`,
which eventually (only after the receiver stops draining and the *transport-level* window
fills) back-pressures the sender — but by then the server has already committed to *who* gets
which node, with no way to hand a stuck node to a different, idle worker without an explicit
un-dispatch/retry protocol. This is the well-known head-of-line problem with server push:
**capacity signaling and item routing collapse into one unreliable byte-buffer heuristic.**
Long poll sidesteps the entire problem by construction — a poll call only exists while the
worker actually wants one unit of work, so there is never a "the server pushed too much"
state to reach.

---

## 3. Bidirectional streaming with explicit credit — where it's the right tool, and where it isn't

### 3.1 Envoy xDS ADS: nonce/ack as a *convergence* protocol, not a capacity protocol

Envoy's Aggregated Discovery Service multiplexes many resource types over one bidi stream.
`DiscoveryRequest` carries `version_info` (the last version the client accepted),
`resource_names` (what it wants), `type_url`, and — the mechanism worth stealing —
`response_nonce`, which must echo the `nonce` of the `DiscoveryResponse` being
acknowledged or rejected. **ACK**: client resends the request with the new `version_info` and
the response's `nonce`. **NACK**: client keeps its *old* `version_info` but still echoes the
new `nonce`, plus fills `error_detail`
([xDS protocol docs](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol)). The
nonce exists purely to disambiguate *which* response a request is acknowledging when several
are in flight — "servers should not send a DiscoveryResponse for any DiscoveryRequest that has
a stale nonce"
([source](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol)). This is not a
credit/backpressure protocol at all — it's a convergence/idempotency protocol for
eventually-consistent config sync (a slow/misbehaving client can be sent more updates whether
it acked the last one or not; xDS updates are absolute-state snapshots, not deltas, so no
element of it is throttled by consumer readiness). This is exactly why the assignment's third
shape ("worker sends I-have-capacity-for-K") *isn't* ADS's actual mechanism, even though ADS is
the canonical bidi-stream-with-ack example — worth being precise about, since it's tempting to
reach for ADS as "the" bidi credit pattern when it demonstrates something adjacent but distinct
(resumable convergence, which dagworker *does* want for `Watch`, see §6) rather than
backpressure (which dagworker gets a different way, §4).

### 3.2 Kubernetes CRI: explicitly *not* a push/streaming dispatch model

CRI is worth including specifically as **negative evidence**. `Exec`, `Attach`, and
`PortForward` are unary RPCs that return **a URL to a separate streaming server** —
`ExecResponse.url` — rather than streaming bytes over the CRI gRPC channel itself
([cri-api `runtime/v1/api.proto`](https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto)).
There is no watch/subscribe RPC for container or pod status in the CRI surface at all — the
kubelet's PLEG (pod lifecycle event generator) gets status by **periodically polling**
`ListPodSandboxStatus`/`ListContainerStats` and diffing, not by having the runtime push
transitions to it
([source](https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto)).
Two lessons: (1) even a system with genuinely heavy, real-time-sensitive dispatch (starting
and stopping containers) chose to keep its *interactive* streaming (exec/attach) completely
out of the control RPC channel via a redirect-to-a-dedicated-server pattern rather than
multiplexing it onto the same stream as scheduling decisions — dagworker has no interactive
byte-stream requirement at all (a node's payload/result are just opaque bytes, not a live
terminal), so this pattern doesn't directly apply, but the *principle* — don't let one
mechanism do two jobs with different latency/volume profiles — is exactly why `Watch` is kept
separate from `ClaimNode` in this design; (2) CRI shipped a real, large-scale production
system on plain polling for status propagation, which is reassuring evidence that "poll,
don't push" is a perfectly serviceable default and not something dagworker needs to apologize
for.

### 3.3 Reactive Streams `request(n)`: the pattern in the abstract

The Reactive Streams / `Flow.Subscriber` model is the textbook version of assignment shape
(c): a `Subscription` exposes `request(long n)` (grant the publisher permission to send up to
`n` more elements) and `cancel()`; the publisher must never send more than the outstanding
requested amount ([reactive-streams.org](https://www.reactive-streams.org/)). This is real and
useful when one producer must serve many slow, heterogeneous consumers over one channel each
with a different, fluctuating appetite — genuinely credit-based flow control. gRPC/Buildbarn's
REv2 world doesn't need a bespoke version of it because of §4's observation: **when the
"credit" a worker has is just "one open slot," a unary RPC per slot already **is** the
`request(1)` call** — no separate credit message needs inventing.

---

## 4. Why the hybrid is narrow: HTTP/2's own flow control **is** the credit protocol

This is the crux of the decision. A dagworker consumer's capacity is granular and discrete: a
worker process has some number of execution slots (goroutines, OS threads, whatever), and each
slot wants *at most one* outstanding node at a time. That is not "give me a byte-rate," it's
"give me at most K concurrently outstanding units of work" — which is *precisely* what an
HTTP/2 connection's `SETTINGS_MAX_CONCURRENT_STREAMS` already limits, and what a client that
opens exactly K concurrent `ClaimNode` calls already expresses without any additional field on
the wire. grpc-go exposes the server side of this knob directly as `grpc.MaxConcurrentStreams`,
a `ServerOption` on `grpc.NewServer`. Point a fleet of dagworkerd replicas' `MaxConcurrentStreams`
at (say) 4096, have each worker open exactly as many concurrent `ClaimNode` calls as it has
execution slots, and the "credit" ADS/CRI-style protocols would build with an explicit
`available_capacity` field is *already enforced by the transport*, for free, with a mechanism
gRPC operators already understand and can observe (`grpc-go`'s stats handler surfaces stream
counts, §12). Building a second, bespoke credit protocol on top — the way ADS needs one because
it is fundamentally push/pub-sub over one shared stream per client, not "one stream per unit of
capacity" — would be solving a problem dagworker doesn't have, at the cost of exactly the
complexity §3.1 catalogs (nonce bookkeeping, un-acked-request tracking, re-issuing credit after
a task finishes). The one piece worth keeping from the credit-protocol family is Buildbarn's
`next_synchronization_at` idea (§1.3, folded into `ClaimNodeResponse.retry_after`, §11) — a
one-directional, optional backoff hint, not a full bidirectional accounting scheme.

`Watch` is bidirectional *not* because dagworker needs credit accounting, but for the same
reason etcd's `Watch` is bidirectional: so a client can multiplex several independent
watches — and *cancel one of them* — on a single connection without tearing the whole stream
down (§6). It is a convergence/resume protocol (ADS's actual lesson, §3.1), not a
backpressure protocol.

---

## 5. The complete `.proto` file

```protobuf
// Copyright (c) dagworker authors.
// SPDX-License-Identifier: MIT
//
// dagworker/v1/dagworker.proto — wire protocol for remote workers and
// control-plane clients of dagworkerd. Nothing in the dagworker core module
// imports anything generated from this file; see §14 for the module
// boundary this enforces.
//
// Design lineage (see this file's containing dossier for the full analysis):
//   - poll/ack/heartbeat/task-token shape:   Temporal WorkflowService
//   - Watch resume-by-revision shape:        etcd's Watch RPC (etcdserverpb)
//   - long-poll-with-scheduled-resync shape: Buildbarn RemoteWorker.Synchronize
//
// buf lint: STANDARD. buf breaking: FILE category (see §13).

syntax = "proto3";

package dagworker.v1;

option go_package = "github.com/specialistvlad/dagworker/gen/go/dagworker/v1;dagworkerv1";

import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";
import "buf/validate/validate.proto";

// ===========================================================================
// Enums
// ===========================================================================

// NodeStatus is the MINIMAL public status of a node (ADR: node status model).
// Internal scheduling state — e.g. "claimed but not yet heartbeated" — is
// derived server-side from (status, Lease) and is never sent on the wire as
// its own enum value; a node with status IN_PROGRESS and an active lease
// looks identical on this wire to one whose lease just silently expired,
// by design (a client should never branch on lease presence to guess intent).
enum NodeStatus {
  NODE_STATUS_UNSPECIFIED = 0;
  NODE_STATUS_NEW         = 1;
  NODE_STATUS_IN_PROGRESS = 2;
  NODE_STATUS_SUCCESS     = 3;
  NODE_STATUS_ERROR       = 4;
  NODE_STATUS_CANCELLED   = 5;
  // Values 1-15 fit in a single-byte varint tag; keep this range for
  // statuses that appear in every WatchEvent before spending it on rarer ones.
  reserved 6 to 15;
}

// FailureKind is carried in FailNode and mirrors Temporal's split between an
// application-level activity failure and a worker/timeout-driven one; it
// becomes ErrorInfo.reason (google.rpc.ErrorInfo) on the *next* read of this
// node's terminal error, not just a FailNode request field.
enum FailureKind {
  FAILURE_KIND_UNSPECIFIED  = 0;
  FAILURE_KIND_APPLICATION  = 1; // the work itself failed; business-logic error
  FAILURE_KIND_WORKER_PANIC = 2; // the worker crashed/exited while holding the lease
  FAILURE_KIND_CANCELLED    = 3; // worker honored CancelNode / a cancellation_requested heartbeat
}

enum WatchEventKind {
  WATCH_EVENT_KIND_UNSPECIFIED    = 0;
  WATCH_EVENT_KIND_TRANSITION     = 1; // node changed status; `node` is populated
  WATCH_EVENT_KIND_WORK_AVAILABLE = 2; // scope-wide wake signal; `node` is unset
}

// ===========================================================================
// Core resources
// ===========================================================================

message Node {
  string id       = 1;
  string scope    = 2;
  NodeStatus status = 3;
  bytes payload   = 4;
  map<string, string> labels = 5; // capability routing, e.g. {"gpu": "true"}
  int32 attempt      = 6;
  int32 max_attempts = 7;
  google.protobuf.Timestamp created_at = 8;
  google.protobuf.Timestamp updated_at = 9;
  int64 revision = 10; // storage-assigned, monotonic per scope; the Watch cursor unit (see etcd mod_revision)
  reserved 11 to 15;   // headroom in the 1-byte-tag range for hot fields added later
}

message Edge {
  string scope        = 1;
  string from_node_id = 2; // must reach NODE_STATUS_SUCCESS before to_node_id can become ready
  string to_node_id   = 3;
}

// Lease is the artifact ClaimNode hands out. task_token is the ONLY value
// the server trusts on ExtendLease/CompleteNode/FailNode; fencing_token is
// the same generation counter exposed in the clear, for storage backends
// (Postgres, Redis) that want a plain integer to CAS on instead of parsing
// the opaque token server-side on every call.
message Lease {
  bytes task_token   = 1;
  int64 fencing_token = 2;
  Node node            = 3; // inlined so ClaimNode is a single round trip
  google.protobuf.Timestamp lease_expires_at = 4; // absolute deadline; INDEPENDENT of the RPC deadline, see §7
}

// ===========================================================================
// WorkerService — the surface a remote worker uses. Deliberately separate
// from ControlService so a worker's mTLS identity/scope grant never needs
// AddNodes/CancelNode privileges (see §11 AuthZ).
// ===========================================================================

service WorkerService {
  // ClaimNode long-polls: it blocks server-side until a ready node in
  // `scope` matching `label_selector` exists, or until poll_timeout elapses,
  // whichever is first. A response with `lease` unset means "no work; the
  // caller should immediately re-call" — exactly Temporal returning an
  // empty task_token, not an error. Capacity is expressed by how many
  // concurrent ClaimNode calls a worker keeps open, not by a field (§4).
  rpc ClaimNode(ClaimNodeRequest) returns (ClaimNodeResponse);

  // ExtendLease is the heartbeat. Must be called again before
  // lease_expires_at or the node fails server-side with a timeout reason and
  // becomes claimable again, subject to retry policy — independent of
  // whether this RPC's own deadline, or even the daemon process, is still
  // alive at that moment (state lives in storage, not in dagworkerd; §9).
  rpc ExtendLease(ExtendLeaseRequest) returns (ExtendLeaseResponse);

  // CompleteNode reports success. Mirrors RespondActivityTaskCompleted.
  rpc CompleteNode(CompleteNodeRequest) returns (CompleteNodeResponse);

  // FailNode reports failure. Mirrors RespondActivityTaskFailed. Kept as its
  // own RPC rather than a `oneof outcome` on one Ack RPC because Temporal's
  // own service does the same, and because FailNodeResponse grows
  // retry-scheduling fields that CompleteNodeResponse never needs.
  rpc FailNode(FailNodeRequest) returns (FailNodeResponse);
}

message ClaimNodeRequest {
  string scope     = 1 [(buf.validate.field).string.min_len = 1];
  string worker_id = 2 [(buf.validate.field).string.min_len = 1];
  map<string, string> label_selector = 3;
  google.protobuf.Duration lease_duration = 4; // requested; server clamps to a scope-configured [min,max]
  google.protobuf.Duration poll_timeout = 5 [(buf.validate.field).duration = {
    gte: { seconds: 0 }
    lte: { seconds: 600 } // hard ceiling, matching Nomad's 10-minute blocking-query cap
  }];
  reserved 6 to 10;
}

message ClaimNodeResponse {
  Lease lease = 1;                            // unset => no work before poll_timeout
  google.protobuf.Duration retry_after = 2;   // optional server-side backoff hint (Buildbarn's next_synchronization_at, relative form)
}

message ExtendLeaseRequest {
  bytes task_token = 1 [(buf.validate.field).bytes.min_len = 1];
  google.protobuf.Duration requested_extension = 2;
}

message ExtendLeaseResponse {
  google.protobuf.Timestamp lease_expires_at = 1;
  bool cancellation_requested = 2; // set when an operator called CancelNode while this lease was outstanding
}

message CompleteNodeRequest {
  bytes task_token = 1 [(buf.validate.field).bytes.min_len = 1];
  bytes result = 2;
}

message CompleteNodeResponse {
  int64 revision = 1;
}

message FailNodeRequest {
  bytes task_token = 1 [(buf.validate.field).bytes.min_len = 1];
  FailureKind kind = 2;
  string message   = 3;
  bool retryable   = 4; // worker's opinion; the scope's retry policy has final say
}

message FailNodeResponse {
  int64 revision = 1;
  bool will_retry = 2;
  google.protobuf.Timestamp next_attempt_at = 3; // set iff will_retry
}

// ===========================================================================
// ControlService — DAG mutation, inspection, and the event Watch stream.
// This surface is never granted to worker credentials (see §11).
// ===========================================================================

service ControlService {
  rpc AddNodes(AddNodesRequest) returns (AddNodesResponse);
  rpc AddEdges(AddEdgesRequest) returns (AddEdgesResponse);
  rpc GetNode(GetNodeRequest) returns (GetNodeResponse);
  rpc CancelNode(CancelNodeRequest) returns (CancelNodeResponse);

  // Watch streams every status transition plus scope-wide WORK_AVAILABLE
  // wake signals, bidirectionally, so one connection can multiplex several
  // independent watches and cancel any one of them without tearing the
  // whole stream down — etcd's Watch shape (§6), not a credit protocol (§4).
  rpc Watch(stream WatchRequest) returns (stream WatchResponse);
}

message NewNode {
  string client_id = 1; // caller-chosen key, unique within this request only; resolves `edges` below
  bytes payload     = 2;
  map<string, string> labels = 3;
  int32 max_attempts = 4;
}

message NewEdge {
  string from_client_id = 1; // resolves against a NewNode.client_id in the SAME request, or an existing node_id
  string to_client_id   = 2;
}

message AddNodesRequest {
  string scope = 1 [(buf.validate.field).string.min_len = 1];
  repeated NewNode nodes = 2 [(buf.validate.field).repeated.min_items = 1];
  // Optional: attach edges atomically with the nodes they connect, so no
  // node is ever briefly visible as ready with a dependency edge missing —
  // a real race if nodes and edges are two separate calls (see Open Questions).
  repeated NewEdge edges = 3;
}

message AddNodesResponse {
  repeated Node nodes = 1; // same order as the request; id and revision filled in
}

message AddEdgesRequest {
  string scope = 1 [(buf.validate.field).string.min_len = 1];
  repeated Edge edges = 2 [(buf.validate.field).repeated.min_items = 1];
}

message AddEdgesResponse {
  int64 revision = 1;
}

message GetNodeRequest {
  string scope   = 1 [(buf.validate.field).string.min_len = 1];
  string node_id = 2 [(buf.validate.field).string.min_len = 1];
}

message GetNodeResponse {
  Node node = 1;
}

message CancelNodeRequest {
  string scope   = 1 [(buf.validate.field).string.min_len = 1];
  string node_id = 2 [(buf.validate.field).string.min_len = 1];
  string reason  = 3;
}

message CancelNodeResponse {
  Node node = 1;
}

// --- Watch ------------------------------------------------------------

message WatchEvent {
  int64 revision              = 1;
  WatchEventKind kind          = 2;
  Node node                    = 3; // set iff kind == WATCH_EVENT_KIND_TRANSITION
  NodeStatus previous_status  = 4;
}

message WatchCreateRequest {
  int64 watch_id  = 1; // client-assigned, unique on this stream; echoed on every response for demultiplexing
  string scope     = 2 [(buf.validate.field).string.min_len = 1];
  string node_id_filter = 3; // optional: narrow to one node's transitions
  int64 start_revision  = 4; // 0 = "start now"; >0 resumes from a prior cursor (§6)
  bool progress_notify  = 5; // periodic empty WatchResponse so a quiet client can still advance its cursor
}

message WatchCancelRequest {
  int64 watch_id = 1;
}

message WatchRequest {
  oneof request {
    WatchCreateRequest create = 1;
    WatchCancelRequest cancel = 2;
  }
}

message WatchResponse {
  int64 watch_id            = 1;
  bool created                = 2;
  bool canceled                = 3;
  string cancel_reason        = 4;
  int64 compacted_revision   = 5; // set iff canceled because start_revision was already compacted away
  repeated WatchEvent events = 6;
}
```

Numbering discipline used throughout: fields 1–15 (single-byte varint tags) are reserved for
values present on nearly every message of that type; `reserved` ranges are pre-declared on
`Node` and `NodeStatus` so a future field/enum value never silently reuses a number a very old
client cached; every RPC has its own `*Request`/`*Response` pair (never a shared `Empty`) so
each can evolve independently — the exact discipline buf's `RPC_REQUEST_RESPONSE_UNIQUE` lint
rule enforces (§13).

---

## 6. Watch: resume semantics, modeled directly on etcd

etcd's `Watch` is the textbook version of "stream of changes with a resumable cursor," and
dagworker's `Watch` borrows its shape field-for-field. The real etcd RPC
([`etcdserverpb/rpc.proto`](https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto)):

```protobuf
service Watch { rpc Watch(stream WatchRequest) returns (stream WatchResponse); }

message WatchCreateRequest {
  bytes key = 1; bytes range_end = 2;
  int64 start_revision = 3;   // 0 = "watch from now"
  bool progress_notify = 4;
  repeated FilterType filters = 5;
  bool prev_kv = 6; int64 watch_id = 7; bool fragment = 8;
}
message WatchResponse {
  ResponseHeader header = 1; int64 watch_id = 2;
  bool created = 3; bool canceled = 4;
  int64 compact_revision = 5;   // set + canceled=true iff start_revision was already compacted away
  string cancel_reason = 6; bool fragment = 7;
  repeated mvccpb.Event events = 11;
}
```

The resume protocol
([etcd learning/api docs](https://etcd.io/docs/v3.5/learning/api/),
[proto source](https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto)):

1. A client tracks the last `revision` it successfully processed (from `events` or from a
   periodic `progress_notify` heartbeat response sent even when nothing changed, so a quiet
   watch's cursor still advances).
2. On reconnect, it opens a fresh `Watch` stream and sends `WatchCreateRequest{start_revision:
   last_seen + 1}` — every event since that revision streams back, nothing is missed.
3. If the server already compacted history past `start_revision` (it only retains a bounded
   window of old revisions), it responds `WatchResponse{canceled: true, compact_revision: N}`
   instead of events — the client cannot resume from where it left off and must fall back to a
   full resync (e.g. re-list current state, then watch from the fresh revision the list
   returned).

dagworker's `WatchCreateRequest.start_revision` / `WatchResponse.compacted_revision` are a
direct copy of this. The concrete mapping onto dagworker's storage: `Node.revision` plays the
role of etcd's per-key `mod_revision`, and the *scope* needs one monotonically increasing
counter (dagworker's equivalent of etcd's cluster-wide revision) that every mutation — a status
transition, `AddNodes`, `AddEdges` — bumps, so `Watch` can order and gap-detect across
different nodes within one scope, not just within one node's own history. **Compaction** for
dagworker means: the in-memory/Redis/Postgres backend only retains a bounded ring/queue of
recent transition events per scope (bounded by count or age, a config knob), and a `Watch`
whose `start_revision` falls before the oldest retained event gets `canceled + compacted_revision`
exactly like etcd, forcing the client through `GetNode` (or a bulk equivalent) to resync rather
than silently missing history. This bound is what keeps the event log O(1)-ish per scope
instead of an unbounded append log — consistent with the project's O(1)/O(log n) performance
goal even at the 1M-node benchmark scale, since the *retained event window* is independent of
total node count.

`progress_notify` matters specifically for a dagworker deployment with many idle scopes: a
quiet scope's watchers would otherwise have no signal to distinguish "connection silently died"
from "genuinely nothing happened in 20 minutes," which interacts directly with the keepalive
discussion in §9 — a periodic empty `WatchResponse` is an *application-level* liveness signal
layered on top of, not a replacement for, gRPC's own transport-level keepalive pings.

---

## 7. Deadlines and cancellation — and why the lease deadline must be its own thing

gRPC deadlines are relative-then-absolute: a client sets a timeout, gRPC converts it to a wall
clock deadline, and that deadline is what actually propagates on the wire (as
`grpc-timeout`) — critically, **gRPC converts the deadline to a timeout from which the already
elapsed time is deducted** before forwarding it to a downstream call, specifically to avoid
clock-skew problems across hops ([gRPC deadlines guide](https://grpc.io/docs/guides/deadlines/)).
In Go, this propagation is automatic *if and only if* the downstream call reuses the same
(or a `context.WithValue`-derived) `context.Context` — `context.WithTimeout`/`WithDeadline`
carry the deadline; a fresh `context.Background()` does not
([source](https://grpc.io/docs/guides/deadlines/)). When a deadline fires, the *server* gets
told via `ctx.Done()`/`Err() == context.DeadlineExceeded`, and gRPC itself does **not** stop
whatever work the handler spawned — "the server application is responsible for stopping any
activity it has spawned to service the RPC"
([source](https://grpc.io/docs/guides/deadlines/)). This last point is exactly why the
`ClaimNode` handler must itself watch `ctx.Done()` inside its blocking wait rather than relying
on gRPC to kill it (§8's graceful-shutdown handler reuses the identical pattern).

**The load-bearing design rule: `poll_timeout` (a field inside `ClaimNodeRequest`, bounding how
long the long poll itself may block) and `lease_duration`/`lease_expires_at` (how long the
*resulting* lease is valid once a node is actually claimed) are two independent numbers that
must never be derived from the same context.** Concretely:

```go
// WRONG: reuses the poll's context (and its ~30s deadline) for the whole job.
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
lease, _ := worker.ClaimNode(ctx, &pb.ClaimNodeRequest{PollTimeout: durationpb.New(30 * time.Second)})
// ... 4 minutes of real work later ...
worker.CompleteNode(ctx, &pb.CompleteNodeRequest{TaskToken: lease.TaskToken}) // ctx is ALREADY EXPIRED

// RIGHT: each RPC gets its own short-lived context; the lease's clock lives
// only in dagworkerd's storage, keyed by task_token, never in any client ctx.
pollCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
lease, _ := worker.ClaimNode(pollCtx, &pb.ClaimNodeRequest{PollTimeout: durationpb.New(30 * time.Second)})
cancel()
// ... do the work, calling ExtendLease periodically on ITS OWN fresh short ctx ...
completeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
worker.CompleteNode(completeCtx, &pb.CompleteNodeRequest{TaskToken: lease.TaskToken})
```

The bug on the "WRONG" path is not hypothetical: sharing one context across a poll-then-work
loop is the single most common mistake ported from naive HTTP client code, and it silently
turns every job whose real runtime exceeds the poll timeout into a guaranteed `CompleteNode`
failure. The RPC deadline governs *transport-level patience*; the lease deadline governs
*correctness* (whether another worker is allowed to pick the node up) and is enforced entirely
server-side against wall-clock time recorded in storage — it must survive the RPC that created
it ending, the connection dropping, and even the specific `dagworkerd` replica that issued it
being killed (§9). `ExtendLease`'s own RPC deadline should likewise be short (a few seconds) and
independent of both — a slow heartbeat call timing out is a transient network hiccup to retry,
not evidence the lease itself expired.

Cancellation flows the other direction through `ExtendLeaseResponse.cancellation_requested`
(§1.1's Temporal parallel): an operator calling `CancelNode` doesn't reach into the worker
process at all (there is no way to, over a plain RPC protocol, without the worker itself
polling or streaming) — it flips state in storage, and the *next* heartbeat the worker
happens to send surfaces it. A worker that never heartbeats (a tight CPU-bound loop) will not
see a cancellation until it finishes or its lease times out; this is a real, disclosed
limitation, not a bug — heartbeat frequency is the resolution of dagworker's cancellation
signal, exactly as it is Temporal's.

---

## 8. Keepalive — the setting that silently breaks long polls

Long poll's entire value proposition (an idle-looking RPC that's actually still useful) is the
exact shape gRPC's keepalive machinery was tuned to be suspicious of, and the defaults on
*both* sides actively fight it if left alone.

**Defaults** ([grpc-go `keepalive.go`](https://github.com/grpc/grpc-go/blob/master/keepalive/keepalive.go),
[grpc.io keepalive guide](https://grpc.io/docs/guides/keepalive/),
[grpc/grpc keepalive spec](https://github.com/grpc/grpc/blob/master/doc/keepalive.md)):

| Parameter | Client default | Server default |
|---|---|---|
| `Time` (ping interval) | `INT_MAX` — pings **disabled** | 2 hours |
| `Timeout` (ping ack wait) | 20s | 20s |
| `PermitWithoutStream` | false | false (`PERMIT_KEEPALIVE_WITHOUT_CALLS`) |
| `EnforcementPolicy.MinTime` | — | 5 minutes |

The failure mode this dossier was asked to study directly: if a client pings more often than
the server's `MinTime` allows (or pings with `PermitWithoutStream` while the server disallows
it), the server sends **`GOAWAY` with debug data `too_many_pings`, the HTTP/2
`ENHANCE_YOUR_CALM` error code** — the connection is torn down mid-flight, taking every
in-progress `ClaimNode` long poll and `Watch` stream on it with it
([grpc.io guide](https://grpc.io/docs/guides/keepalive/),
[grpc/grpc spec](https://github.com/grpc/grpc/blob/master/doc/keepalive.md)). The grpc.io guide is
blunt about the failure being self-inflicted: **"it is recommended to avoid enabling keepalive
without calls and for clients to avoid configuring their keepalive much below one minute"** —
and mismatched client/server settings (client `PermitWithoutStream=true` against a server that
disallows it) is called out explicitly as a common trigger
([source](https://grpc.io/docs/guides/keepalive/)).

For dagworker, `PermitWithoutStream` is not optional — a worker parked in a `ClaimNode` long
poll or an idle `Watch` has no other traffic on the connection to piggyback a liveness check
on, so without it, a half-dead TCP connection (NAT box silently dropped state, a cloud LB
recycled without an RST) is invisible until the *next* real call, which for a long poll could
be tens of seconds to minutes away. Since dagworkerd controls both ends (it ships as a Go
daemon *and* is the natural home for a reference client SDK), the fix is to ship matched,
explicit settings on both sides rather than accept the client-disabled/server-2-hour defaults:

```go
// dagworkerd server:
grpc.KeepaliveParams(keepalive.ServerParameters{
    MaxConnectionIdle:     0,                // idle IS the steady state for a parked long-poll worker
    MaxConnectionAge:      30 * time.Minute, // periodic forced GOAWAY — see §10 for why this is a feature, not a bug
    MaxConnectionAgeGrace: 5 * time.Minute,  // let an in-flight ClaimNode/Watch drain before the hard close
    Time:                  2 * time.Minute,  // detect dead peers well under any sane lease_duration
    Timeout:               20 * time.Second,
}),
grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
    MinTime:             1 * time.Minute, // <= every client Time below, or legitimate client pings look abusive
    PermitWithoutStream: true,
}),

// reference worker SDK:
grpc.WithKeepaliveParams(keepalive.ClientParameters{
    Time:                1 * time.Minute,  // >= server MinTime, per the guide's own warning above
    Timeout:             20 * time.Second,
    PermitWithoutStream: true,             // a blocked long poll must still be pingable
}),
```

Independently of gRPC's own keepalive, `poll_timeout` should default well under common
infrastructure idle-connection ceilings (many managed L4/L7 load balancers default to 60s;
some cloud NAT gateways to ~350s) — 30–45s is a defensible default — as defense in depth *on
top of* keepalive, not instead of it: keepalive detects a dead connection; a bounded
`poll_timeout` guarantees the *application* re-evaluates readiness periodically even if nothing
ever kills the transport, which also bounds how stale a worker's view of "is my scope even
still valid" can get.

---

## 9. Graceful shutdown and draining

grpc-go's server exposes two shutdown paths with genuinely different semantics
([`grpc-go/server.go`](https://github.com/grpc/grpc-go/blob/master/server.go)):

- **`Stop()`**: *"immediately closes all open connections and listeners. It cancels all active
  RPCs on the server side and the corresponding pending RPCs on the client side will get
  notified by connection errors."* Every in-flight `ClaimNode` long poll and `Watch` stream
  dies with a transport error — a worker mid-poll sees `Unavailable`, not a clean "no work"
  response.
- **`GracefulStop()`**: *"stops the server from accepting new connections and RPCs and blocks
  until all the pending RPCs are finished."* It sends `GOAWAY` (refusing new streams on
  existing connections) and then waits — with no built-in timeout — for every RPC already in
  flight to return on its own.

The trap: `GracefulStop()`'s unbounded wait is exactly wrong for a service whose steady state
*is* long-lived blocked RPCs — calling it naively means the daemon hangs for up to
`poll_timeout` (or, for `Watch`, effectively forever, since nothing about a healthy watch ever
"finishes" on its own) before a `SIGTERM` actually takes effect. The fix is for the `ClaimNode`
and `Watch` handlers themselves to select on a shutdown broadcast alongside their normal wait
condition, so they return promptly *as part of* the graceful drain instead of being waited out:

```go
func (s *workerServer) ClaimNode(ctx context.Context, req *pb.ClaimNodeRequest) (*pb.ClaimNodeResponse, error) {
    deadline := time.Now().Add(req.PollTimeout.AsDuration())
    for {
        if lease, ok := s.store.TryClaim(ctx, req.Scope, req.LabelSelector); ok {
            return &pb.ClaimNodeResponse{Lease: leaseToProto(lease)}, nil
        }
        select {
        case <-ctx.Done():
            return nil, status.FromContextError(ctx.Err()).Err() // client-side cancel or its own deadline
        case <-s.shutdown:                                        // daemon-wide drain signal
            return &pb.ClaimNodeResponse{}, nil                   // empty response: "no work", worker retries elsewhere
        case <-s.store.WakeSignal(req.Scope):                      // pushed on AddNodes/status transitions; avoids busy-polling storage
        case <-time.After(time.Until(deadline)):
            return &pb.ClaimNodeResponse{}, nil
        }
    }
}

func (d *Daemon) Shutdown(ctx context.Context) error {
    close(d.shutdown)          // unblocks every parked ClaimNode/Watch handler immediately
    done := make(chan struct{})
    go func() { d.grpcServer.GracefulStop(); close(done) }()
    select {
    case <-done:
        return nil
    case <-ctx.Done():
        d.grpcServer.Stop()    // last resort: hard-cancel anything that didn't drain in time
        return ctx.Err()
    }
}
```

The single fact that makes any of this safe rather than merely convenient: **a lease's state
lives in shared storage (in-memory only for the single-process case; Redis/Postgres/memcached
otherwise), never in the daemon process's own memory.** Restarting `dagworkerd` — planned
deploy, crash, OOM-kill — does **not** orphan any outstanding lease: `task_token` is not bound
to a TCP connection or to the specific replica that issued it, so any other replica sharing the
same storage backend can accept the eventual `ExtendLease`/`CompleteNode`/`FailNode` for that
token, and if the worker never reaches *any* replica before `lease_expires_at`, the storage
layer's own timeout sweep fails the node on schedule regardless of whether the issuing daemon
process still exists. This is the direct payoff of putting lease state in storage rather than
daemon RAM, structurally identical to why Temporal's Frontend/Matching services are stateless
in front of a persisted History — restart the stateless tier freely, the durable tier is where
correctness lives.

---

## 10. Error model: `google.rpc.Status` and canonical code mapping

Every dagworker error is a `google.rpc.Status{code, message, details}` surfaced through gRPC's
normal status/trailer mechanism, with `details` populated from the standard payloads in
[`google/rpc/error_details.proto`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto):

- **`ErrorInfo{reason, domain, metadata}`** — AIP-193 makes this **mandatory on every error**:
  `reason` is a stable, `SCREAMING_SNAKE_CASE` machine-readable identifier (max 63 chars,
  `[A-Z][A-Z0-9_]+[A-Z0-9]`), `domain` is the owning service (`dagworker.v1`), `metadata` carries
  the dynamic, per-instance context (which `node_id`, which `scope`)
  ([AIP-193](https://google.aip.dev/193)). The `(reason, domain)` pair is the actual stable
  contract across releases — `message` text is free to improve.
- **`RetryInfo{retry_delay}`** — attached to `UNAVAILABLE`/`RESOURCE_EXHAUSTED` so a client
  knows how long to back off rather than guessing.
- **`PreconditionFailure{violations: [{type, subject, description}]}`** — attached to
  `FAILED_PRECONDITION` (e.g. a rejected `AddEdges` call that would create a cycle).
- **`QuotaFailure{violations: [{subject, quota_metric, quota_value}]}`** — attached to
  `RESOURCE_EXHAUSTED` when a scope hits an in-flight-node or claim-rate limit.
- **`BadRequest{field_violations: [{field, description}]}`** — attached to `INVALID_ARGUMENT`,
  and in practice mostly produced automatically by the `protovalidate` interceptor (§13) before
  a request ever reaches business logic.

| Condition | gRPC code | `ErrorInfo.reason` | Notes |
|---|---|---|---|
| `task_token` from an expired lease | `NOT_FOUND` | `LEASE_EXPIRED` | Same code Temporal uses for this whole family; a *new* `ClaimNode` is the only correct recovery, never a retry of the same token. |
| `task_token` already consumed by a prior `Complete`/`Fail` | `NOT_FOUND` | `ALREADY_COMPLETED` | See idempotency caveat below — recommend making an *identical* retried `CompleteNode` a no-op success instead. |
| `task_token` never issued / garbage | `NOT_FOUND` | `UNKNOWN_TASK_TOKEN` | |
| `AddEdges` would create a cycle | `FAILED_PRECONDITION` | `CYCLE_DETECTED` | `PreconditionFailure{type: "DAG_ACYCLIC", subject: "<from>-><to>"}`. |
| `GetNode`/`CancelNode` on unknown `node_id` | `NOT_FOUND` | `NODE_NOT_FOUND` | |
| Scope over its in-flight-node quota | `RESOURCE_EXHAUSTED` | `SCOPE_QUOTA_EXCEEDED` | + `QuotaFailure`, + `RetryInfo`. |
| Credential valid but wrong scope | `PERMISSION_DENIED` | `SCOPE_FORBIDDEN` | Raised by the authz interceptor, §11. |
| No/invalid credential | `UNAUTHENTICATED` | — | |
| Daemon draining, redirect elsewhere | `UNAVAILABLE` | `DRAINING` | + `RetryInfo{retry_delay: 1s}`; client-side LB (§12) should mark the peer down and reconnect. |
| Malformed request (bad selector, negative duration) | `INVALID_ARGUMENT` | field-specific | Caught by `protovalidate`, never reaches storage. |

**Open call-out, not resolved by fiat here:** Temporal collapses "already completed" into the
same `NotFound` as a genuinely expired/garbage token, which is defensible for Temporal's
workflow-replay model but is a sharper edge for dagworker, where a worker that completes real
work, then times out waiting for `CompleteNode`'s *response* and retries the identical call, is
a completely ordinary occurrence (not a bug) under at-least-once RPC semantics. Recommendation
(§16): make `CompleteNode`/`FailNode` idempotent per `task_token` — a second call with the same
token and equivalent payload returns the same success response instead of `NOT_FOUND`, and only
a *different* payload (a genuinely conflicting completion) is treated as an error. This is a
storage-layer decision (needs to remember the last outcome per token for a bounded window, not
just the token's validity), flagged here rather than pre-decided.

---

## 11. AuthN / AuthZ

**Transport**: mTLS is the default for worker↔`dagworkerd` traffic — `credentials.NewTLS` with
`tls.RequireAndVerifyClientCert` server-side; a worker's certificate SAN encodes its identity
and, ideally, the scope(s) it's allowed to touch, so the *transport handshake itself* is the
first authorization gate before any RPC-specific logic runs
([gRPC auth guide](https://grpc.io/docs/guides/auth/)). For lighter-weight consumers (the
HTTP/JSON adapter, or SDKs that can't easily manage client certs), gRPC's `PerRPCCredentials`
interface attaches a bearer token as metadata on every call and composes with the transport
credentials via `grpc.WithPerRPCCredentials`
([source](https://grpc.io/docs/guides/auth/)) — dagworkerd should reject any `PerRPCCredentials`
whose `RequireTransportSecurity()` returns false in production, so a bearer token is never sent
in the clear.

**Two services, two credential shapes**: `WorkerService` and `ControlService` being separate
gRPC services (§5) is what makes scope-level authorization tractable rather than
best-effort — a worker's mTLS cert or bearer token should simply never carry a claim that maps
to `ControlService` RPCs at all, so a compromised worker cannot mutate the DAG it's consuming
from even if application-layer authz logic has a bug.

**Scope-level authorization hook**: a single unary+stream server interceptor, since every
request message that touches a scope carries a `scope` field at position 1 or 2 by convention
(§5):

```go
type ScopedRequest interface{ GetScope() string }

func ScopeAuthzUnaryInterceptor(authz Authorizer) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        if sr, ok := req.(ScopedRequest); ok {
            if !authz.AllowsScope(ctx, sr.GetScope()) { // checks the mTLS SAN / verified bearer claims already on ctx
                st := status.New(codes.PermissionDenied, "credential is not authorized for this scope")
                st, _ = st.WithDetails(&errdetails.ErrorInfo{
                    Reason: "SCOPE_FORBIDDEN", Domain: "dagworker.v1",
                    Metadata: map[string]string{"scope": sr.GetScope()},
                })
                return nil, st.Err()
            }
        }
        return handler(ctx, req)
    }
}
```

The identity itself is attached earlier in the chain by a separate authentication interceptor
(peer cert inspection via `peer.FromContext(ctx).AuthInfo`, or bearer-token verification) —
kept as two interceptors, not one, so authn and authz can be tested, replaced, and reasoned
about independently (e.g. swapping mTLS for an external OIDC-token authenticator later touches
only the first one).

---

## 12. Observability

**Transport-level**: register the OpenTelemetry `stats.Handler` implementation
(`otelgrpc.NewServerHandler()` / `otelgrpc.NewClientHandler()`) via `grpc.StatsHandler(...)` at
`grpc.NewServer`/dial time rather than writing interceptors for this — grpc-go's `stats.Handler`
interface (`TagRPC`, `HandleRPC`, `TagConn`, `HandleConn`) is specifically the hook that also
sees `ConnBegin`/`ConnEnd`
([pkg.go.dev grpc/stats](https://pkg.go.dev/google.golang.org/grpc/stats)), which interceptors
cannot — and connection-level visibility is exactly what §9's "are our long-poll connections
actually spread across replicas" question needs. This emits the standard OpenTelemetry gRPC
semantic-convention attributes: `rpc.system=grpc`, `rpc.service`, `rpc.method`,
`rpc.grpc.status_code`/`rpc.response.status_code`, `server.address`/`server.port`,
`network.peer.address`/`network.peer.port`
([OTel gRPC semconv](https://opentelemetry.io/docs/specs/semconv/rpc/grpc/)).

**The metric that generic RPC instrumentation gets wrong for this protocol**: a standard
"RPC duration" histogram on `ClaimNode` is close to meaningless by construction — a healthy call
that waited the full `poll_timeout` because the scope was quiet and one that returned in 2ms
because work was already queued are *both* correct, and a p99-latency dashboard built from the
raw duration will look identically "bad" during a genuinely idle scope and during real
overload. Emit two custom attributes/metrics instead: `dagworker.claim.outcome ∈
{leased, empty, deadline_exceeded}` on every call, and a duration histogram *split by that
outcome* — only the `empty` bucket's duration is informative about queue emptiness at all, and
only `leased` duration (should be near-zero) is informative about scheduling latency.

**Application-level span/metric attributes worth adding on top of the generic gRPC ones**:
`dagworker.scope`, `dagworker.node.attempt`, `dagworker.lease.fencing_token` (on
`ExtendLease`/`CompleteNode`/`FailNode` spans, so a trace can be joined end-to-end across a
node's entire claim→heartbeat×N→complete lifecycle even though each call is a separate RPC and,
usually, a separate connection).

---

## 13. Backwards compatibility: buf lint, buf breaking, protovalidate

**Lint** — `buf.yaml`'s `lint.use: [STANDARD]` (the default) already enforces the naming
discipline this proto follows: `SERVICE_PASCAL_CASE`, `RPC_PASCAL_CASE`,
`RPC_REQUEST_STANDARD_NAME`/`RPC_RESPONSE_STANDARD_NAME` (every RPC's request/response is named
`<Method>Request`/`<Method>Response`), `RPC_REQUEST_RESPONSE_UNIQUE` (no RPC may reuse another
RPC's message, and — the reason `google.protobuf.Empty` is never used above — a shared `Empty`
response can't evolve independently per-RPC later), plus `PACKAGE_DIRECTORY_MATCH` and
`PACKAGE_VERSION_SUFFIX` for repo layout (§14)
([buf lint rules](https://buf.build/docs/lint/rules/)). The stricter `COMMENTS` category (every
message/field/RPC must be documented) is worth turning on for this specific proto given it's a
cross-language public contract, even though the org default might not enable it everywhere.

**Breaking-change detection** — `buf breaking` has four strictness categories, from loosest to
strictest: `WIRE` (binary wire compatibility only), `WIRE_JSON` (wire + JSON field-name
compatibility — buf's own recommended floor), `PACKAGE` (also forbids moving generated symbols
between packages), `FILE` (also forbids moving them between *files*, i.e. the strictest —
matches "we commit generated code," §14, since a `FILE`-level break is the level most likely to
also break the committed Go package's import structure)
([buf breaking rules](https://buf.build/docs/breaking/rules/)). Recommendation: `FILE` category
in `buf.yaml`, run in CI as `buf breaking --against '.git#branch=main'` on every PR touching
`proto/`, with `EXTENSION_MESSAGE_NO_DELETE`-style narrow excepts only if a specific, reviewed
migration needs one.

**protovalidate** — constraints are declared as field options
(`(buf.validate.field).string.min_len`, `.bytes.min_len`, `.repeated.min_items`,
`.duration.{gte,lte}`, as used throughout §5) and enforced by a runtime library
(`protovalidate-go` for this project) rather than hand-written `if req.Scope == ""` checks
scattered through handlers ([protovalidate](https://github.com/bufbuild/protovalidate)). Wired
as a single interceptor ahead of the authn/authz ones from §11:

```go
validator, _ := protovalidate.New()
func ValidationUnaryInterceptor(v protovalidate.Validator) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
        if pm, ok := req.(proto.Message); ok {
            if err := v.Validate(pm); err != nil {
                return nil, protovalidateToStatus(err) // maps to INVALID_ARGUMENT + BadRequest.FieldViolation
            }
        }
        return handler(ctx, req)
    }
}
```

This keeps `INVALID_ARGUMENT` handling in exactly one place and guarantees business logic never
sees, e.g., a negative `poll_timeout` or an empty `scope` — the constraints are also visible
directly in the `.proto` file, which doubles as living documentation for every non-Go SDK
generated off the same schema (§14).

---

## 14. Codegen and repo layout

**Proto directory layout.** The single consolidated file in §5 is for legibility as a dossier
centerpiece; the real repo splits it by resource, one small file per buf's
`PACKAGE_DIRECTORY_MATCH`/`FILE`-category expectations (§13):

```
dagworker-proto/                       # separate repo (or a separate module within a monorepo) —
├── buf.yaml                           # never imported by the dagworker core module (§16 item 8)
├── buf.gen.yaml
├── buf.lock
└── proto/
    └── dagworker/
        └── v1/
            ├── node.proto             # Node, Edge, Lease, NodeStatus, FailureKind
            ├── worker_service.proto   # WorkerService + Claim/ExtendLease/Complete/Fail messages
            ├── control_service.proto  # ControlService + AddNodes/AddEdges/GetNode/CancelNode messages
            └── watch.proto            # Watch + WatchEvent/WatchCreateRequest/WatchCancelRequest
```

**`buf.yaml` (v2)** — `FILE`-category breaking checks (§13) and the `STANDARD` lint set, scoped
to the one module:

```yaml
version: v2
modules:
  - path: proto
    name: buf.build/specialistvlad/dagworker
lint:
  use: [STANDARD]
breaking:
  use: [FILE]
deps:
  - buf.build/bufbuild/protovalidate
```
([buf.yaml v2 reference](https://buf.build/docs/configuration/v2/buf-yaml/))

**`buf.gen.yaml` (v2)** — generates the committed Go package plus gRPC stubs; non-Go SDKs are
produced separately via BSR remote generation (below), not by extending this file per language:

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/specialistvlad/dagworker/gen/go
plugins:
  - remote: buf.build/protocolbuffers/go
    out: gen/go
    opt: [paths=source_relative]
  - remote: buf.build/grpc/go
    out: gen/go
    opt: [paths=source_relative]
inputs:
  - directory: proto
```
([buf.gen.yaml v2 reference](https://buf.build/docs/configuration/v2/buf-gen-yaml/))

**Commit the generated Go code — yes.** The counter-argument (generated code is a build
artifact, don't commit build artifacts) is the right default for most codegen but wrong here,
for reasons specific to what this package is: (1) it is a **public, cross-language wire
contract**, and `go get github.com/specialistvlad/dagworker/gen/go/dagworker/v1` must work for
every downstream Go consumer — a worker SDK author, a scheduler, a test harness — with zero
local `buf`/`protoc` toolchain, zero plugin-version skew between contributors, and zero
build-time codegen step to cache correctly in CI; (2) a proto field-numbering mistake or an
accidental breaking rename becomes visible **in the same PR diff** as the generated Go it
produces, rather than requiring a reviewer to mentally regenerate code to see the blast radius;
(3) `buf breaking` in CI (§13) is what actually guards correctness here, not the act of
regenerating — committing removes "did anyone remember to regenerate before merging" as a
whole class of drift bug. The generated tree is still fully reproducible
(`buf generate` from the pinned `buf.lock` reproduces it byte-for-byte), so nothing about
committing it sacrifices the ability to regenerate — it only removes the *requirement* to.

**Publishing to the Buf Schema Registry.** CI pushes the module on every merge to `main`:

```
buf push --tag "$(git rev-parse --short HEAD)"
```

This is what actually delivers "Python/Node/Rust/Java workers can participate" (the assignment's
framing goal), not the `.proto` files existing in a Git repo somewhere: a Python worker author
runs `buf generate buf.build/specialistvlad/dagworker --template buf.gen.python.yaml` (or
depends on BSR's remote-generated packages directly, for ecosystems BSR publishes to
natively) without ever cloning this repo or installing `buf` themselves beyond the one `buf
generate` invocation — the Go module stays hand-committed for the reasons above, while every
other language rides on BSR's remote-generation path off the identical, versioned schema.

## 15. Load balancing across replicas — why long poll + an L4 LB is a trap

gRPC/HTTP2 multiplexes many RPCs over one long-lived TCP connection; an L4 (TCP) load balancer
only makes a placement decision *once, at connection setup* — after that, **"all requests will
get pinned to a single destination pod"**
([Kubernetes blog: gRPC Load Balancing on Kubernetes without Tears](https://kubernetes.io/blog/2018/11/07/grpc-load-balancing-on-kubernetes-without-tears/)).
This is already a known gRPC problem for ordinary short unary RPCs behind a plain L4 VIP; it is
*worse* for dagworker specifically, because a fleet of workers each holding a handful of very
long-lived, mostly-idle connections (parked in `ClaimNode`/`Watch`) means an unlucky initial
placement doesn't average out the way bursty, short-lived connections eventually would — a
`dagworkerd` replica that happened to receive more connections at rollout time stays
overloaded, potentially for the connection's entire multi-hour lifetime.

Two real fixes, not one:

1. **Client-side load balancing with real multi-address resolution.** grpc-go's `pick_first`
   (the default) connects to the first resolved address and sticks with it — fine for a single
   backend, wrong for a fleet; `round_robin` opens a subchannel *per resolved address* and
   distributes successive RPCs across whichever are `READY`
   ([grpc load-balancing doc](https://github.com/grpc/grpc/blob/master/doc/load-balancing.md)).
   This only works if the resolver actually returns multiple addresses — a headless Kubernetes
   `Service` (`dns:///dagworkerd.ns.svc.cluster.local`) or an xDS resolver, **never** a
   ClusterIP/VIP, which resolves to exactly one address and makes `round_robin` degenerate back
   to `pick_first`. Ship the worker SDK defaulting to:
   ```go
   grpc.NewClient("dns:///dagworkerd.dagworker.svc.cluster.local:9443",
       grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
       grpc.WithTransportCredentials(creds))
   ```
2. **`MaxConnectionAge` as a forced-rebalancing dial**, independent of #1 — even a
   correctly-`round_robin`-configured client only rebalances *when it opens new connections*;
   a connection that's been `READY` for hours never moves. Server-side `MaxConnectionAge` (§8's
   30-minute example) periodically `GOAWAY`s every connection regardless of health, forcing
   every client to re-resolve and reconnect on a bounded cycle — cheap, general, and doesn't
   depend on the client having implemented anything beyond ordinary gRPC reconnect handling.
   `MaxConnectionAgeGrace` softens this into "stop accepting new streams now, finish what's
   in flight, then close" rather than a hard cut.

A lookaside/L7 proxy (Envoy/Linkerd doing per-*stream*, not per-*connection*, balancing) is the
production-grade alternative to both and is what the Kubernetes blog above ultimately
recommends for service-mesh deployments — but it's an infrastructure choice orthogonal to the
protocol itself, whereas #1 and #2 are protocol-level defaults dagworker's own SDK and daemon
should ship regardless of what the deployer puts in front of them.

---

## 16. Recommendations for dagworker

1. **Adopt the hybrid exactly as decided above ("Decision, up front") and in §4**: `ClaimNode`/`ExtendLease`/`CompleteNode`/`FailNode`
   as unary long-poll + token RPCs (Temporal shape), `Watch` as a separate bidi etcd-shaped
   stream for events/wake signals — do not build a third, generic push-dispatch RPC; it solves
   a problem this workload doesn't have (§2, §4).
2. **Ship the `.proto` in §5 as the literal starting point**, split into `node.proto`,
   `worker_service.proto`, `control_service.proto`, `watch.proto` under `proto/dagworker/v1/`
   in the real repo (buf's `PACKAGE_DIRECTORY_MATCH` wants small, package-per-directory files;
   the single-file version here is for legibility as a dossier centerpiece only).
3. **Make `CompleteNode`/`FailNode` idempotent per `task_token`** rather than copying Temporal's
   flat "already completed ⇒ NotFound" for that specific case (§10) — at-least-once RPC delivery
   makes a duplicate completion an expected event, not an error path, for this workload.
4. **Put `MaxConcurrentStreams` and matched client/server keepalive params in the reference
   worker SDK's defaults, not just in docs** (§8, §4) — the whole "HTTP/2 concurrency limit as
   the credit protocol" argument only holds if `dagworkerd` actually sets a deliberate limit
   instead of the effectively-unbounded default.
5. **Default the worker SDK's dialer to `dns:///` + `round_robin`, never a bare
   `host:port`/ClusterIP**, and set a `MaxConnectionAge` on the server out of the box (§15) —
   this is exactly the kind of footgun ("it works in dev with one replica, silently pins to one
   replica in prod with three") that should be closed by the SDK's defaults rather than left as
   an operator's homework.
6. **Bound the Watch event-retention window per scope** (a count or age cap, §6) and implement
   the `compacted_revision` reject path from day one, even before there's any realistic chance
   of hitting it at small scale — retrofitting "clients might have silently missed history" is
   much harder than shipping the resume-or-resync contract up front.
7. **Split `WorkerService` and `ControlService` mTLS/token issuance from day one** (§11) even if
   the very first deployment only has one caller wearing both hats — the credential boundary is
   cheap to establish now and expensive to retrofit once real worker fleets exist with
   long-lived certs already issued.
8. **Split `dagworker-proto`/`dagworkerd` into their own Go module(s)**, never a package
   subdirectory of the core `dagworker` module — a `go.mod` boundary is a build-time-enforced
   guarantee that `dagworker` core cannot accidentally gain a gRPC/`net/http` import edge,
   whereas an internal package boundary is only a lint convention someone can accidentally
   violate in a later PR.
9. **Commit generated Go code** (`gen/go/...`) to the repo (§14) and publish the module to the
   Buf Schema Registry for non-Go consumers on every merge to main — this is what actually
   delivers "Python/Node/Rust/Java workers can participate" rather than merely making it
   theoretically possible via a `.proto` file nobody outside the Go build can easily consume.
10. **Emit `dagworker.claim.outcome`-partitioned metrics, not raw RPC-duration histograms, on
    `ClaimNode`** from the first observability pass (§12) — this is a one-line change to make
    now versus a confusing "why does p99 latency look terrible" incident investigation later
    that turns out to be a perfectly healthy idle scope.

## Open questions

- **Exact `protovalidate` constraint DSL for `Duration` fields** (`gte`/`lte` nesting as
  sketched in §5) should be checked against the currently-pinned `protovalidate` release before
  landing — the example reflects the library's general shape, not a copy-pasted verified
  snippet, since the specific `DurationRules` field names weren't independently confirmed here.
- **Should `AddNodes`' inline `edges` field** (§5's atomicity rationale: attaching edges in the
  same call avoids a window where a node is briefly ready with a dependency missing) **be the
  *only* way to add edges for newly-created nodes**, with standalone `AddEdges` restricted to
  connecting already-existing nodes — or is a genuine cross-batch "add edges between two nodes
  that were each added in separate, already-committed `AddNodes` calls" a real enough use case
  to keep both paths equally supported? The proto in §5 allows both; the DAG-mutation ADR should
  pick a stance on which is the "blessed" path before client SDK ergonomics harden around it.
- **Where does the `Watch` event-retention bound live** — a fixed global config, a per-scope
  setting, or something storage-backend-dependent (Redis Streams' own trimming vs. a
  hand-rolled ring in the in-memory backend vs. a Postgres table with a cron-vacuumed TTL)?
  Each backend choice in the project's storage matrix implies a different natural
  compaction mechanism, and picking one uniform contract across all four (in-memory, Redis,
  PostgreSQL, memcached) needs its own short spec pass.
- **Does `FailNodeResponse.will_retry`/`next_attempt_at` duplicate information a worker could
  otherwise get from a subsequent `GetNode` or `Watch` event** — is exposing retry-scheduling
  outcome synchronously in the ack response worth the coupling to the retry-policy engine's
  internals, or should `FailNode` stay a pure ack and push retry visibility entirely through
  `Watch`?
- **HTTP/JSON transcoding surface**: this dossier specifies the gRPC wire protocol only; whether
  the HTTP/JSON adapter is `grpc-gateway`-style REST transcoding of this exact proto (annotated
  with `google.api.http` options) or a deliberately smaller, hand-designed REST surface (likely
  excluding `ClaimNode`'s long-poll shape, which maps awkwardly onto plain HTTP long-polling
  without the keepalive tooling gRPC provides) is an open design question for a companion
  dossier, not resolved here.

## Sources

- [Temporal `workflowservice/v1/service.proto`](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/service.proto)
- [Temporal `workflowservice/v1/request_response.proto`](https://github.com/temporalio/api/blob/master/temporal/api/workflowservice/v1/request_response.proto)
- [Temporal docs: Detecting Activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
- [etcd Learning: API](https://etcd.io/docs/v3.5/learning/api/)
- [etcd `etcdserverpb/rpc.proto`](https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto)
- [Nomad HTTP API: Blocking Queries](https://developer.hashicorp.com/nomad/api-docs#blocking-queries)
- [Buildbarn `remoteworker/remoteworker.proto`](https://github.com/buildbarn/bb-remote-execution/blob/master/pkg/proto/remoteworker/remoteworker.proto)
- [Bazel Remote Execution API v2 `remote_execution.proto`](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)
- [Envoy xDS protocol docs (ADS)](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol)
- [Kubernetes CRI `runtime/v1/api.proto`](https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto)
- [reactive-streams.org](https://www.reactive-streams.org/)
- [grpc.io: Deadlines guide](https://grpc.io/docs/guides/deadlines/)
- [grpc.io: Keepalive guide](https://grpc.io/docs/guides/keepalive/)
- [grpc-go `keepalive/keepalive.go`](https://github.com/grpc/grpc-go/blob/master/keepalive/keepalive.go)
- [grpc/grpc keepalive spec](https://github.com/grpc/grpc/blob/master/doc/keepalive.md)
- [grpc-go `server.go`](https://github.com/grpc/grpc-go/blob/master/server.go) (`GracefulStop`/`Stop`)
- [`google/rpc/error_details.proto`](https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto)
- [AIP-193: Errors](https://google.aip.dev/193)
- [AIP-190: Naming conventions](https://google.aip.dev/190)
- [grpc/grpc: Load Balancing doc](https://github.com/grpc/grpc/blob/master/doc/load-balancing.md)
- [Kubernetes blog: gRPC Load Balancing on Kubernetes without Tears](https://kubernetes.io/blog/2018/11/07/grpc-load-balancing-on-kubernetes-without-tears/)
- [grpc.io: Authentication guide](https://grpc.io/docs/guides/auth/)
- [OpenTelemetry Semantic Conventions: gRPC](https://opentelemetry.io/docs/specs/semconv/rpc/grpc/)
- [pkg.go.dev `google.golang.org/grpc/stats`](https://pkg.go.dev/google.golang.org/grpc/stats)
- [buf.build: Breaking change rules](https://buf.build/docs/breaking/rules/)
- [buf.build: Lint rules](https://buf.build/docs/lint/rules/)
- [buf.build: `buf.yaml` v2 configuration](https://buf.build/docs/configuration/v2/buf-yaml/)
- [buf.build: `buf.gen.yaml` v2 configuration](https://buf.build/docs/configuration/v2/buf-gen-yaml/)
- [bufbuild/protovalidate](https://github.com/bufbuild/protovalidate)
