---
title: Writing workers, in process and over the network
description: In-process worker pools, retries and backoff, heartbeats for long work, and the gRPC and HTTP adapters for workers elsewhere — plus the one mistake that expires a job mid-run.
---

The library's job is the graph. Yours is the worker. This page covers the
shape of that code for three deployment shapes: goroutines in the same
process as the `Manager`, a pool of them with retries and heartbeats, and
workers that live in another process — or aren't written in Go at all —
talking over the gRPC or HTTP adapters.

## In-process workers

The loop is always the same four calls: claim, do the work, ack or nack.

```go
for {
	lease, err := m.Claim(ctx, "release") // blocks until something is ready
	if err != nil {
		return err // ctx ended, or the Manager is closing
	}
	result, err := doTheWork(ctx, lease.Node)
	if err != nil {
		if _, err := m.Nack(ctx, lease, err); err != nil {
			return err
		}
		continue
	}
	if err := m.Ack(ctx, lease, result); err != nil {
		return err
	}
}
```

Run as many of these as you have concurrency for, in as many goroutines or
processes as you like — the store's own atomic claim is what makes two
workers racing for the same node safe, with nothing to coordinate on your
side. `Claim` blocks; `TryClaim` doesn't, returning `ErrNoWork` immediately
when nothing is ready, which is the shape a batch worker that should stop
once idle wants instead.

## Retries and backoff

Whether a failed attempt gets retried is the scope's policy, never the
worker's decision — the worker just reports what happened:

```go
outcome, err := m.Nack(ctx, lease, cause)
if err != nil {
	return err // e.g. ErrLeaseMismatch: someone else already has this node
}
if outcome.Retrying {
	log.Printf("will retry at %s", outcome.NextAttemptAt)
} else {
	log.Printf("failed permanently: %s", cause)
}
```

The decision comes from `MaxAttempts`, `RetryBaseDelay`, and `RetryMaxDelay`
— set per scope via `ScopeConfig`, or per node via `NodeSpec.Retry`, which
overrides the scope's defaults field by field. The backoff itself is full
jitter: a delay drawn uniformly from `[0, min(MaxDelay, BaseDelay·2^attempt))`.
It beats both plain exponential backoff and "equal jitter" on aggregate
contention — a fleet retrying the same class of failure spreads out instead
of clustering — at the cost of an occasional near-zero-delay retry, which is
an accepted trade, not a bug.

`Attempt` and the lease epoch are the same field. A retry is a new attempt
on the *same node*, never a new one, and the fenced compare-and-swap that
protects `Ack`/`Nack` (see [Concepts](/dagworker/guide/concepts/)) is keyed
on exactly this number — which is why a stale worker can never distinguish
"I was too slow" from "someone else already handled this." Both cases lead
to the same correct action (stop), so the library doesn't bother telling
them apart.

## Heartbeats for long-running work

A lease has a deadline. Work that might run longer than that deadline needs
to prove it's still alive periodically, or the sweeper will reclaim the node
out from under it. That's what `Extend` is for — a call distinct from
`Ack`/`Nack` specifically so that "I'm still here" and "I'm finished" can
never be conflated into one signal:

```go
func runWithHeartbeat(ctx context.Context, m *dagworker.Manager, lease dagworker.Lease, every time.Duration, handle func(context.Context, dagworker.Node) ([]byte, error)) {
	stop := make(chan struct{})
	current := lease
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				extended, err := m.Extend(ctx, current, 0) // 0 reuses the scope default
				if err != nil {
					return // lease is gone; the handler's own Ack/Nack will be refused
				}
				current = extended
			}
		}
	}()
	defer close(stop)

	result, err := handle(ctx, lease.Node)
	if err != nil {
		_, _ = m.Nack(ctx, current, err)
		return
	}
	_ = m.Ack(ctx, current, result)
}
```

Pick `every` comfortably shorter than the lease timeout — a third of it is a
reasonable starting point — so one slow or dropped heartbeat still leaves
margin before the deadline. If `Extend` ever fails, don't panic and don't
retry aggressively: it means the lease is already gone, and the handler's
eventual `Ack`/`Nack` will be refused by the same fencing check, which is
the correct outcome. This is exactly the pattern the project's own
end-to-end worker pool uses internally, along with catching a handler panic
and reporting it as a failed attempt rather than taking the whole pool down.

## Workers that aren't in this process

If every worker in your deployment lives in the same Go process as the
`Manager`, you don't need anything below this point — everything above is
the whole story. Reach for the network adapters when workers are separate
processes, written in a language other than Go, or you simply want
dagworker running as its own service (`dagworkerd` — see [Running it in
production](/dagworker/guide/operations/)).

Both adapters expose the identical claim → heartbeat → complete/fail/skip
shape as the in-process loop above; only the transport differs.

**gRPC.** `adapters/grpc/client` is the reference Go SDK, though the wire
protocol is a committed `.proto` any language's gRPC stack can consume:

```go
conn, _ := client.Dial("dns:///dagworkerd.svc.cluster.local:443", insecureOrTLSCreds)
w := client.NewWorker(conn, "release", client.WithWorkerID("worker-1"))

err := w.Run(ctx, func(ctx context.Context, n *pb.Node) client.Outcome {
	result, err := doTheWork(ctx, n)
	if err != nil {
		return client.Fail(err.Error())
	}
	return client.Complete(result)
})
```

`Worker.Run` owns the heartbeat loop for you — it calls `ExtendLease`
automatically at roughly half the remaining lease, on its own schedule, so
your handler function never has to think about it. One `Worker` keeps
exactly one `ClaimNode` call outstanding at a time, which is dagworker's own
capacity signal: a host wanting N-way concurrency runs N `Worker`s, not one
`Worker` claiming N nodes at once.

**HTTP/JSON.** `adapters/http/client` is the equivalent for a plain HTTP
client, with the same auto-renewing shape available via `ClaimAndRenew`:

```go
c := client.New("https://dagworkerd.example.com/v1")
handles, _ := c.ClaimAndRenew(ctx, "release", client.ClaimOptions{Wait: 30 * time.Second}, 0)
for _, h := range handles {
	result, err := doTheWork(ctx, h.Lease().Node)
	if err != nil {
		_, _ = h.Fail(ctx, err.Error())
		continue
	}
	_, _ = h.Complete(ctx, result)
}
```

Both are long-poll designs, not push: the client's claim call blocks
server-side for up to a bounded wait (clamped to 60 seconds on the HTTP
adapter regardless of what's requested) and returns empty rather than an
error when nothing was ready in that window — having no work is ordinary,
never a failure to handle specially. This is deliberate: pushing work at a
pool whose free capacity the server can't see is how a queue overwhelms its
own workers. One outstanding claim per worker slot makes the transport's own
flow control — HTTP/2's stream limit, or simply "how many `Worker`s you
run" — the capacity signal instead.

Both adapters map the same error taxonomy identically — `ErrLeaseMismatch`
always becomes `ABORTED`/409 with a `lease-superseded` problem type,
`ErrNotFound` always becomes `NOT_FOUND`/404, and so on through the full
table in the [adapter contract](/dagworker/reference/adapters/) — so client
code written against one maps cleanly onto the other if you ever need to
switch.

## The trap: the lease deadline is not the request deadline

This is the single most common mistake in a poll-then-work client, and it's
worth stating as plainly as the adapter contract itself does: **a claim's
lease lives in storage and outlives the RPC that granted it, the connection
it arrived on, and the process that served it.** It has no relationship to
any `context.Context` deadline your claim call happened to carry.

```go
// WRONG — reuses the claim call's context for the eventual acknowledgement.
// The moment that context's deadline passes, mid-job, Complete fails with
// no relation whatsoever to whether the actual work succeeded.
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
lease, _ := client.Claim(ctx, scope, opts)
result := doSlowWork(lease.Node) // takes five minutes
client.Complete(ctx, scope, lease.ID, result) // ctx is long dead by now
```

```go
// RIGHT — the claim call and the eventual report each get their own,
// independent context. Neither is derived from the other.
claimCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
lease, err := client.Claim(claimCtx, scope, opts)
cancel()

result := doSlowWork(lease.Node)

completeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
client.Complete(completeCtx, scope, lease.ID, result)
```

Both reference clients here follow this rule structurally rather than by
caller discipline: every heartbeat and every final report is rooted at its
own fresh, short-lived context, never at the one that made the original
claim call or the one governing your handler function. If you're writing a
worker client from scratch against the gRPC or HTTP wire protocol directly,
this is the one rule to enforce as a matter of code structure, not
convention — the [adapter contract](/dagworker/reference/adapters/) states
it as a `MUST` for exactly this reason.
