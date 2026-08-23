package dagworker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ClaimOption configures a claim.
type ClaimOption interface{ applyClaim(*ClaimRequest) }

type claimOptionFunc func(*ClaimRequest)

func (f claimOptionFunc) applyClaim(r *ClaimRequest) { f(r) }

// OfKind restricts the claim to nodes of these kinds. With none given, any kind
// is eligible.
func OfKind(kinds ...string) ClaimOption {
	return claimOptionFunc(func(r *ClaimRequest) { r.Kinds = append(r.Kinds, kinds...) })
}

// WithLeaseTimeout sets how long this claim's lease lasts. The scope clamps it
// to its own bounds, so a request outside them is corrected rather than
// refused. Zero uses the scope's default.
func WithLeaseTimeout(d time.Duration) ClaimOption {
	return claimOptionFunc(func(r *ClaimRequest) { r.Timeout = d })
}

// AsWorker records who claimed the node, for observability. It has no bearing
// on correctness: the right to complete a node comes from the lease epoch, not
// from an identity anyone could assert.
func AsWorker(id string) ClaimOption {
	return claimOptionFunc(func(r *ClaimRequest) { r.WorkerID = id })
}

// Poll interval bounds for the blocking-claim fallback. The floor keeps an idle
// fleet from hammering the backend; the ceiling bounds how long a missed
// doorbell signal can delay a worker.
const (
	minPollInterval = 50 * time.Millisecond
	maxPollInterval = 2 * time.Second
)

func (m *Manager) buildClaim(scope Scope, maxNodes int, opts []ClaimOption) (ClaimRequest, error) {
	req := ClaimRequest{Scope: scope, Max: maxNodes}
	for _, o := range opts {
		if o == nil {
			return req, fmt.Errorf("%w: nil claim option", ErrInvalidArgument)
		}
		o.applyClaim(&req)
	}
	for _, k := range req.Kinds {
		if err := validateKind(k); err != nil {
			return req, err
		}
	}
	if req.Timeout < 0 {
		return req, invalidArg("lease timeout", "must not be negative")
	}
	return req, nil
}

// TryClaim takes one ready node without waiting. It returns [ErrNoWork] when
// nothing is ready, which is an ordinary outcome rather than a failure — check
// for it with [errors.Is] and go do something else.
func (m *Manager) TryClaim(ctx context.Context, scope Scope, opts ...ClaimOption) (Lease, error) {
	leases, err := m.ClaimBatch(ctx, scope, 1, opts...)
	if err != nil {
		return Lease{}, err
	}
	if len(leases) == 0 {
		return Lease{}, ErrNoWork
	}
	return leases[0], nil
}

// ClaimBatch takes up to n ready nodes without waiting, returning however many
// were available — possibly none.
//
// Batching matters on a networked backend: one round trip that returns ten
// nodes costs about what one that returns one costs, so a worker pool that
// claims singly spends most of its time waiting on the network rather than
// working.
func (m *Manager) ClaimBatch(ctx context.Context, scope Scope, n int, opts ...ClaimOption) ([]Lease, error) {
	if err := m.check(scope); err != nil {
		return nil, err
	}
	req, err := m.buildClaim(scope, max(n, 1), opts)
	if err != nil {
		return nil, err
	}
	// Clamp before the store sees it. A backend that grants a batch atomically
	// cannot defend itself mid-operation, and the network adapters hand this
	// number straight through from a caller.
	if cfg, cerr := m.store.ScopeConfig(ctx, scope); cerr == nil {
		req.Max = cfg.ClampClaimBatch(req.Max)
	} else {
		req.Max = ScopeConfig{}.ClampClaimBatch(req.Max)
	}
	res, err := m.store.Claim(ctx, req)
	m.publish(scope, res.Effects)
	if err != nil {
		return nil, err
	}
	return res.Leases, nil
}

// Claim waits for a ready node and takes it. It returns only when it has a
// lease, or when ctx ends.
//
// The wakeup protocol has three parts, and all three are necessary:
//
//   - An immediate attempt, because the common case is that work is already
//     waiting and a worker should not pay a scheduling delay to discover it.
//   - A doorbell, when the backend offers one, so an idle worker costs nothing
//     while it waits.
//   - A jittered poll, always. A doorbell is advisory: a signal published
//     between the failed claim and the wait is lost on any backend whose
//     notification is edge-triggered, and Postgres' LISTEN/NOTIFY does not
//     survive a dropped connection at all. Without the poll those cases are a
//     hang; with it they cost one interval of latency.
//
// The jitter matters at scale: a fleet that started together and polls on a
// fixed interval stays synchronised and arrives in lockstep bursts forever.
func (m *Manager) Claim(ctx context.Context, scope Scope, opts ...ClaimOption) (Lease, error) {
	if err := m.check(scope); err != nil {
		return Lease{}, err
	}
	req, err := m.buildClaim(scope, 1, opts)
	if err != nil {
		return Lease{}, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return Lease{}, err
		}
		if m.isClosed() {
			return Lease{}, ErrClosed
		}

		res, err := m.store.Claim(ctx, req)
		m.publish(scope, res.Effects)
		if err != nil {
			return Lease{}, err
		}
		if len(res.Leases) > 0 {
			return res.Leases[0], nil
		}
		if err := m.waitForWork(ctx, scope, req.Kinds); err != nil {
			return Lease{}, err
		}
	}
}

// waitForWork blocks until work may be available, the poll interval elapses, or
// the context ends. Returning nil means "try again", never "work is there".
func (m *Manager) waitForWork(ctx context.Context, scope Scope, kinds []string) error {
	wait := jitter(m.pollInterval())

	if d, ok := m.store.(Doorbell); ok && m.caps.Has(CapDoorbell) {
		// Bound the wait even with a doorbell, so a signal lost in the gap
		// between the failed claim and this call costs one interval rather
		// than blocking forever.
		wctx, cancel := context.WithTimeout(ctx, wait)
		defer cancel()

		switch err := d.WaitForWork(wctx, scope, kinds); {
		case err == nil, errors.Is(err, context.DeadlineExceeded):
			return ctx.Err()
		case errors.Is(err, context.Canceled):
			return ctx.Err()
		case errors.Is(err, ErrClosed):
			return ErrClosed
		default:
			// A doorbell that fails is a degraded doorbell, not a broken
			// claim — but it must not become a busy loop. WaitForWork can
			// return an error immediately and forever (a Redis pool with no
			// free connection, a Postgres LISTEN that cannot be established),
			// and returning here would send the caller straight back into
			// another claim with no wait at all: measured at 1.9 million
			// claims a second against a broken doorbell, which turns a
			// degraded notification path into a self-inflicted outage.
			//
			// So fall through to the timed wait below, which is the fallback
			// the doorbell was an optimisation over in the first place.
			m.cfg.logger.WarnContext(ctx, "dagworker: doorbell failed, falling back to polling",
				"scope", scope, "error", err)
		}
	}

	select {
	case <-m.cfg.clock.After(wait):
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closed:
		return ErrClosed
	}
}

func (m *Manager) pollInterval() time.Duration {
	d := m.cfg.pollInterval
	if d <= 0 {
		d = maxPollInterval
	}
	return min(max(d, minPollInterval), maxPollInterval)
}

// Ack reports that the worker completed the node successfully.
//
// It is fenced on the lease: if the lease was reclaimed and reissued while the
// worker was busy — because it stalled, or its host paused — this returns
// [ErrLeaseMismatch] and the write is refused. That error is never retryable,
// and it means the work may already have been redone by someone else.
func (m *Manager) Ack(ctx context.Context, lease Lease, result []byte) error {
	return m.complete(ctx, CompleteRequest{Lease: lease, Success: true, Result: result})
}

// AttemptResult reports what recording a failed attempt did.
//
// The retry decision belongs to the scope's policy, not the worker, so the
// worker has no way to predict it -- and reading it back with a second call
// would be a different question asked at a different instant. This comes from
// the same atomic operation that made the decision.
type AttemptResult struct {
	// Retrying is true when the failure was recorded as another attempt and the
	// node returned to [StatusNew], false when it became terminal.
	Retrying bool
	// NextAttemptAt is when a retrying node becomes claimable again. Zero when
	// Retrying is false.
	NextAttemptAt time.Time
}

// Nack reports that the attempt failed. Whether that ends the node or schedules
// another attempt is the scope's retry policy, not the worker's decision; the
// returned [AttemptResult] says which happened.
func (m *Manager) Nack(ctx context.Context, lease Lease, cause error) (AttemptResult, error) {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	res, err := m.completeWithResult(ctx, CompleteRequest{
		Lease: lease, Success: false, Reason: ReasonWorkerError, Message: msg,
	})
	return AttemptResult{Retrying: res.Retrying, NextAttemptAt: res.NextAttemptAt}, err
}

// Skip reports that there was nothing to do. Unlike [Manager.Nack] this is
// terminal on the first report — a retry would reach the same conclusion — and
// successors distinguish it from a genuine failure through their trigger rule:
// [TriggerNoneFailed] runs after a skipped predecessor, [TriggerAllSuccess]
// does not. This is the branch primitive.
func (m *Manager) Skip(ctx context.Context, lease Lease, reason string) error {
	return m.complete(ctx, CompleteRequest{
		Lease: lease, Success: false, Reason: ReasonSkipped, Message: reason,
	})
}

func (m *Manager) complete(ctx context.Context, req CompleteRequest) error {
	_, err := m.completeWithResult(ctx, req)
	return err
}

func (m *Manager) completeWithResult(ctx context.Context, req CompleteRequest) (CompleteResult, error) {
	if m.isClosed() {
		return CompleteResult{}, ErrClosed
	}
	if !req.Lease.Valid() {
		return CompleteResult{}, invalidArg("lease", "is not one this library issued")
	}
	res, err := m.store.Complete(ctx, req)
	m.publish(req.Lease.Scope, res.Effects)
	return res, err
}

// Extend moves a lease's deadline out by d, measured from now rather than from
// the original grant. Long-running work calls it periodically to prove it is
// still alive.
//
// It is deliberately a different operation from [Manager.Ack]: conflating "I am
// still here" with "I am finished" is how a liveness signal ends up being read
// as progress. It is fenced on the same epoch, so a worker whose lease was
// already reclaimed learns so here rather than at the end of a long job.
func (m *Manager) Extend(ctx context.Context, lease Lease, d time.Duration) (Lease, error) {
	if m.isClosed() {
		return lease, ErrClosed
	}
	if !lease.Valid() {
		return lease, invalidArg("lease", "is not one this library issued")
	}
	if d < 0 {
		return lease, invalidArg("extension", "must not be negative")
	}
	deadline, err := m.store.Extend(ctx, ExtendRequest{Lease: lease, Timeout: d})
	if err != nil {
		return lease, err
	}
	lease.Deadline = deadline
	return lease, nil
}
