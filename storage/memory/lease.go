package memory

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/internal/pq"
)

// clampAttempt narrows the lease epoch to the attempt counter's width. The two
// are the same number by design, and a node reaching four billion attempts is
// not a scenario worth representing -- but silently wrapping to zero would
// reset the retry budget, so it saturates instead.
func clampAttempt(epoch uint64) uint32 {
	if epoch > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(epoch)
}

// effectiveRetry folds a node's own retry policy over the scope's, field by
// field, so a node can override one setting without restating the others.
func (s *scope) effectiveRetry(h int32, cfg dw.ScopeConfig) (attempts uint32, base, maxDelay time.Duration) {
	r := &s.recs[h]
	attempts = cfg.MaxAttempts
	if r.retry.MaxAttempts > 0 {
		attempts = r.retry.MaxAttempts
	}
	b, m := cfg.RetryBaseDelay, cfg.RetryMaxDelay
	if r.retry.BaseDelay > 0 {
		b = r.retry.BaseDelay
	}
	if r.retry.MaxDelay > 0 {
		m = r.retry.MaxDelay
	}
	return attempts, b, m
}

// failAttempt records that an attempt did not succeed, and decides between
// another attempt and a terminal failure.
//
// It is the single path for every way an attempt can fail — a worker's Nack and
// a reclaimed lease alike — so the two can never diverge in how they count
// attempts, compute backoff, or fan out to successors.
func (s *scope) failAttempt(h int32, reason dw.Reason, msg string, cfg dw.ScopeConfig, now int64, out []dw.Effect) []dw.Effect {
	r := &s.recs[h]
	maxAttempts, base, maxDelay := s.effectiveRetry(h, cfg)

	s.leases.Remove(h)
	s.deadline[h] = 0

	if r.attempt >= maxAttempts {
		return s.terminate(h, dw.StatusError, reason, msg, out)
	}

	from := r.status
	delay := dw.Backoff(r.attempt, base, maxDelay, s.store.jitter)
	r.reason = reason
	r.message = msg
	r.worker = ""
	r.updatedAt = now
	s.schedule(h, now+int64(delay))
	return append(out, s.record(h, dw.EventTransition, from))
}

// reclaimExpired revokes every lease whose deadline has passed, up to limit.
//
// Correctness never depends on only one instance doing this. Every write it
// makes goes through the same fenced path a worker's own acknowledgement uses,
// so two instances reclaiming the same lease is wasted work, never a wrong
// answer.
func (s *scope) reclaimExpired(now int64, cfg dw.ScopeConfig, limit int, out []dw.Effect) ([]dw.Effect, int, bool) {
	reclaimed := 0
	for reclaimed < limit {
		h, ok := s.leases.Peek()
		if !ok || s.deadline[h] > now {
			return out, reclaimed, false
		}
		out = s.failAttempt(h, dw.ReasonTimeout,
			"the worker did not acknowledge before the lease deadline", cfg, now, out)
		reclaimed++
	}
	_, more := s.leases.Peek()
	return out, reclaimed, more && s.deadlineOfTop() <= now
}

func (s *scope) deadlineOfTop() int64 {
	h, ok := s.leases.Peek()
	if !ok {
		return 1<<63 - 1
	}
	return s.deadline[h]
}

// popReady takes the best claimable node for the requested kinds.
//
// With no kinds named it compares the head of every kind's queue, which is
// linear in the number of distinct kinds in the scope — not in the number of
// nodes. Kind is a partition key for a worker pool, so its cardinality is
// expected to be small; a caller that mints kinds per node has misunderstood
// the field and should use Labels instead.
func (s *scope) popReady(kinds []string) (int32, bool) {
	best := int32(-1)
	var bestHeap *pq.Heap

	consider := func(h *pq.Heap) {
		top, ok := h.Peek()
		if !ok {
			return
		}
		if best < 0 || s.recs[top].priority > s.recs[best].priority ||
			(s.recs[top].priority == s.recs[best].priority && s.fifo[top] < s.fifo[best]) {
			best, bestHeap = top, h
		}
	}

	if len(kinds) == 0 {
		for _, h := range s.ready {
			consider(h)
		}
	} else {
		for _, k := range kinds {
			if h, ok := s.ready[k]; ok {
				consider(h)
			}
		}
	}
	if best < 0 {
		return 0, false
	}
	bestHeap.Pop()
	return best, true
}

// Claim implements [dagworker.Store].
func (st *Store) Claim(_ context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
	s, err := st.scopeFor(req.Scope, false)
	if err != nil {
		return dw.ClaimResult{}, err
	}
	if s == nil {
		// A scope nobody has written to has no work. That is an ordinary
		// answer, not an error.
		return dw.ClaimResult{}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cfg := s.cfg.Resolved()
	now := s.now()

	var res dw.ClaimResult
	res.Effects, _, _ = s.reclaimExpired(now, cfg, cfg.SweepBatchSize, res.Effects)
	res.Effects = append(res.Effects, s.promoteScheduled(now)...)

	want := max(req.Max, 1)
	leaseFor := cfg.ClampLease(req.Timeout)

	for len(res.Leases) < want {
		if cfg.MaxInFlight > 0 && s.stats.InProgress >= uint64(cfg.MaxInFlight) {
			break
		}
		h, ok := s.popReady(req.Kinds)
		if !ok {
			break
		}

		r := &s.recs[h]
		from := r.status
		s.leaveBucket(h)
		r.epoch++
		r.attempt = clampAttempt(r.epoch)
		r.phase = dw.PhaseClaimed
		r.status = dw.StatusInProgress
		r.worker = req.WorkerID
		r.updatedAt = now
		s.deadline[h] = now + int64(leaseFor)
		s.leases.Push(h)
		s.enterBucket(h)

		res.Effects = append(res.Effects, s.record(h, dw.EventTransition, from))
		res.Leases = append(res.Leases, dw.Lease{
			Scope:    s.name,
			NodeID:   r.id,
			Epoch:    r.epoch,
			Deadline: unix(s.deadline[h]),
			Node:     s.snapshot(h),
		})
	}
	return res, nil
}

// Complete implements [dagworker.Store].
func (st *Store) Complete(_ context.Context, req dw.CompleteRequest) (dw.CompleteResult, error) {
	if !req.Lease.Valid() {
		return dw.CompleteResult{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	s, err := st.mustScope(req.Lease.Scope)
	if err != nil {
		return dw.CompleteResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.lookup(req.Lease.NodeID)
	if !ok {
		return dw.CompleteResult{}, dw.ErrNotFound
	}

	// The fencing check. A worker that was merely paused rather than dead
	// arrives here after its lease was reclaimed and reissued; its epoch no
	// longer matches and its write is refused instead of overwriting whatever
	// the current holder has since recorded.
	r := &s.recs[h]
	if r.phase != dw.PhaseClaimed || r.epoch != req.Lease.Epoch {
		return dw.CompleteResult{}, fmt.Errorf("%w: node %q is at epoch %d, lease presented %d",
			dw.ErrLeaseMismatch, req.Lease.NodeID, r.epoch, req.Lease.Epoch)
	}

	cfg := s.cfg.Resolved()
	now := s.now()

	var out dw.CompleteResult
	if req.Success {
		if len(req.Result) > cfg.PayloadCap {
			return dw.CompleteResult{}, &dw.PayloadTooLargeError{Size: len(req.Result), Cap: cfg.PayloadCap}
		}
		r.result = bytes.Clone(req.Result)
		r.worker = ""
		out.Effects = s.terminate(h, dw.StatusSuccess, dw.ReasonNone, "", nil)
	} else {
		reason := req.Reason
		if reason == dw.ReasonNone {
			reason = dw.ReasonWorkerError
		}
		if reason == dw.ReasonSkipped {
			// Skipping is a decision, not a fault: the worker looked and
			// concluded there was nothing to do. Retrying it would just reach
			// the same conclusion, so it is terminal on the first report. This
			// is the branch primitive the trigger rules exist to serve, and it
			// is the only way ReasonSkipped enters a graph.
			s.leases.Remove(h)
			s.deadline[h] = 0
			r.worker = ""
			out.Effects = s.terminate(h, dw.StatusError, dw.ReasonSkipped, req.Message, nil)
			s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
			return out, nil
		}
		out.Effects = s.failAttempt(h, reason, req.Message, cfg, now, nil)
		if s.recs[h].phase == dw.PhaseScheduled {
			out.Retrying = true
			out.NextAttemptAt = unix(s.readyAt[h])
		}
	}

	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return out, nil
}

// Extend implements [dagworker.Store].
func (st *Store) Extend(_ context.Context, req dw.ExtendRequest) (time.Time, error) {
	if !req.Lease.Valid() {
		return time.Time{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	s, err := st.mustScope(req.Lease.Scope)
	if err != nil {
		return time.Time{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.lookup(req.Lease.NodeID)
	if !ok {
		return time.Time{}, dw.ErrNotFound
	}
	r := &s.recs[h]
	if r.phase != dw.PhaseClaimed || r.epoch != req.Lease.Epoch {
		return time.Time{}, fmt.Errorf("%w: node %q is at epoch %d, lease presented %d",
			dw.ErrLeaseMismatch, req.Lease.NodeID, r.epoch, req.Lease.Epoch)
	}

	cfg := s.cfg.Resolved()
	// Measured from now, not from the original grant: a worker asking for more
	// time means "another N seconds", not "N seconds from whenever I started".
	s.deadline[h] = s.now() + int64(cfg.ClampLease(req.Timeout))
	s.leases.Fix(h)
	// Deliberately no change to status, attempt or sequence: extending a lease
	// is not an event in the node's life, and emitting one would make a
	// heartbeat indistinguishable from progress.
	return unix(s.deadline[h]), nil
}

// Sweep implements [dagworker.Store].
func (st *Store) Sweep(_ context.Context, name dw.Scope, limit int) (dw.SweepResult, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return dw.SweepResult{}, err
	}
	if s == nil {
		return dw.SweepResult{}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cfg := s.cfg.Resolved()
	if limit <= 0 {
		limit = cfg.SweepBatchSize
	}
	now := s.now()

	var res dw.SweepResult
	res.Effects, res.Reclaimed, res.More = s.reclaimExpired(now, cfg, limit, res.Effects)
	res.Effects = append(res.Effects, s.promoteScheduled(now)...)
	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return res, nil
}
