package memory

import (
	"bytes"
	"maps"
	"sync"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/internal/pq"
)

// predEdge is one incoming dependency. Satisfaction is tracked per edge rather
// than as a bare counter so that it is addressable: removing an edge, or
// re-evaluating after a predecessor changes outcome, needs to know *which*
// dependency moved, which a counter cannot express. It is also idempotent by
// construction — marking an already-satisfied edge satisfied is a no-op — which
// is what makes the fan-out safe to repeat.
type predEdge struct {
	from      int32
	satisfied bool
}

// nodeRec is one node's record. Records live in a single contiguous slice, not
// as individually allocated objects: at a million nodes, one allocation per
// node is a million pointers the garbage collector walks on every cycle. The
// fields the claim and sweep paths touch on every iteration — rank, deadline,
// ready time, insertion order — are kept out of this struct entirely, in
// pointer-free parallel arrays, so those loops never walk pointer-bearing
// memory at all.
type nodeRec struct {
	id       dw.NodeID
	kind     string
	message  string
	worker   string
	payload  []byte
	result   []byte
	labels   map[string]string
	retry    dw.RetryPolicy
	deps     dw.DepCounts
	seq      dw.Seq
	epoch    uint64
	attempt  uint32
	priority int16
	status   dw.Status
	reason   dw.Reason
	phase    dw.Phase
	trigger  dw.TriggerRule
	alive    bool

	createdAt int64
	updatedAt int64
}

// scope holds one namespace's entire graph. All of it is guarded by one
// RWMutex.
//
// One lock per scope, rather than striping within a scope, is a deliberate
// choice. The storage port requires that completing a node — writing its
// terminal state, marking each out-edge satisfied, re-evaluating every
// successor's trigger rule, and pushing the newly claimable ones onto the ready
// set — be indivisible. Striping by node would make that fan-out span several
// stripes and reintroduce lock ordering as a correctness problem, which is
// exactly the deadlock class that recurs in schedulers that try it. Scopes are
// independent, so parallelism comes from using more than one of them; within a
// scope, every operation is O(1) or O(log n) under the lock, so the critical
// section is short by construction.
type scope struct {
	mu sync.RWMutex

	name  dw.Scope
	store *Store

	cfg    dw.ScopeConfig
	sealed bool

	index map[dw.NodeID]int32
	recs  []nodeRec
	free  []int32

	succ [][]int32
	pred [][]predEdge

	// Pointer-free parallel arrays, indexed by handle.
	ord      []int64 // topological rank
	deadline []int64 // lease expiry, unix nanos; 0 when unclaimed
	readyAt  []int64 // retry release time, unix nanos; 0 when not scheduled
	fifo     []uint64

	nextOrd  int64
	nextFifo uint64

	ready     map[string]*pq.Heap // one per node kind
	leases    *pq.Heap            // ordered by deadline
	scheduled *pq.Heap            // ordered by readyAt

	stats dw.ScopeStats

	// doorbell is closed and replaced whenever a node becomes claimable, so a
	// blocking claim can wait on it instead of polling.
	doorbell chan struct{}

	// The event log is a bounded ring. logStart is the cursor of log[0]; a
	// subscriber asking for anything older is told its cursor expired rather
	// than being silently given an incomplete stream.
	cursor   dw.Cursor
	log      []dw.Event
	logStart dw.Cursor
	logCap   int
	logBell  chan struct{}
}

func newScope(name dw.Scope, st *Store, cfg dw.ScopeConfig) *scope {
	s := &scope{
		name:     name,
		store:    st,
		cfg:      cfg,
		index:    make(map[dw.NodeID]int32),
		ready:    make(map[string]*pq.Heap),
		doorbell: make(chan struct{}),
		logBell:  make(chan struct{}),
		logCap:   st.logCap,
		logStart: 1,
		nextOrd:  1,
	}
	s.leases = pq.New(func(a, b int32) bool { return s.deadline[a] < s.deadline[b] }, 0)
	s.scheduled = pq.New(func(a, b int32) bool { return s.readyAt[a] < s.readyAt[b] }, 0)
	return s
}

// readyHeap returns the ready set for a node kind, creating it on first use.
// Ordering is priority descending, then insertion order ascending, so a
// higher-priority node always wins and equal priorities are served fairly.
func (s *scope) readyHeap(kind string) *pq.Heap {
	h, ok := s.ready[kind]
	if !ok {
		h = pq.New(func(a, b int32) bool {
			if s.recs[a].priority != s.recs[b].priority {
				return s.recs[a].priority > s.recs[b].priority
			}
			return s.fifo[a] < s.fifo[b]
		}, 0)
		s.ready[kind] = h
	}
	return h
}

func (s *scope) now() int64 { return s.store.clock.Now().UnixNano() }

// alloc reserves a handle, reusing a freed slot when one is available so that a
// long-lived scope under churn does not grow its arrays without bound.
func (s *scope) alloc() int32 {
	if n := len(s.free); n > 0 {
		h := s.free[n-1]
		s.free = s.free[:n-1]
		s.recs[h] = nodeRec{}
		s.succ[h] = s.succ[h][:0]
		s.pred[h] = s.pred[h][:0]
		s.ord[h], s.deadline[h], s.readyAt[h], s.fifo[h] = 0, 0, 0, 0
		return h
	}
	h := int32(len(s.recs))
	s.recs = append(s.recs, nodeRec{})
	s.succ = append(s.succ, nil)
	s.pred = append(s.pred, nil)
	s.ord = append(s.ord, 0)
	s.deadline = append(s.deadline, 0)
	s.readyAt = append(s.readyAt, 0)
	s.fifo = append(s.fifo, 0)
	return h
}

func (s *scope) release(h int32) {
	s.recs[h] = nodeRec{}
	s.succ[h] = s.succ[h][:0]
	s.pred[h] = s.pred[h][:0]
	s.free = append(s.free, h)
}

func (s *scope) lookup(id dw.NodeID) (int32, bool) {
	h, ok := s.index[id]
	return h, ok
}

// ---------------------------------------------------------------- statistics

// bucket returns the counter a node currently belongs to. Every count is
// maintained incrementally here; nothing in this backend ever derives a
// statistic by scanning, because ScopeStats is contractually O(1).
func (s *scope) bucket(r *nodeRec) *uint64 {
	switch r.phase {
	case dw.PhaseBlocked:
		return &s.stats.Blocked
	case dw.PhaseScheduled:
		return &s.stats.Scheduled
	case dw.PhaseReady:
		return &s.stats.Ready
	case dw.PhaseClaimed:
		return &s.stats.InProgress
	default:
		if r.status == dw.StatusSuccess {
			return &s.stats.Succeeded
		}
		return &s.stats.Failed
	}
}

func (s *scope) leaveBucket(h int32) { *s.bucket(&s.recs[h])-- }
func (s *scope) enterBucket(h int32) { *s.bucket(&s.recs[h])++ }

// ---------------------------------------------------------------- scheduling

// makeReady moves a node into the ready set. It is the only path by which a
// node becomes claimable.
func (s *scope) makeReady(h int32) {
	r := &s.recs[h]
	s.leaveBucket(h)
	r.phase = dw.PhaseReady
	r.status = dw.StatusNew
	s.readyAt[h] = 0
	s.scheduled.Remove(h)
	s.nextFifo++
	s.fifo[h] = s.nextFifo
	s.readyHeap(r.kind).Push(h)
	s.enterBucket(h)
	s.ringDoorbell()
}

// makeBlocked pulls a node out of the ready set because a new dependency was
// added. Doing this in the same critical section as the edge insert is what
// stops a worker claiming the node through the gap.
func (s *scope) makeBlocked(h int32) {
	r := &s.recs[h]
	s.leaveBucket(h)
	s.readyHeap(r.kind).Remove(h)
	s.scheduled.Remove(h)
	s.readyAt[h] = 0
	r.phase = dw.PhaseBlocked
	r.status = dw.StatusNew
	s.enterBucket(h)
}

// schedule parks a node until a retry backoff elapses.
func (s *scope) schedule(h int32, at int64) {
	r := &s.recs[h]
	s.leaveBucket(h)
	s.readyHeap(r.kind).Remove(h)
	r.phase = dw.PhaseScheduled
	r.status = dw.StatusNew
	s.readyAt[h] = at
	s.scheduled.Push(h)
	s.enterBucket(h)
}

// promoteScheduled releases every node whose backoff has elapsed. It runs on
// the claim and sweep paths, so a retry becomes visible without depending on a
// timer having fired.
func (s *scope) promoteScheduled(now int64) []dw.Effect {
	var effects []dw.Effect
	for {
		h, ok := s.scheduled.Peek()
		if !ok || s.readyAt[h] > now {
			return effects
		}
		s.scheduled.Pop()
		s.makeReady(h)
		effects = append(effects, s.record(h, dw.EventReady, dw.StatusNew))
	}
}

// settle applies a node's trigger rule to its current dependency counts and
// moves it to whichever state the rule implies. It is called after any change
// to a node's dependencies, and it is idempotent.
//
// The returned effects include the node's own transition and, when the node
// became terminal because its rule can never be satisfied, the cascade into its
// own successors — which is what stops a graph from ever containing a node that
// waits forever on a dependency that will not resolve.
func (s *scope) settle(h int32, out []dw.Effect) []dw.Effect {
	r := &s.recs[h]
	if r.phase != dw.PhaseBlocked && r.phase != dw.PhaseReady {
		return out
	}
	switch {
	case r.deps.Unsatisfiable(r.trigger):
		reason := r.deps.TerminalReason()
		return s.terminate(h, dw.StatusError, reason, terminalMessage(reason), out)
	case r.deps.Ready(r.trigger):
		if r.phase != dw.PhaseReady {
			s.makeReady(h)
			out = append(out, s.record(h, dw.EventReady, dw.StatusNew))
		}
	default:
		if r.phase != dw.PhaseBlocked {
			s.makeBlocked(h)
		}
	}
	return out
}

func terminalMessage(r dw.Reason) string {
	switch r {
	case dw.ReasonUpstreamFailed:
		return "a predecessor failed and the trigger rule can no longer be satisfied"
	case dw.ReasonSkipped:
		return "the trigger rule can no longer be satisfied"
	case dw.ReasonRemoved:
		return "a predecessor was removed"
	case dw.ReasonCancelled:
		return "cancelled"
	default:
		return ""
	}
}

// terminate drives a node to a final status and propagates the consequence
// through the graph.
//
// The walk is a queue, not recursion: a deep dependency chain is exactly the
// shape that would overflow the stack, and a scheduler should not have a
// maximum graph depth imposed by an implementation detail.
//
// Propagation is not "fail everything downstream". Each successor is told that
// one of its dependencies resolved, and then its own trigger rule decides:
// TriggerAllDone successors become claimable on a failed predecessor, while
// TriggerAllSuccess successors cannot and are terminated in turn. This is why
// the same function serves both Ack and Nack.
func (s *scope) terminate(root int32, status dw.Status, reason dw.Reason, msg string, out []dw.Effect) []dw.Effect {
	type item struct {
		h      int32
		status dw.Status
		reason dw.Reason
		msg    string
	}
	queue := []item{{h: root, status: status, reason: reason, msg: msg}}

	for len(queue) > 0 {
		it := queue[0]
		queue = queue[1:]

		r := &s.recs[it.h]
		if !r.alive || r.phase == dw.PhaseDone {
			continue
		}

		from := r.status
		s.leaveBucket(it.h)
		s.readyHeap(r.kind).Remove(it.h)
		s.scheduled.Remove(it.h)
		s.leases.Remove(it.h)
		s.deadline[it.h] = 0
		s.readyAt[it.h] = 0
		r.phase = dw.PhaseDone
		r.status = it.status
		r.reason = it.reason
		r.message = it.msg
		r.updatedAt = s.now()
		s.enterBucket(it.h)
		out = append(out, s.record(it.h, dw.EventTransition, from))

		doneStatus, doneReason := r.status, r.reason
		for _, w := range s.succ[it.h] {
			if !s.markSatisfied(w, it.h, doneStatus, doneReason) {
				continue
			}
			wr := &s.recs[w]
			if wr.phase != dw.PhaseBlocked && wr.phase != dw.PhaseReady {
				continue
			}
			switch {
			case wr.deps.Unsatisfiable(wr.trigger):
				rs := wr.deps.TerminalReason()
				queue = append(queue, item{h: w, status: dw.StatusError, reason: rs, msg: terminalMessage(rs)})
			case wr.deps.Ready(wr.trigger):
				if wr.phase != dw.PhaseReady {
					s.makeReady(w)
					out = append(out, s.record(w, dw.EventReady, dw.StatusNew))
				}
			}
		}
	}
	return out
}

// markSatisfied records that one incoming edge resolved and updates the
// successor's dependency tally. It reports whether anything changed, so a
// repeated fan-out costs a comparison rather than a double count.
func (s *scope) markSatisfied(succHandle, predHandle int32, status dw.Status, reason dw.Reason) bool {
	edges := s.pred[succHandle]
	for i := range edges {
		if edges[i].from != predHandle || edges[i].satisfied {
			continue
		}
		edges[i].satisfied = true
		d := &s.recs[succHandle].deps
		if d.Unsatisfied > 0 {
			d.Unsatisfied--
		}
		switch {
		case status == dw.StatusSuccess:
			d.Succeeded++
		case reason == dw.ReasonSkipped:
			d.Skipped++
		default:
			d.Failed++
		}
		return true
	}
	return false
}

// ---------------------------------------------------------------- edges

func (s *scope) hasEdge(from, to int32) bool {
	for _, e := range s.pred[to] {
		if e.from == from {
			return true
		}
	}
	return false
}

// linkEdge records a dependency and updates the successor's tally. The caller
// has already checked the ordering invariant and the terminal-node rule.
func (s *scope) linkEdge(from, to int32) {
	pr := &s.recs[from]
	satisfied := pr.phase == dw.PhaseDone
	s.pred[to] = append(s.pred[to], predEdge{from: from, satisfied: satisfied})
	s.succ[from] = append(s.succ[from], to)

	d := &s.recs[to].deps
	switch {
	case !satisfied:
		d.Unsatisfied++
	case pr.status == dw.StatusSuccess:
		d.Succeeded++
	case pr.reason == dw.ReasonSkipped:
		d.Skipped++
	default:
		d.Failed++
	}
}

func (s *scope) unlinkEdge(from, to int32) bool {
	edges := s.pred[to]
	idx := -1
	for i := range edges {
		if edges[i].from == from {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false
	}
	e := edges[idx]
	s.pred[to] = append(edges[:idx], edges[idx+1:]...)

	d := &s.recs[to].deps
	switch {
	case !e.satisfied:
		if d.Unsatisfied > 0 {
			d.Unsatisfied--
		}
	case s.recs[from].status == dw.StatusSuccess:
		if d.Succeeded > 0 {
			d.Succeeded--
		}
	case s.recs[from].reason == dw.ReasonSkipped:
		if d.Skipped > 0 {
			d.Skipped--
		}
	default:
		if d.Failed > 0 {
			d.Failed--
		}
	}

	outs := s.succ[from]
	for i, w := range outs {
		if w == to {
			s.succ[from] = append(outs[:i], outs[i+1:]...)
			break
		}
	}
	return true
}

// ---------------------------------------------------------------- snapshots

func (s *scope) snapshot(h int32) dw.Node {
	r := &s.recs[h]
	n := dw.Node{
		Scope:     s.name,
		ID:        r.id,
		Kind:      r.kind,
		Status:    r.status,
		Reason:    r.reason,
		Message:   r.message,
		Attempt:   r.attempt,
		Priority:  r.priority,
		Trigger:   r.trigger,
		Retry:     r.retry,
		Seq:       r.seq,
		CreatedAt: unix(r.createdAt),
		UpdatedAt: unix(r.updatedAt),
	}
	if len(r.payload) > 0 {
		n.Payload = bytes.Clone(r.payload)
	}
	if len(r.labels) > 0 {
		n.Labels = maps.Clone(r.labels)
	}
	return n
}

// specMatches reports whether an existing node was created from an identical
// definition, which is what makes AddNodes idempotent. Runtime state — status,
// attempt, sequence — is deliberately not compared: re-adding a node that has
// since run is still the same node.
func (s *scope) specMatches(h int32, spec dw.NodeSpec) bool {
	r := &s.recs[h]
	return r.kind == spec.Kind &&
		r.priority == spec.Priority &&
		r.trigger == spec.Trigger &&
		r.retry == spec.Retry &&
		bytes.Equal(r.payload, spec.Payload) &&
		maps.Equal(r.labels, spec.Labels)
}
