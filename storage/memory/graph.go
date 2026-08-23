package memory

import (
	"bytes"
	"context"
	"fmt"
	"maps"

	dw "github.com/specialistvlad/dagworker"
)

// journal records what a batch mutated so a later failure in the same batch can
// be undone. It exists because the storage port promises AddNodes and AddEdges
// are all-or-nothing, and the last thing a batch does — check whether an edge
// closes a cycle — can fail after earlier edges have already been linked.
//
// Only structure is journalled, never events: nothing is recorded to the event
// log until every fallible step has succeeded, so an aborted batch leaves no
// trace for a subscriber to see and no cursor to roll back.
//
// Rank values are deliberately not restored. Reordering preserves the
// topological invariant for every edge that remains, so the ranks left behind
// by an aborted batch are still a valid ordering — just a different one, which
// no caller can observe.
type journal struct {
	created []int32
	edges   [][2]int32
}

// orderedSet keeps insertion order while still deduplicating, so that the order
// in which nodes enter the ready set is the order the caller asked for rather
// than whatever a map iteration produced.
type orderedSet struct {
	items []int32
	seen  map[int32]struct{}
}

func newOrderedSet(hint int) *orderedSet {
	return &orderedSet{items: make([]int32, 0, hint), seen: make(map[int32]struct{}, hint)}
}

func (o *orderedSet) add(h int32) {
	if _, dup := o.seen[h]; dup {
		return
	}
	o.seen[h] = struct{}{}
	o.items = append(o.items, h)
}

func (s *scope) rollback(j *journal) {
	for i := len(j.edges) - 1; i >= 0; i-- {
		s.unlinkEdge(j.edges[i][0], j.edges[i][1])
	}
	for i := len(j.created) - 1; i >= 0; i-- {
		h := j.created[i]
		s.leaveBucket(h)
		s.stats.Total--
		delete(s.index, s.recs[h].id)
		s.release(h)
	}
}

// create materialises a node in the blocked phase. Its rank is the next value
// from a monotonic counter, which is correct without any search because a
// node's declared dependencies must already exist and therefore already have
// lower ranks.
func (s *scope) create(spec dw.NodeSpec, now int64) int32 {
	h := s.alloc()
	r := &s.recs[h]
	r.id = spec.ID
	r.kind = spec.Kind
	r.priority = spec.Priority
	r.trigger = spec.Trigger
	r.retry = spec.Retry
	r.status = dw.StatusNew
	r.phase = dw.PhaseBlocked
	r.alive = true
	r.createdAt = now
	r.updatedAt = now
	if len(spec.Payload) > 0 {
		r.payload = bytes.Clone(spec.Payload)
	}
	if len(spec.Labels) > 0 {
		r.labels = maps.Clone(spec.Labels)
	}

	s.index[spec.ID] = h
	s.nextOrd++
	s.ord[h] = s.nextOrd
	s.stats.Total++
	s.enterBucket(h)
	return h
}

func (s *scope) cycleError(from, to int32, path []int32) error {
	ids := make([]dw.NodeID, 0, len(path))
	for _, h := range path {
		ids = append(ids, s.recs[h].id)
	}
	return &dw.CycleError{Scope: s.name, From: s.recs[from].id, To: s.recs[to].id, Path: ids}
}

// AddNodes implements [dagworker.Store].
func (st *Store) AddNodes(_ context.Context, name dw.Scope, specs []dw.NodeSpec) ([]dw.Effect, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	s, err := st.scopeFor(name, true)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil, dw.ErrScopeSealed
	}
	cfg := s.cfg.Resolved()
	if len(specs) > cfg.MaxBatchSize {
		return nil, fmt.Errorf("%w: batch of %d exceeds the scope's limit of %d",
			dw.ErrInvalidArgument, len(specs), cfg.MaxBatchSize)
	}

	seen := make(map[dw.NodeID]struct{}, len(specs))
	for _, spec := range specs {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if len(spec.Payload) > cfg.PayloadCap {
			return nil, &dw.PayloadTooLargeError{Size: len(spec.Payload), Cap: cfg.PayloadCap}
		}
		if _, dup := seen[spec.ID]; dup {
			return nil, fmt.Errorf("%w: node %q appears twice in the batch", dw.ErrInvalidArgument, spec.ID)
		}
		seen[spec.ID] = struct{}{}
	}

	var j journal
	committed := false
	defer func() {
		if !committed {
			s.rollback(&j)
		}
	}()

	now := s.now()
	fresh := make([]int32, 0, len(specs))

	// Pass one: materialise nodes. Nothing here can fail after the validation
	// above except an identity conflict, which is checked before any write.
	for _, spec := range specs {
		if h, exists := s.lookup(spec.ID); exists {
			if !s.specMatches(h, spec) {
				return nil, fmt.Errorf("%w: node %q", dw.ErrIDConflict, spec.ID)
			}
			continue
		}
		h := s.create(spec, now)
		j.created = append(j.created, h)
		fresh = append(fresh, h)
	}

	// Pass two: link declared dependencies. This is where a cycle can be
	// discovered, which is why the journal exists.
	//
	// The touched set is an ordered slice, not a map. Settling in map order
	// would assign ready-set insertion numbers in a random order, so two nodes
	// added in one batch would be served in an arbitrary order rather than the
	// order the caller wrote them — which is exactly the fairness property
	// equal-priority FIFO is supposed to provide.
	touched := newOrderedSet(len(specs))
	for _, spec := range specs {
		to := s.index[spec.ID]
		touched.add(to)
		for _, dep := range spec.Deps {
			from, exists := s.lookup(dep)
			if !exists {
				return nil, fmt.Errorf("%w: %q depends on %q, which does not exist",
					dw.ErrNotFound, spec.ID, dep)
			}
			if s.hasEdge(from, to) {
				continue
			}
			if s.recs[to].phase == dw.PhaseDone {
				return nil, fmt.Errorf("%w: %q", dw.ErrAlreadyTerminal, spec.ID)
			}
			if res := s.addEdgeOrder(from, to); res.cyclePath != nil {
				return nil, s.cycleError(from, to, res.cyclePath)
			}
			s.linkEdge(from, to)
			j.edges = append(j.edges, [2]int32{from, to})
		}
	}

	committed = true

	// Pass three: announce. Creation is reported before readiness so a
	// subscriber never sees a node become claimable before it has heard the
	// node exists.
	var effects []dw.Effect
	for _, h := range fresh {
		effects = append(effects, s.record(h, dw.EventCreated, dw.StatusNew))
	}
	for _, h := range touched.items {
		effects = s.settle(h, effects)
	}
	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return effects, nil
}

// AddEdges implements [dagworker.Store].
func (st *Store) AddEdges(_ context.Context, name dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	s, err := st.mustScope(name)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var j journal
	committed := false
	defer func() {
		if !committed {
			s.rollback(&j)
		}
	}()

	touched := newOrderedSet(len(edges))
	for _, e := range edges {
		from, ok := s.lookup(e.From)
		if !ok {
			return nil, fmt.Errorf("%w: edge source %q", dw.ErrNotFound, e.From)
		}
		to, ok := s.lookup(e.To)
		if !ok {
			return nil, fmt.Errorf("%w: edge target %q", dw.ErrNotFound, e.To)
		}
		if from == to {
			return nil, fmt.Errorf("%w: %q depends on itself", dw.ErrCycle, e.From)
		}
		if s.hasEdge(from, to) {
			continue
		}
		// Nothing leaves a terminal status, so a terminal node cannot acquire a
		// dependency that would have to re-block it.
		if s.recs[to].phase == dw.PhaseDone {
			return nil, fmt.Errorf("%w: %q", dw.ErrAlreadyTerminal, e.To)
		}
		if res := s.addEdgeOrder(from, to); res.cyclePath != nil {
			return nil, s.cycleError(from, to, res.cyclePath)
		}
		s.linkEdge(from, to)
		j.edges = append(j.edges, [2]int32{from, to})
		touched.add(to)
	}

	committed = true

	var effects []dw.Effect
	for _, h := range touched.items {
		effects = s.settle(h, effects)
	}
	return effects, nil
}

// RemoveEdges implements [dagworker.Store].
func (st *Store) RemoveEdges(_ context.Context, name dw.Scope, edges []dw.Edge) ([]dw.Effect, error) {
	if len(edges) == 0 {
		return nil, nil
	}
	s, err := st.mustScope(name)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	touched := newOrderedSet(len(edges))
	for _, e := range edges {
		from, ok := s.lookup(e.From)
		if !ok {
			return nil, fmt.Errorf("%w: edge source %q", dw.ErrNotFound, e.From)
		}
		to, ok := s.lookup(e.To)
		if !ok {
			return nil, fmt.Errorf("%w: edge target %q", dw.ErrNotFound, e.To)
		}
		if s.unlinkEdge(from, to) {
			touched.add(to)
		}
	}

	var effects []dw.Effect
	for _, h := range touched.items {
		effects = s.settle(h, effects)
	}
	return effects, nil
}

// RemoveNode implements [dagworker.Store].
func (st *Store) RemoveNode(_ context.Context, name dw.Scope, id dw.NodeID, policy dw.CascadePolicy) ([]dw.Effect, error) {
	s, err := st.mustScope(name)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.lookup(id)
	if !ok {
		return nil, dw.ErrNotFound
	}
	if s.recs[h].phase == dw.PhaseClaimed {
		return nil, fmt.Errorf("%w: %q", dw.ErrNodeInFlight, id)
	}

	var effects []dw.Effect
	if len(s.succ[h]) > 0 {
		switch policy {
		case dw.CascadeReject:
			return nil, fmt.Errorf("%w: %q has %d successors", dw.ErrHasSuccessors, id, len(s.succ[h]))
		case dw.CascadeFail:
			// Fail the successors first, while the edges still exist to walk.
			for _, w := range append([]int32(nil), s.succ[h]...) {
				effects = s.terminate(w, dw.StatusError, dw.ReasonRemoved, terminalMessage(dw.ReasonRemoved), effects)
			}
		case dw.CascadeDetach:
			// Nothing to do here: dropping the edges below is the whole policy,
			// and each successor is settled afterwards.
		}
	}

	successors := append([]int32(nil), s.succ[h]...)
	for _, w := range successors {
		s.unlinkEdge(h, w)
	}
	for _, e := range append([]predEdge(nil), s.pred[h]...) {
		s.unlinkEdge(e.from, h)
	}

	s.readyHeap(s.recs[h].kind).Remove(h)
	s.scheduled.Remove(h)
	s.leases.Remove(h)
	s.leaveBucket(h)
	s.stats.Total--
	delete(s.index, id)
	s.release(h)

	if policy == dw.CascadeDetach {
		for _, w := range successors {
			effects = s.settle(w, effects)
		}
	}
	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return effects, nil
}

// Cancel implements [dagworker.Store].
func (st *Store) Cancel(_ context.Context, name dw.Scope, ids []dw.NodeID) ([]dw.Effect, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	s, err := st.mustScope(name)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var effects []dw.Effect
	for _, id := range ids {
		h, ok := s.lookup(id)
		if !ok {
			return effects, fmt.Errorf("%w: %q", dw.ErrNotFound, id)
		}
		if s.recs[h].phase == dw.PhaseDone {
			continue // cancelling something already finished is a no-op, not an error
		}
		effects = s.terminate(h, dw.StatusError, dw.ReasonCancelled, terminalMessage(dw.ReasonCancelled), effects)
	}
	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return effects, nil
}

// CancelScope implements [dagworker.Store].
func (st *Store) CancelScope(_ context.Context, name dw.Scope) ([]dw.Effect, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var effects []dw.Effect
	// Snapshot the handles first: terminate mutates the phase buckets as it
	// cascades, and a live iteration over the index would see its own writes.
	live := make([]int32, 0, len(s.index))
	for _, h := range s.index {
		if s.recs[h].phase != dw.PhaseDone {
			live = append(live, h)
		}
	}
	for _, h := range live {
		if s.recs[h].phase == dw.PhaseDone {
			continue
		}
		effects = s.terminate(h, dw.StatusError, dw.ReasonCancelled, terminalMessage(dw.ReasonCancelled), effects)
	}
	s.stats.Complete = s.sealed && s.stats.NonTerminal() == 0
	return effects, nil
}
