package memory

import (
	"fmt"
	"sort"

	dw "github.com/specialistvlad/dagworker"
)

// Snapshot is the whole of a Store's state in a form that can be written to
// durable storage and loaded back.
//
// It exists for `storage/file`, which keeps this backend in memory and an
// fsynced command log on disk, and needs to compact that log: a log that is
// never truncated makes startup grow without bound (ADR-0047). Filtering the
// log instead is tempting and wrong -- completing a node releases its
// successors, so dropping a removed node's records can leave a survivor
// un-readied -- so compaction needs the state itself.
//
// # What it is not
//
// It is not a wire format and it carries no version tag, because it is never
// exchanged between builds: `storage/file` writes a snapshot and reads it back
// within one binary, and a snapshot it cannot decode is discarded in favour of
// replaying the log. Do not persist one across an upgrade and expect it to
// load.
//
// Only the primary state is captured. The ready, lease and retry indexes are
// derived from a node's phase and are rebuilt by [Restore], as is the id
// lookup, so they cannot drift from the data they index.
type Snapshot struct {
	Scopes []ScopeSnapshot
}

// ScopeSnapshot is one scope's state.
type ScopeSnapshot struct {
	Name       string
	Config     dw.ScopeConfig
	Sealed     bool
	EpochFloor uint64
	NextOrd    int64
	NextFifo   uint64
	Cursor     dw.Cursor
	Stats      dw.ScopeStats
	Nodes      []NodeSnapshot
}

// NodeSnapshot is one node, with its edges named by node id rather than by
// internal handle so that [Restore] is free to allocate handles differently.
type NodeSnapshot struct {
	ID, Kind, Message, Worker string
	Payload, Result           []byte
	Labels                    map[string]string
	Retry                     dw.RetryPolicy
	Deps                      dw.DepCounts
	Seq                       dw.Seq
	Epoch                     uint64
	Attempt                   uint32
	Priority                  int16
	Status                    dw.Status
	Reason                    dw.Reason
	Phase                     dw.Phase
	Trigger                   dw.TriggerRule
	CreatedAt, UpdatedAt      int64
	Ord, Deadline, ReadyAt    int64
	Fifo                      uint64
	Succ                      []string
	Pred                      []PredSnapshot
}

// PredSnapshot is one incoming edge and whether it has been satisfied.
type PredSnapshot struct {
	From      string
	Satisfied bool
}

// Snapshot captures the store's state. Scopes are emitted in name order so two
// snapshots of the same state are byte-identical, which is what lets a caller
// tell whether anything actually changed.
func (st *Store) Snapshot() Snapshot {
	st.mu.RLock()
	names := make([]string, 0, len(st.scopes))
	byName := make(map[string]*scope, len(st.scopes))
	for name, s := range st.scopes {
		names = append(names, string(name))
		byName[string(name)] = s
	}
	st.mu.RUnlock()
	sort.Strings(names)

	out := Snapshot{Scopes: make([]ScopeSnapshot, 0, len(names))}
	for _, name := range names {
		out.Scopes = append(out.Scopes, byName[name].snapshotScope())
	}
	return out
}

func (s *scope) snapshotScope() ScopeSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := ScopeSnapshot{
		Name:       string(s.name),
		Config:     s.cfg,
		Sealed:     s.sealed,
		EpochFloor: s.epochFloor,
		NextOrd:    s.nextOrd,
		NextFifo:   s.nextFifo,
		Cursor:     s.cursor,
		Stats:      s.stats,
		Nodes:      make([]NodeSnapshot, 0, len(s.index)),
	}
	for h := range s.recs {
		r := &s.recs[h]
		if !r.alive {
			continue // a free-list slot: nothing to restore
		}
		n := NodeSnapshot{
			ID: string(r.id), Kind: r.kind, Message: r.message, Worker: r.worker,
			Payload: r.payload, Result: r.result, Labels: r.labels,
			Retry: r.retry, Deps: r.deps, Seq: r.seq,
			Epoch: r.epoch, Attempt: r.attempt, Priority: r.priority,
			Status: r.status, Reason: r.reason, Phase: r.phase, Trigger: r.trigger,
			CreatedAt: r.createdAt, UpdatedAt: r.updatedAt,
			Ord: s.ord[h], Deadline: s.deadline[h], ReadyAt: s.readyAt[h], Fifo: s.fifo[h],
		}
		for _, w := range s.succ[h] {
			n.Succ = append(n.Succ, string(s.recs[w].id))
		}
		for _, e := range s.pred[h] {
			n.Pred = append(n.Pred, PredSnapshot{From: string(s.recs[e.from].id), Satisfied: e.satisfied})
		}
		out.Nodes = append(out.Nodes, n)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })
	return out
}

// Restore builds a Store holding exactly the state in snap.
//
// The derived structures -- the ready, lease and retry heaps, and the id index
// -- are rebuilt here from each node's phase rather than captured, so they
// cannot disagree with the nodes they index. The event ring is deliberately not
// restored: a subscriber resuming across a restart is resuming from history
// this process never had, and telling it so with ErrCursorExpired is the same
// answer the bounded ring already gives a subscriber that fell too far behind.
// The cursor itself is restored, so new events continue where the old ones
// stopped rather than colliding with them.
func Restore(snap Snapshot, opts ...Option) (*Store, error) {
	st := New(opts...)
	for _, ss := range snap.Scopes {
		if ss.Name == "" {
			return nil, fmt.Errorf("%w: snapshot contains a scope with no name", dw.ErrInvalidArgument)
		}
		s, err := restoreScope(st, ss)
		if err != nil {
			return nil, err
		}
		st.scopes[dw.Scope(ss.Name)] = s
	}
	return st, nil
}

func restoreScope(st *Store, ss ScopeSnapshot) (*scope, error) {
	s := newScope(dw.Scope(ss.Name), st, ss.Config)
	s.sealed = ss.Sealed
	s.epochFloor = ss.EpochFloor
	s.nextOrd = ss.NextOrd
	s.nextFifo = ss.NextFifo
	s.cursor = ss.Cursor
	s.stats = ss.Stats
	// An empty ring whose window starts after the last event: a resume from
	// any earlier cursor correctly reports ErrCursorExpired rather than
	// silently returning a stream with a hole in it.
	s.logStart = ss.Cursor + 1

	n := len(ss.Nodes)
	s.recs = make([]nodeRec, 0, n)
	s.succ = make([][]int32, n)
	s.pred = make([][]predEdge, n)
	s.ord = make([]int64, n)
	s.deadline = make([]int64, n)
	s.readyAt = make([]int64, n)
	s.fifo = make([]uint64, n)

	for i, ns := range ss.Nodes {
		h := int32(i) //nolint:gosec // i is bounded by len(ss.Nodes), which came from an int32-indexed slice
		if _, dup := s.index[dw.NodeID(ns.ID)]; dup {
			return nil, fmt.Errorf("%w: snapshot repeats node %q in scope %q",
				dw.ErrInvalidArgument, ns.ID, ss.Name)
		}
		s.index[dw.NodeID(ns.ID)] = h
		s.recs = append(s.recs, nodeRec{
			id: dw.NodeID(ns.ID), kind: ns.Kind, message: ns.Message, worker: ns.Worker,
			payload: ns.Payload, result: ns.Result, labels: ns.Labels,
			retry: ns.Retry, deps: ns.Deps, seq: ns.Seq,
			epoch: ns.Epoch, attempt: ns.Attempt, priority: ns.Priority,
			status: ns.Status, reason: ns.Reason, phase: ns.Phase, trigger: ns.Trigger,
			alive: true, createdAt: ns.CreatedAt, updatedAt: ns.UpdatedAt,
		})
		s.ord[h], s.deadline[h], s.readyAt[h], s.fifo[h] = ns.Ord, ns.Deadline, ns.ReadyAt, ns.Fifo
	}

	// Edges second: every node now has a handle, so an edge can be resolved
	// whichever order the endpoints appear in.
	for i, ns := range ss.Nodes {
		h := int32(i) //nolint:gosec // bounded as above
		for _, id := range ns.Succ {
			w, ok := s.index[dw.NodeID(id)]
			if !ok {
				return nil, fmt.Errorf("%w: node %q has an edge to %q, which the snapshot does not contain",
					dw.ErrInvalidArgument, ns.ID, id)
			}
			s.succ[h] = append(s.succ[h], w)
		}
		for _, p := range ns.Pred {
			f, ok := s.index[dw.NodeID(p.From)]
			if !ok {
				return nil, fmt.Errorf("%w: node %q has an edge from %q, which the snapshot does not contain",
					dw.ErrInvalidArgument, ns.ID, p.From)
			}
			s.pred[h] = append(s.pred[h], predEdge{from: f, satisfied: p.Satisfied})
		}
	}

	// The indexes, from each node's phase. Not enterBucket: the statistics were
	// restored wholesale above, and counting them again would double them.
	for i := range ss.Nodes {
		h := int32(i) //nolint:gosec // bounded as above
		switch s.recs[h].phase {
		case dw.PhaseReady:
			s.readyHeap(s.recs[h].kind).Push(h)
		case dw.PhaseClaimed:
			s.leases.Push(h)
		case dw.PhaseScheduled:
			s.scheduled.Push(h)
		case dw.PhaseBlocked, dw.PhaseDone:
			// Neither is indexed: a blocked node waits on an edge and a
			// terminal one waits for nothing.
		}
	}
	return s, nil
}
