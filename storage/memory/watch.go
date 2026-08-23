package memory

import (
	"context"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

func unix(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// record stamps a state change with a fresh per-node sequence and a fresh
// scope-wide cursor, appends it to the scope's event log, and returns it as an
// effect for the caller to hand back to the Manager.
//
// It must be called after the node's state has been mutated, never before: an
// event that describes a state no reader can yet observe is the read-your-
// writes hazard this ordering exists to avoid.
func (s *scope) record(h int32, kind dw.EventKind, from dw.Status) dw.Effect {
	r := &s.recs[h]
	r.seq++
	s.cursor++

	ef := dw.Effect{
		NodeID:   r.id,
		Kind:     kind,
		From:     from,
		To:       r.status,
		Reason:   r.reason,
		Message:  r.message,
		Attempt:  r.attempt,
		NodeKind: r.kind,
		Seq:      r.seq,
		Cursor:   s.cursor,
		At:       unix(s.now()),
	}
	s.appendLog(ef)
	return ef
}

func (s *scope) appendLog(ef dw.Effect) {
	s.log = append(s.log, dw.Event{
		Kind:     ef.Kind,
		Scope:    s.name,
		NodeID:   ef.NodeID,
		Seq:      ef.Seq,
		Cursor:   ef.Cursor,
		From:     ef.From,
		To:       ef.To,
		Reason:   ef.Reason,
		Message:  ef.Message,
		Attempt:  ef.Attempt,
		NodeKind: ef.NodeKind,
		At:       ef.At,
	})

	// Compact only once the buffer has grown to twice its target, so the shift
	// costs O(1) amortised rather than O(capacity) on every single append.
	if len(s.log) >= 2*s.logCap {
		drop := len(s.log) - s.logCap
		copy(s.log, s.log[drop:])
		s.log = s.log[:s.logCap]
		s.logStart += dw.Cursor(drop) //nolint:gosec // drop is a positive slice length
	}

	close(s.logBell)
	s.logBell = make(chan struct{})
}

// ringDoorbell wakes every blocked claimer. Closing and replacing the channel
// is a broadcast that costs nothing when nobody is listening, and cannot be
// missed by a waiter that captured the channel before the state changed.
func (s *scope) ringDoorbell() {
	close(s.doorbell)
	s.doorbell = make(chan struct{})
}

// WaitForWork implements dagworker.Doorbell. It returns as soon as a node in
// the scope becomes claimable, or the context ends.
//
// A wakeup is advisory: returning nil means "try again", never "work is
// definitely there". A spurious wakeup costs one wasted claim attempt and a
// missed one costs a poll interval, so neither can produce a wrong answer.
func (st *Store) WaitForWork(ctx context.Context, scopeName dw.Scope, kinds []string) error {
	// Create the scope if it is new. Waiting on a scope nobody has written to
	// yet is an ordinary thing to do — a worker often starts before its
	// producer — and parking without a doorbell to wait on would mean sleeping
	// through the work when it arrives. Scopes are implicit and cost nothing.
	s, err := st.scopeFor(scopeName, true)
	if err != nil {
		return err
	}

	s.mu.RLock()
	if s.anyReady(kinds) {
		s.mu.RUnlock()
		return nil
	}
	bell := s.doorbell
	s.mu.RUnlock()

	select {
	case <-bell:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-st.closed:
		return dw.ErrClosed
	}
}

func (s *scope) anyReady(kinds []string) bool {
	if len(kinds) == 0 {
		for _, h := range s.ready {
			if h.Len() > 0 {
				return true
			}
		}
		return false
	}
	for _, k := range kinds {
		if h, ok := s.ready[k]; ok && h.Len() > 0 {
			return true
		}
	}
	return false
}

// Watch implements dagworker.DurableEventStream.
//
// The guarantee is at-least-once within a bounded retention window: every event
// appended after the given cursor is delivered in cursor order, and a
// subscriber that falls behind the window is told so with ErrCursorExpired
// rather than being handed a stream with a silent hole in it.
func (st *Store) Watch(ctx context.Context, req dw.WatchRequest) (<-chan dw.Event, error) {
	if req.Scope == "" {
		return nil, dw.ErrUnsupported
	}
	s, err := st.scopeFor(req.Scope, true)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	var next dw.Cursor
	switch {
	case req.From > 0:
		next = req.From + 1
		if next < s.logStart {
			s.mu.RUnlock()
			return nil, dw.ErrCursorExpired
		}
	case req.Replay:
		next = s.logStart
	default:
		next = s.cursor + 1
	}
	s.mu.RUnlock()

	out := make(chan dw.Event)
	go s.pump(ctx, next, out)
	return out, nil
}

func (s *scope) pump(ctx context.Context, next dw.Cursor, out chan<- dw.Event) {
	defer close(out)
	for {
		s.mu.RLock()
		if next < s.logStart {
			s.mu.RUnlock()
			return // cursor fell out of retention; the closed channel signals it
		}
		var batch []dw.Event
		// next >= logStart was checked above, so the difference is a
		// non-negative offset within the retained window.
		if idx := int(next - s.logStart); idx < len(s.log) { //nolint:gosec // checked above
			batch = append(batch, s.log[idx:]...)
			next = s.cursor + 1
		}
		bell := s.logBell
		s.mu.RUnlock()

		for _, ev := range batch {
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			case <-s.store.closed:
				return
			}
		}
		if len(batch) > 0 {
			continue
		}
		select {
		case <-bell:
		case <-ctx.Done():
			return
		case <-s.store.closed:
			return
		}
	}
}
