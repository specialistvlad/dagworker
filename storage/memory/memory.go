// Package memory implements the dagworker storage port entirely in process
// memory. It is the default backend and the reference implementation: when the
// conformance suite and this package disagree, this package is what the other
// backends are read against.
//
// It is suitable whenever every worker lives in the same process as the
// Manager. It reports neither durable storage nor cross-process capability, so
// a host that needs either gets an honest answer from Capabilities rather than
// a surprise after a restart.
package memory

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// DefaultEventLogSize is how many events a scope retains for resumable
// subscriptions before the oldest are dropped and a lagging subscriber is told
// its cursor expired.
const DefaultEventLogSize = 4096

// Store is an in-memory implementation of [dagworker.Store].
type Store struct {
	mu     sync.RWMutex
	scopes map[dw.Scope]*scope

	clock    dw.Clock
	defaults dw.ScopeConfig
	logCap   int
	jitter   func(n int64) int64

	closed    chan struct{}
	closeOnce sync.Once
}

// Option configures a [Store].
type Option interface{ apply(*Store) }

type optionFunc func(*Store)

func (f optionFunc) apply(s *Store) { f(s) }

// WithClock sets the time source. The in-memory backend is the one place where
// "the storage's own clock" and "the caller's clock" are the same clock, which
// is what lets tests drive lease expiry deterministically.
//
// A nil clock is ignored rather than stored. These options cannot return an
// error, so the alternatives are to ignore the bad value or to panic on the
// first lease — and a scheduler that dies the first time it looks at the time
// is the worse of the two.
func WithClock(c dw.Clock) Option {
	return optionFunc(func(s *Store) {
		if c != nil {
			s.clock = c
		}
	})
}

// WithScopeDefaults sets the configuration applied to scopes that have none of
// their own.
func WithScopeDefaults(cfg dw.ScopeConfig) Option {
	return optionFunc(func(s *Store) { s.defaults = cfg })
}

// WithEventLogSize sets the per-scope retained event count.
func WithEventLogSize(n int) Option {
	return optionFunc(func(s *Store) {
		if n > 0 {
			s.logCap = n
		}
	})
}

// WithJitter replaces the randomness used for retry backoff. fn must return a
// value in [0, n). Tests pass a deterministic function so that a backoff
// schedule is reproducible.
func WithJitter(fn func(n int64) int64) Option {
	return optionFunc(func(s *Store) {
		if fn != nil {
			s.jitter = fn
		}
	})
}

// New returns an empty in-memory store.
func New(opts ...Option) *Store {
	s := &Store{
		scopes: make(map[dw.Scope]*scope),
		clock:  dw.SystemClock{},
		logCap: DefaultEventLogSize,
		jitter: func(n int64) int64 {
			if n <= 0 {
				return 0
			}
			// Retry backoff spread, not a secret: a cryptographic source
			// here would buy nothing and cost entropy on every failure.
			return rand.Int64N(n) //nolint:gosec // scheduling jitter, not a secret
		},
		closed: make(chan struct{}),
	}
	for _, o := range opts {
		if o != nil {
			o.apply(s)
		}
	}
	// Belt and braces: every option guards its own input, but a future one
	// might not, and a nil clock is a panic on the first claim rather than an
	// error anyone can act on.
	if s.clock == nil {
		s.clock = dw.SystemClock{}
	}
	if s.jitter == nil {
		s.jitter = func(n int64) int64 {
			if n <= 0 {
				return 0
			}
			// Retry backoff spread, not a secret: a cryptographic source
			// here would buy nothing and cost entropy on every failure.
			return rand.Int64N(n) //nolint:gosec // scheduling jitter, not a secret
		}
	}
	if s.logCap <= 0 {
		s.logCap = DefaultEventLogSize
	}
	return s
}

// Capabilities implements [dagworker.CapabilityReporter]. It reports neither
// durable storage nor cross-process sharing, because this backend has neither
// and a caller is better served by finding that out from the capability set
// than from a lost graph.
func (st *Store) Capabilities() dw.Capabilities {
	return dw.Capabilities(dw.CapList | dw.CapDurableEvents | dw.CapDoorbell | dw.CapCollect)
}

func (st *Store) isClosed() bool {
	select {
	case <-st.closed:
		return true
	default:
		return false
	}
}

// scopeFor returns the named scope, optionally creating it. A nil scope with a
// nil error means "does not exist and we were not asked to create it".
func (st *Store) scopeFor(name dw.Scope, create bool) (*scope, error) {
	if st.isClosed() {
		return nil, dw.ErrClosed
	}
	st.mu.RLock()
	s := st.scopes[name]
	st.mu.RUnlock()
	if s != nil || !create {
		return s, nil
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if s = st.scopes[name]; s != nil {
		return s, nil
	}
	s = newScope(name, st, st.defaults)
	st.scopes[name] = s
	return s, nil
}

// mustScope returns the scope or ErrNotFound, for read paths where creating a
// scope as a side effect of asking about it would be wrong.
func (st *Store) mustScope(name dw.Scope) (*scope, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, dw.ErrNotFound
	}
	return s, nil
}

// ---------------------------------------------------------------- scope admin

// ScopeConfig implements [dagworker.Store].
func (st *Store) ScopeConfig(_ context.Context, name dw.Scope) (dw.ScopeConfig, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return dw.ScopeConfig{}, err
	}
	if s == nil {
		return st.defaults, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg, nil
}

// SetScopeConfig implements [dagworker.Store].
func (st *Store) SetScopeConfig(_ context.Context, name dw.Scope, cfg dw.ScopeConfig) error {
	s, err := st.scopeFor(name, true)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg = cfg
	return nil
}

// Seal implements [dagworker.Store].
func (st *Store) Seal(_ context.Context, name dw.Scope) error {
	s, err := st.scopeFor(name, true)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed = true
	s.stats.Sealed = true
	s.stats.Complete = s.stats.NonTerminal() == 0
	return nil
}

// ScopeStats implements [dagworker.Store]. Every counter is maintained
// incrementally by the transition that changes it, so this never scans.
func (st *Store) ScopeStats(_ context.Context, name dw.Scope) (dw.ScopeStats, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return dw.ScopeStats{}, err
	}
	if s == nil {
		return dw.ScopeStats{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.stats
	out.Cursor = s.cursor
	out.Complete = s.sealed && out.NonTerminal() == 0
	return out, nil
}

// Scopes implements [dagworker.Store].
func (st *Store) Scopes(_ context.Context) ([]dw.Scope, error) {
	if st.isClosed() {
		return nil, dw.ErrClosed
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	out := make([]dw.Scope, 0, len(st.scopes))
	for name := range st.scopes {
		out = append(out, name)
	}
	return out, nil
}

// Close implements [dagworker.Store]. It is idempotent, and it unblocks every
// in-flight Watch and WaitForWork rather than leaving their goroutines parked.
func (st *Store) Close(context.Context) error {
	st.closeOnce.Do(func() { close(st.closed) })
	return nil
}

// ---------------------------------------------------------------- reads

// GetNode implements [dagworker.Store].
func (st *Store) GetNode(_ context.Context, name dw.Scope, id dw.NodeID) (dw.Node, error) {
	s, err := st.mustScope(name)
	if err != nil {
		return dw.Node{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.lookup(id)
	if !ok {
		return dw.Node{}, dw.ErrNotFound
	}
	return s.snapshot(h), nil
}

// Inspect implements [dagworker.Store].
func (st *Store) Inspect(_ context.Context, name dw.Scope, id dw.NodeID) (dw.Inspection, error) {
	s, err := st.mustScope(name)
	if err != nil {
		return dw.Inspection{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.lookup(id)
	if !ok {
		return dw.Inspection{}, dw.ErrNotFound
	}
	r := &s.recs[h]
	insp := dw.Inspection{
		Node:          s.snapshot(h),
		Phase:         r.phase,
		Deps:          r.deps,
		Rank:          s.ord[h],
		LeaseDeadline: unix(s.deadline[h]),
		ReadyAt:       unix(s.readyAt[h]),
	}
	for _, e := range s.pred[h] {
		if !e.satisfied {
			insp.Waiting = append(insp.Waiting, s.recs[e.from].id)
		}
	}
	for _, w := range s.succ[h] {
		insp.Successors = append(insp.Successors, s.recs[w].id)
	}
	return insp, nil
}

// ListNodes implements [dagworker.Lister]. Pagination is keyset over the node's
// own identifier ordering, never an offset: skipping N rows to reach page N is
// the linear operation this library promises not to have.
func (st *Store) ListNodes(_ context.Context, name dw.Scope, opts dw.ListOptions) (dw.ListResult, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return dw.ListResult{}, err
	}
	if s == nil {
		return dw.ListResult{}, nil
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]dw.NodeID, 0, len(s.index))
	for id := range s.index {
		if string(id) > opts.Cursor {
			ids = append(ids, id)
		}
	}
	sortNodeIDs(ids)

	var out dw.ListResult
	for _, id := range ids {
		h := s.index[id]
		r := &s.recs[h]
		if len(opts.Statuses) > 0 && !containsStatus(opts.Statuses, r.status) {
			continue
		}
		if len(opts.Kinds) > 0 && !containsStr(opts.Kinds, r.kind) {
			continue
		}
		if len(out.Nodes) == limit {
			out.Next = string(out.Nodes[len(out.Nodes)-1].ID)
			return out, nil
		}
		out.Nodes = append(out.Nodes, s.snapshot(h))
	}
	return out, nil
}

// CollectTerminal implements [dagworker.Collector].
func (st *Store) CollectTerminal(_ context.Context, name dw.Scope, cutoff time.Time, limit int) (int, bool, error) {
	s, err := st.scopeFor(name, false)
	if err != nil {
		return 0, false, err
	}
	if s == nil {
		return 0, false, nil
	}
	if limit <= 0 {
		limit = 100
	}
	cut := cutoff.UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	deleted := 0
	for id, h := range s.index {
		r := &s.recs[h]
		if r.phase != dw.PhaseDone || r.updatedAt > cut || len(s.succ[h]) > 0 {
			continue
		}
		if deleted == limit {
			return deleted, true, nil
		}
		for _, e := range s.pred[h] {
			s.detachSucc(e.from, h)
		}
		s.leaveBucket(h)
		s.stats.Total--
		delete(s.index, id)
		s.release(h)
		deleted++
	}
	return deleted, false, nil
}

func (s *scope) detachSucc(from, to int32) {
	outs := s.succ[from]
	for i, w := range outs {
		if w == to {
			s.succ[from] = append(outs[:i], outs[i+1:]...)
			return
		}
	}
}

func containsStatus(xs []dw.Status, x dw.Status) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func sortNodeIDs(ids []dw.NodeID) {
	// Insertion order is not meaningful for a map iteration, and the page must
	// be stable, so sort by identifier.
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

var (
	_ dw.Store              = (*Store)(nil)
	_ dw.Lister             = (*Store)(nil)
	_ dw.Doorbell           = (*Store)(nil)
	_ dw.DurableEventStream = (*Store)(nil)
	_ dw.Collector          = (*Store)(nil)
	_ dw.CapabilityReporter = (*Store)(nil)
)
