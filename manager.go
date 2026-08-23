package dagworker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Manager is the library's entry point: a thin, validating facade over a
// [Store] that adds the things a storage backend has no business knowing about
// — input validation, the blocking-claim wakeup protocol, subscription fan-out
// and backpressure, and the background maintenance loops.
//
// Everything that must be atomic lives in the Store. The Manager never composes
// several store calls into something it pretends is one operation, because
// across a network that pretence is where correctness goes to die.
//
// A Manager is safe for concurrent use. Several Managers, in the same process
// or in different ones, may share one Store; work is distributed by competing
// for the store's own atomic claim.
type Manager struct {
	store Store
	cfg   config
	caps  Capabilities

	mu      sync.RWMutex
	subs    map[int64]*Subscription
	nextSub int64

	bgCtx     context.Context
	bgCancel  context.CancelFunc
	wg        sync.WaitGroup
	closed    chan struct{}
	closeOnce sync.Once
}

// New returns a Manager over store.
//
// It does not take ownership of store: whoever opened it closes it. [Close]
// stops the Manager's own goroutines and leaves the backend alone, so two
// Managers may share one Store and the first to close does not disconnect the
// second.
func New(store Store, opts ...Option) (*Manager, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	cfg := defaultConfig()
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("%w: nil option", ErrInvalidConfig)
		}
		o.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	m := &Manager{
		store:  store,
		cfg:    cfg,
		subs:   make(map[int64]*Subscription),
		closed: make(chan struct{}),
	}
	if r, ok := store.(CapabilityReporter); ok {
		m.caps = r.Capabilities()
	}
	m.bgCtx, m.bgCancel = context.WithCancel(context.WithoutCancel(context.Background()))

	if !cfg.sweepDisabled {
		m.wg.Add(1)
		go m.maintain()
	}
	return m, nil
}

// Capabilities reports what the underlying backend can do. Check it rather than
// discovering a missing facet from an [ErrUnsupported] at an inconvenient
// moment.
func (m *Manager) Capabilities() Capabilities { return m.caps }

func (m *Manager) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}

func (m *Manager) check(scope Scope) error {
	if m.isClosed() {
		return ErrClosed
	}
	return scope.validate()
}

// Close stops every goroutine the Manager started and terminates every open
// subscription. It returns only once they have all exited, so a caller can rely
// on no callback firing and no channel being written after it returns — which
// is what makes a Manager safe to discard.
//
// It does not close the Store. Close is idempotent.
func (m *Manager) Close(ctx context.Context) error {
	m.closeOnce.Do(func() {
		close(m.closed)
		m.bgCancel()
	})

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.mu.Lock()
	subs := make([]*Subscription, 0, len(m.subs))
	for _, s := range m.subs {
		subs = append(subs, s)
	}
	m.subs = make(map[int64]*Subscription)
	m.mu.Unlock()

	for _, s := range subs {
		s.finish(ErrClosed)
	}
	return nil
}

// ---------------------------------------------------------------- graph

// Configure stores a scope's policy. It is visible to every Manager sharing the
// backend, so two instances cannot disagree about a scope's lease bounds or
// retry limits.
func (m *Manager) Configure(ctx context.Context, scope Scope, cfg ScopeConfig) error {
	if err := m.check(scope); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	return m.store.SetScopeConfig(ctx, scope, cfg)
}

// ScopeConfig returns a scope's stored policy.
func (m *Manager) ScopeConfig(ctx context.Context, scope Scope) (ScopeConfig, error) {
	if err := m.check(scope); err != nil {
		return ScopeConfig{}, err
	}
	return m.store.ScopeConfig(ctx, scope)
}

// AddNode creates one node. It is idempotent: adding a node whose definition is
// byte-identical to an existing one succeeds and changes nothing, so a caller
// retrying after an ambiguous failure does not create a duplicate.
func (m *Manager) AddNode(ctx context.Context, scope Scope, id NodeID, payload []byte, opts ...NodeOption) error {
	sp := NodeSpec{ID: id, Payload: payload}
	for _, o := range opts {
		if o == nil {
			return fmt.Errorf("%w: nil node option", ErrInvalidArgument)
		}
		o.applyNode(&sp)
	}
	return m.AddNodes(ctx, scope, []NodeSpec{sp})
}

// AddNodes creates nodes and their declared dependencies atomically: every spec
// in the batch lands, or none does.
//
// A dependency must already exist, or appear earlier in the same batch. Forward
// references across separate calls are not supported, because an edge to a node
// that does not exist yet cannot be checked for a cycle.
func (m *Manager) AddNodes(ctx context.Context, scope Scope, specs []NodeSpec) error {
	if err := m.check(scope); err != nil {
		return err
	}
	for i := range specs {
		if err := specs[i].Validate(); err != nil {
			return fmt.Errorf("spec %d: %w", i, err)
		}
	}
	effects, err := m.store.AddNodes(ctx, scope, specs)
	m.publish(scope, effects)
	return err
}

// AddEdge records that to depends on from. An edge that would close a cycle is
// rejected with a [*CycleError], and one into a node that has already finished
// is rejected with [ErrAlreadyTerminal].
func (m *Manager) AddEdge(ctx context.Context, scope Scope, from, to NodeID) error {
	return m.AddEdges(ctx, scope, []Edge{{From: from, To: to}})
}

// AddEdges records dependencies atomically.
func (m *Manager) AddEdges(ctx context.Context, scope Scope, edges []Edge) error {
	if err := m.check(scope); err != nil {
		return err
	}
	if err := validateEdges(edges); err != nil {
		return err
	}
	effects, err := m.store.AddEdges(ctx, scope, edges)
	m.publish(scope, effects)
	return err
}

// RemoveEdge drops a dependency. The successor may become claimable at once.
func (m *Manager) RemoveEdge(ctx context.Context, scope Scope, from, to NodeID) error {
	return m.RemoveEdges(ctx, scope, []Edge{{From: from, To: to}})
}

// RemoveEdges drops dependencies atomically.
func (m *Manager) RemoveEdges(ctx context.Context, scope Scope, edges []Edge) error {
	if err := m.check(scope); err != nil {
		return err
	}
	if err := validateEdges(edges); err != nil {
		return err
	}
	effects, err := m.store.RemoveEdges(ctx, scope, edges)
	m.publish(scope, effects)
	return err
}

// RemoveNode deletes a node.
//
// A node with successors is refused with [ErrHasSuccessors] unless policy says
// what should happen to them, because a cleanup call that silently fails a
// subgraph is too surprising to be a default. A claimed node is refused with
// [ErrNodeInFlight]; cancel it first.
func (m *Manager) RemoveNode(ctx context.Context, scope Scope, id NodeID, policy CascadePolicy) error {
	if err := m.check(scope); err != nil {
		return err
	}
	if err := id.validate(); err != nil {
		return err
	}
	if policy > CascadeFail {
		return invalidArg("cascade policy", "unknown value %d", uint8(policy))
	}
	effects, err := m.store.RemoveNode(ctx, scope, id, policy)
	m.publish(scope, effects)
	return err
}

// Cancel terminates nodes and everything downstream that can no longer run.
// Cancelling a claimed node revokes its lease, so the worker's later
// acknowledgement is refused rather than resurrecting it.
func (m *Manager) Cancel(ctx context.Context, scope Scope, ids ...NodeID) error {
	if err := m.check(scope); err != nil {
		return err
	}
	for _, id := range ids {
		if err := id.validate(); err != nil {
			return err
		}
	}
	effects, err := m.store.Cancel(ctx, scope, ids)
	m.publish(scope, effects)
	return err
}

// CancelScope terminates every unfinished node in the scope.
func (m *Manager) CancelScope(ctx context.Context, scope Scope) error {
	if err := m.check(scope); err != nil {
		return err
	}
	effects, err := m.store.CancelScope(ctx, scope)
	m.publish(scope, effects)
	return err
}

// GetNode returns a snapshot of one node.
func (m *Manager) GetNode(ctx context.Context, scope Scope, id NodeID) (Node, error) {
	if err := m.check(scope); err != nil {
		return Node{}, err
	}
	if err := id.validate(); err != nil {
		return Node{}, err
	}
	return m.store.GetNode(ctx, scope, id)
}

// Inspect returns internal scheduling state — which dependencies a node is
// still waiting on, its topological rank, its lease deadline. It answers "why
// is this stuck", which is the first question anyone asks. It carries no
// compatibility promise across minor versions.
func (m *Manager) Inspect(ctx context.Context, scope Scope, id NodeID) (Inspection, error) {
	if err := m.check(scope); err != nil {
		return Inspection{}, err
	}
	if err := id.validate(); err != nil {
		return Inspection{}, err
	}
	return m.store.Inspect(ctx, scope, id)
}

// Seal declares that no more nodes will be added to the scope. It is
// irreversible, and it is what makes completion decidable: without it, "are we
// done" can never be answered, because another node might still arrive.
func (m *Manager) Seal(ctx context.Context, scope Scope) error {
	if err := m.check(scope); err != nil {
		return err
	}
	return m.store.Seal(ctx, scope)
}

// Stats returns the scope's counters. It is O(1): every count is maintained by
// the transition that changes it, never by scanning.
func (m *Manager) Stats(ctx context.Context, scope Scope) (ScopeStats, error) {
	if err := m.check(scope); err != nil {
		return ScopeStats{}, err
	}
	return m.store.ScopeStats(ctx, scope)
}

// IsComplete reports whether the scope is sealed with no unfinished work left.
func (m *Manager) IsComplete(ctx context.Context, scope Scope) (bool, error) {
	st, err := m.Stats(ctx, scope)
	if err != nil {
		return false, err
	}
	return st.Complete, nil
}

// Scopes lists the scopes the backend knows about.
func (m *Manager) Scopes(ctx context.Context) ([]Scope, error) {
	if m.isClosed() {
		return nil, ErrClosed
	}
	return m.store.Scopes(ctx)
}

// ListNodes pages through a scope's nodes. It requires a backend reporting
// [CapList] and returns [ErrUnsupported] otherwise. Paging is keyset-based;
// there is no offset, because skipping rows to reach a page is the linear
// operation this library promises not to have.
func (m *Manager) ListNodes(ctx context.Context, scope Scope, opts ListOptions) (ListResult, error) {
	if err := m.check(scope); err != nil {
		return ListResult{}, err
	}
	l, ok := m.store.(Lister)
	if !ok || !m.caps.Has(CapList) {
		return ListResult{}, fmt.Errorf("%w: listing", ErrUnsupported)
	}
	return l.ListNodes(ctx, scope, opts)
}

func validateEdges(edges []Edge) error {
	for i, e := range edges {
		if err := e.From.validate(); err != nil {
			return fmt.Errorf("edge %d source: %w", i, err)
		}
		if err := e.To.validate(); err != nil {
			return fmt.Errorf("edge %d target: %w", i, err)
		}
		if e.From == e.To {
			return fmt.Errorf("%w: edge %d has the same source and target %q", ErrCycle, i, e.From)
		}
	}
	return nil
}

// ---------------------------------------------------------------- maintenance

// maintain runs the periodic reclaim and retention passes.
//
// Neither is required for correctness. Expired leases are also reclaimed inline
// by whoever next asks for work, and retention is off by default. This loop
// exists so that a dead worker in an otherwise idle scope is noticed promptly
// rather than at the next claim, which might be hours away.
func (m *Manager) maintain() {
	defer m.wg.Done()

	interval := m.cfg.defaults.Resolved().SweepInterval
	for {
		select {
		case <-m.closed:
			return
		case <-m.cfg.clock.After(interval):
		}

		scopes, err := m.store.Scopes(m.bgCtx)
		if err != nil {
			if !errors.Is(err, ErrClosed) {
				m.cfg.logger.WarnContext(m.bgCtx, "dagworker: listing scopes for maintenance", "error", err)
			}
			continue
		}
		for _, scope := range scopes {
			if m.isClosed() {
				return
			}
			m.sweepScope(scope)
			m.collectScope(scope)
		}
	}
}

func (m *Manager) sweepScope(scope Scope) {
	res, err := m.store.Sweep(m.bgCtx, scope, 0)
	if err != nil {
		if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrNotFound) {
			m.cfg.logger.WarnContext(m.bgCtx, "dagworker: sweeping expired leases", "scope", scope, "error", err)
		}
		return
	}
	m.publish(scope, res.Effects)
	if res.Reclaimed > 0 {
		m.cfg.logger.InfoContext(m.bgCtx, "dagworker: reclaimed expired leases",
			"scope", scope, "count", res.Reclaimed, "more", res.More)
	}
}

func (m *Manager) collectScope(scope Scope) {
	c, ok := m.store.(Collector)
	if !ok || !m.caps.Has(CapCollect) {
		return
	}
	cfg, err := m.store.ScopeConfig(m.bgCtx, scope)
	if err != nil {
		return
	}
	// Zero means disabled, not "immediately". A library that deletes a caller's
	// data because they did not configure a retention policy is a defect.
	if cfg.TerminalRetention <= 0 {
		return
	}
	cutoff := m.cfg.clock.Now().Add(-cfg.TerminalRetention)
	deleted, _, err := c.CollectTerminal(m.bgCtx, scope, cutoff, cfg.Resolved().SweepBatchSize)
	if err != nil && !errors.Is(err, ErrClosed) {
		m.cfg.logger.WarnContext(m.bgCtx, "dagworker: collecting terminal nodes", "scope", scope, "error", err)
		return
	}
	if deleted > 0 {
		m.cfg.logger.InfoContext(m.bgCtx, "dagworker: collected terminal nodes", "scope", scope, "count", deleted)
	}
}

// jitter returns a duration drawn uniformly from [d/2, d). Poll intervals are
// jittered so that a fleet of workers that started together does not stay
// synchronised, hammering the backend in lockstep bursts forever.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := int64(d / 2)
	if half <= 0 {
		return d
	}
	return time.Duration(half + rand.Int64N(half))
}
