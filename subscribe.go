package dagworker

import (
	"context"
	"fmt"
	"sync"
)

// Subscription is a stream of [Event]s. Close it when finished, or cancel the
// context passed to [Manager.Subscribe]; either releases it.
type Subscription struct {
	m    *Manager
	id   int64
	opts SubscribeOptions
	pol  OverflowPolicy
	ch   chan Event

	mu      sync.Mutex
	err     error
	done    bool
	gap     bool
	dropped uint64
	closeCh chan struct{}
}

// Events returns the channel. It is closed when the subscription ends; check
// [Subscription.Err] afterwards to learn whether that was a clean shutdown or a
// failure such as [ErrSubscriberLagged].
func (s *Subscription) Events() <-chan Event { return s.ch }

// Err returns why the subscription ended, or nil if it ended cleanly or is
// still running.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Dropped returns how many events this subscription has missed under
// [OverflowDropOldest]. It is a truthful counter, not an estimate: a consumer
// that finds it non-zero has definitely missed transitions and should re-read
// the state it cares about.
func (s *Subscription) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Close ends the subscription. It is idempotent.
func (s *Subscription) Close() error {
	s.m.dropSub(s.id)
	s.finish(nil)
	return nil
}

// finish closes the subscription exactly once, recording why.
//
// The close happens while holding the same mutex every send holds, which is
// what makes it safe. Closing a channel another goroutine is sending on is not
// merely racy, it panics; and since every send here is non-blocking, holding
// the lock across it costs nothing and cannot deadlock.
func (s *Subscription) finish(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked(err)
}

// closeLocked must be called with s.mu held.
func (s *Subscription) closeLocked(err error) {
	if s.done {
		return
	}
	s.done = true
	if s.err == nil {
		s.err = err
	}
	close(s.closeCh)
	close(s.ch)
}

func (s *Subscription) isDone() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done
}

// offer delivers one event without ever blocking the caller. The caller is
// whoever just completed a node, and making a scheduler wait on an observer is
// how one slow consumer stalls everyone.
//
// The whole body runs under s.mu, shared with finish, so a send can never race
// the close. That is safe precisely because every send is non-blocking: the
// lock is held for a channel operation that either succeeds at once or gives up.
func (s *Subscription) offer(ev Event) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	if s.gap {
		ev.Gap = true
	}

	select {
	case s.ch <- ev:
		s.gap = false
		s.mu.Unlock()
		return
	default:
	}

	if s.pol == OverflowCloseSlow {
		s.closeLocked(ErrSubscriberLagged)
		s.mu.Unlock()
		// Deregister outside the lock: dropSub takes the Manager's lock, and
		// taking it while holding a subscription's would invert the ordering
		// publish already relies on.
		s.m.dropSub(s.id)
		return
	}

	// Drop the oldest and take its place. Both steps are non-blocking, so a
	// consumer draining concurrently can make one of them a no-op; that costs
	// an event, which is exactly what this policy promises.
	select {
	case <-s.ch:
	default:
	}
	s.dropped++
	s.gap = true
	ev.Gap = true
	select {
	case s.ch <- ev:
		s.gap = false
	default:
	}
	s.mu.Unlock()
}

// Subscribe returns a stream of events.
//
// This is the observation feed, deliberately separate from claiming work.
// Nothing about correctness depends on an event arriving: readiness is always
// re-derivable from storage, so a missed or duplicated event costs latency, not
// a wrong answer. Do not build a set of node IDs from [EventReady] and treat it
// as authoritative.
//
// With Durable set, the stream comes from the backend's own replayable log and
// can be resumed from a [Cursor]; a backend that cannot genuinely provide
// at-least-once delivery returns [ErrUnsupported] rather than quietly giving
// you something weaker.
func (m *Manager) Subscribe(ctx context.Context, opts SubscribeOptions) (*Subscription, error) {
	if m.isClosed() {
		return nil, ErrClosed
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}

	size := opts.BufferSize
	if size <= 0 {
		size = m.cfg.subscriberBuffer
	}
	pol := m.cfg.overflow
	if opts.Overflow != nil {
		pol = *opts.Overflow
	}

	s := &Subscription{
		m:       m,
		opts:    opts,
		pol:     pol,
		ch:      make(chan Event, size),
		closeCh: make(chan struct{}),
	}

	if opts.Durable || opts.From != 0 || opts.Replay {
		if err := m.startDurable(ctx, s, opts); err != nil {
			return nil, err
		}
		return s, nil
	}

	m.mu.Lock()
	m.nextSub++
	s.id = m.nextSub
	m.addSubLocked(s)
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-ctx.Done():
			m.dropSub(s.id)
			s.finish(ctx.Err())
		case <-s.closeCh:
		case <-m.closed:
		}
	}()
	return s, nil
}

// startDurable backs a subscription with the store's replayable log instead of
// the in-process fan-out, which is what makes resuming from a cursor possible.
func (m *Manager) startDurable(ctx context.Context, s *Subscription, opts SubscribeOptions) error {
	w, ok := m.store.(DurableEventStream)
	if !ok || !m.caps.Has(CapDurableEvents) {
		return fmt.Errorf("%w: durable subscriptions", ErrUnsupported)
	}
	src, err := w.Watch(ctx, WatchRequest{Scope: opts.Scope, From: opts.From, Replay: opts.Replay})
	if err != nil {
		return err
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer s.finish(ctx.Err())
		for {
			select {
			case ev, open := <-src:
				if !open {
					return
				}
				if !opts.wants(ev) {
					continue
				}
				s.offer(ev)
				if s.isDone() {
					return
				}
			case <-s.closeCh:
				return
			case <-m.closed:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// addSubLocked files a subscription in the master registry and in whichever
// delivery index matches its scope filter. Caller holds m.mu.
func (m *Manager) addSubLocked(s *Subscription) {
	m.subs[s.id] = s
	if s.opts.Scope == "" {
		m.anyScope[s.id] = s
		return
	}
	bucket, ok := m.byScope[s.opts.Scope]
	if !ok {
		bucket = make(map[int64]*Subscription)
		m.byScope[s.opts.Scope] = bucket
	}
	bucket[s.id] = s
}

// subsFor returns the subscriptions a write to scope must be offered to: that
// scope's own, plus the ones that asked for every scope. Caller holds m.mu.
//
// It allocates because the caller iterates without the lock — offering an
// event calls into a subscriber's channel, and holding a Manager-wide lock
// across that would let one subscriber's buffer stall every writer in the
// process. The slice is the scope's own subscriber count, not the Manager's.
func (m *Manager) subsFor(scope Scope) []*Subscription {
	bucket := m.byScope[scope]
	if len(bucket)+len(m.anyScope) == 0 {
		return nil
	}
	out := make([]*Subscription, 0, len(bucket)+len(m.anyScope))
	for _, s := range bucket {
		out = append(out, s)
	}
	for _, s := range m.anyScope {
		out = append(out, s)
	}
	return out
}

func (m *Manager) dropSub(id int64) {
	m.mu.Lock()
	if s, ok := m.subs[id]; ok {
		delete(m.subs, id)
		delete(m.anyScope, id)
		if bucket, ok := m.byScope[s.opts.Scope]; ok {
			delete(bucket, id)
			// A Manager may serve a scope per build or per tenant; leaving an
			// empty bucket behind for each one is a leak with no upper bound.
			if len(bucket) == 0 {
				delete(m.byScope, s.opts.Scope)
			}
		}
	}
	m.mu.Unlock()
}

// publish fans a store operation's effects out to the in-process subscribers.
//
// It runs after the store write has committed and carries the sequence and
// cursor the store assigned, so a subscriber never sees an event describing a
// state that a read would not yet show it.
func (m *Manager) publish(scope Scope, effects []Effect) {
	if len(effects) == 0 {
		return
	}
	m.mu.RLock()
	subs := m.subsFor(scope)
	m.mu.RUnlock()
	if len(subs) == 0 {
		return
	}

	for _, ef := range effects {
		ev := Event{
			Kind:     ef.Kind,
			Scope:    scope,
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
		}
		for _, s := range subs {
			if s.opts.wants(ev) {
				s.offer(ev)
			}
		}
	}
}

// Handle is sugar over [Manager.Subscribe] for callers who would rather write a
// function than a receive loop. fn runs on a goroutine the Manager owns and
// must not block for long: it is subject to the same overflow policy as the
// channel it stands in for, so a slow handler drops events.
//
// The returned stop function ends the subscription and waits for any in-flight
// call to fn to return.
func (m *Manager) Handle(ctx context.Context, opts SubscribeOptions, fn func(Event)) (stop func(), err error) {
	if fn == nil {
		return nil, invalidArg("handler", "must not be nil")
	}
	sub, err := m.Subscribe(ctx, opts)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range sub.Events() {
			fn(ev)
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			_ = sub.Close()
			wg.Wait()
		})
	}, nil
}
