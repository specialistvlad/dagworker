package redis

import (
	"context"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// bells owns one Pub/Sub subscription per scope that anyone is waiting on, and
// fans each wakeup out to every waiter on that scope.
//
// The alternative — a subscription per blocked caller — is what this replaces.
// go-redis gives every Subscribe its own connection, tracked outside PoolSize,
// so nothing bounded them: forty workers parked on a claim held forty TCP
// connections to Redis, and a real fleet holds thousands. Nothing in the
// client complains, because the client is not what runs out; Redis's
// maxclients and the memory behind each connection are.
//
// Subscriptions are reference counted and torn down when the last waiter on a
// scope leaves, so an idle Store holds none. That matters more here than it
// would for a fixed set of scopes: scopes are implicit and a host may create
// them per build, per tenant, per day.
type bells struct {
	rdb goredis.UniversalClient
	key func(scope string) string

	mu    sync.Mutex
	state map[string]*bell
}

// bell is one scope's shared subscription plus the broadcast channel its
// waiters select on.
type bell struct {
	// ch is closed and replaced on every wakeup, exactly like memory's
	// doorbell, so a waiter that captured the channel before the wakeup
	// arrived can never miss it.
	ch chan struct{}

	sub  *goredis.PubSub
	stop context.CancelFunc
	done chan struct{}

	// waiters is the reference count. The subscription lives while it is
	// above zero and is torn down when it reaches zero.
	waiters int
}

func newBells(rdb goredis.UniversalClient, key func(string) string) *bells {
	return &bells{rdb: rdb, key: key, state: make(map[string]*bell)}
}

// join subscribes to scope's bell if nobody else is already, and returns the
// channel to select on plus the function that releases this caller's
// reference. The release function is idempotent and must always be called.
//
// It takes no context deliberately. A subscription outlives whichever caller
// happened to open it — the next caller is entitled to find it already there —
// so binding its lifetime to one caller's context would mean the first worker
// to give up tearing the bell out from under everyone still waiting on it.
func (b *bells) join(scope string) (<-chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.state[scope]
	if !ok {
		e = b.open(scope)
		b.state[scope] = e
	}
	e.waiters++
	ch := e.ch

	var once sync.Once
	return ch, func() { once.Do(func() { b.leave(scope) }) }
}

// open starts one scope's subscription and the goroutine that rings its
// channel. Called with b.mu held.
func (b *bells) open(scope string) *bell {
	// The cancel func is not deferred: it is stored as e.stop and called by
	// leave or closeAll, which is what ends the subscription.
	subCtx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is retained as e.stop; see above

	e := &bell{
		ch:   make(chan struct{}),
		sub:  b.rdb.Subscribe(subCtx, b.key(scope)),
		stop: cancel,
		done: make(chan struct{}),
	}
	go b.pump(subCtx, scope, e)
	return e
}

// pump rings scope's bell for every message that arrives.
func (b *bells) pump(ctx context.Context, scope string, e *bell) {
	defer close(e.done)
	ch := e.sub.Channel()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			b.ring(scope)
		case <-ctx.Done():
			return
		}
	}
}

// ring wakes every waiter on scope by closing the channel they hold and
// installing a fresh one.
func (b *bells) ring(scope string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.state[scope]
	if !ok {
		return
	}
	close(e.ch)
	e.ch = make(chan struct{})
}

// leave drops one reference and tears the subscription down at zero.
func (b *bells) leave(scope string) {
	b.mu.Lock()
	e, ok := b.state[scope]
	if !ok {
		b.mu.Unlock()
		return
	}
	e.waiters--
	if e.waiters > 0 {
		b.mu.Unlock()
		return
	}
	delete(b.state, scope)
	b.mu.Unlock()

	// Outside the lock: Close talks to the network, and holding b.mu across it
	// would make one slow teardown block every other scope's waiters.
	e.stop()
	_ = e.sub.Close()
	<-e.done
}

// closeAll tears down every remaining subscription. A Store that is closed
// while callers are still parked leaves them to notice through their own
// select on Store.closed; this only reclaims the connections.
func (b *bells) closeAll() {
	b.mu.Lock()
	remaining := make([]*bell, 0, len(b.state))
	for scope, e := range b.state {
		remaining = append(remaining, e)
		delete(b.state, scope)
	}
	b.mu.Unlock()

	for _, e := range remaining {
		e.stop()
		_ = e.sub.Close()
		<-e.done
	}
}
