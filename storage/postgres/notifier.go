package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// notifier owns the one long-lived LISTEN connection a Store shares across
// every Watch and WaitForWork call, and fans a wakeup out to whichever
// scopes' waiters are currently parked.
//
// It is purely a latency optimization (dossier 04 §3, point 4): every waiter
// also re-polls storage on its own bounded timer, so a notifier that never
// connects, or that drops its connection under load, costs latency and
// nothing else. Nothing in this package's correctness depends on a
// notification ever arriving.
type notifier struct {
	pool    *pgxpool.Pool
	channel string

	mu   sync.Mutex
	bell map[string]chan struct{} // scope -> broadcast channel, closed-and-replaced on wake

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newNotifier(pool *pgxpool.Pool, channel string) *notifier {
	return &notifier{
		pool:    pool,
		channel: channel,
		bell:    make(map[string]chan struct{}),
		stopCh:  make(chan struct{}),
	}
}

// start launches the background LISTEN loop. Safe to call at most once; New
// itself does not call it; only Open does, since a New-constructed store
// might be handed a pool the host does not want an extra background
// connection borrowed from indefinitely without saying so explicitly — Open
// is the entry point that already commits to owning the pool's lifecycle.
func (n *notifier) start() {
	n.wg.Add(1)
	go n.run()
}

func (n *notifier) stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
	n.wg.Wait()
}

// waitChan returns the current broadcast channel for scope, creating one on
// first use. Callers select on the returned channel; ring closes it and
// installs a fresh one, exactly like memory's doorbell, so a waiter that
// captured the channel before the wakeup can never miss it.
func (n *notifier) waitChan(scope string) chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch, ok := n.bell[scope]
	if !ok {
		ch = make(chan struct{})
		n.bell[scope] = ch
	}
	return ch
}

func (n *notifier) ring(scope string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.bell[scope]; ok {
		close(ch)
		delete(n.bell, scope)
	}
}

// run holds one dedicated connection LISTENing on n.channel for as long as
// the notifier is alive, reconnecting with a short backoff on any error —
// including at startup, since the very first connection attempt can race a
// database that is still coming up. A stuck or wedged listener connection is
// exactly the poisoning failure mode dossier 04 §3.3 warns about, which is
// the other reason to treat any error here as fatal-to-this-connection and
// reconnect rather than retry in place.
func (n *notifier) run() {
	defer n.wg.Done()
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		if err := n.listenOnce(); err != nil {
			select {
			case <-time.After(backoff):
			case <-n.stopCh:
				return
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

// listenOnce acquires a connection, LISTENs, and pumps notifications until
// the connection errors, the notifier is stopped, or the pool's context
// otherwise ends. It returns nil only when n.stopCh caused the exit.
func (n *notifier) listenOnce() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := n.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}

	go func() {
		select {
		case <-n.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		note, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			select {
			case <-n.stopCh:
				return nil
			default:
				return err
			}
		}
		n.ring(note.Payload)
	}
}
