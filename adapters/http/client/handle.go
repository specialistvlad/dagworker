package client

import (
	"context"
	"sync"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// Handle wraps one claimed [Lease] with an automatic renew loop, so code
// doing long-running work does not have to remember to call [Client.Renew]
// itself on a timer.
//
// The renew loop runs on a background context this Handle owns — never on
// the context the caller passed to [Client.ClaimAndRenew] — because that
// call has already returned by the time the loop's first tick fires. A
// claim call's deadline describes how long the *claim RPC* may block; it has
// no relationship to how long the resulting lease should be kept alive, and
// conflating the two is precisely the bug docs/spec/02-adapter-contract.md
// §2 requires every adapter, and by extension every client built against
// one, to not reproduce.
type Handle struct {
	client       *Client
	scope        string
	leaseTimeout time.Duration

	mu      sync.Mutex
	lease   Lease
	lastErr error

	cancel context.CancelFunc
	//nolint:containedctx // this IS the lease's cancellation signal, exposed via Context() for
	// callers to select on — a lifetime token, not the request-plumbing smell this linter
	// targets. Every method on Handle that makes a call takes its own ctx parameter (Complete,
	// Fail, Skip below); this field is never used to make one on the type's behalf.
	ctx  context.Context
	done chan struct{}
	once sync.Once
}

// newHandle starts the renew loop immediately.
func newHandle(c *Client, scope string, lease Lease, leaseTimeout, renewEvery time.Duration) *Handle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handle{
		client: c, scope: scope, leaseTimeout: leaseTimeout, lease: lease,
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	go h.renewLoop(renewEvery)
	return h
}

// ClaimAndRenew claims up to opts.MaxNodes ready nodes and wraps each granted
// lease in a [Handle] that renews itself automatically. renewEvery should be
// comfortably shorter than the effective lease timeout so a slow renew call
// or one missed tick still leaves margin before the deadline; zero picks a
// third of opts.LeaseTimeout (or a third of
// [dagworker.DefaultLeaseTimeout] when that is also zero).
func (c *Client) ClaimAndRenew(ctx context.Context, scope string, opts ClaimOptions, renewEvery time.Duration) ([]*Handle, error) {
	leases, err := c.Claim(ctx, scope, opts)
	if err != nil {
		return nil, err
	}
	if renewEvery <= 0 {
		base := opts.LeaseTimeout
		if base <= 0 {
			base = dagworker.DefaultLeaseTimeout
		}
		renewEvery = base / 3
	}
	handles := make([]*Handle, len(leases))
	for i, l := range leases {
		//nolint:contextcheck // newHandle's renew loop deliberately uses its own background
		// context (handle.go's tick), never ctx from this call — ctx bounds the Claim RPC that
		// already returned; the lease it granted must keep renewing long after that.
		handles[i] = newHandle(c, scope, l, opts.LeaseTimeout, renewEvery)
	}
	return handles, nil
}

// Context is canceled the moment this lease is known lost — a failed renew,
// most likely a [StatusError] wrapping "lease-superseded" or "lease-expired"
// — or when [Handle.Close], [Handle.Complete], [Handle.Fail], or
// [Handle.Skip] is called. Code doing the actual work should select on it
// and abandon promptly once it fires: continuing afterward means racing
// whoever holds the lease now, with no fencing protection left once this
// Handle itself has given up on renewing it.
func (h *Handle) Context() context.Context { return h.ctx }

// Lease returns the most recently known lease, including any deadline
// extension a background renew has recorded.
func (h *Handle) Lease() Lease {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lease
}

// Err returns why the renew loop stopped on its own, or nil if it is still
// running or was stopped via Close/Complete/Fail/Skip instead.
func (h *Handle) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastErr
}

func (h *Handle) renewLoop(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-h.done:
			return
		case <-t.C:
			h.tick()
			if h.Err() != nil {
				return
			}
		}
	}
}

func (h *Handle) tick() {
	// A fresh, independent timeout for this one renew call — not the
	// caller's original Claim context (there is none in scope here to reuse
	// even by accident), and not h.ctx either, since h.ctx is what this very
	// call is deciding whether to cancel.
	rctx, cancel := context.WithTimeout(context.Background(), h.client.renewTimeout)
	defer cancel()

	res, err := h.client.Renew(rctx, h.scope, h.Lease().ID, h.leaseTimeout)
	if err != nil {
		h.mu.Lock()
		h.lastErr = err
		h.mu.Unlock()
		h.cancel()
		return
	}
	h.mu.Lock()
	h.lease.Deadline = res.Deadline
	h.mu.Unlock()
}

// stopLoop ends the renew loop without acknowledging the lease. Idempotent.
func (h *Handle) stopLoop() {
	h.once.Do(func() { close(h.done) })
}

// Close abandons the lease without reporting an outcome: the renew loop
// stops and the lease is left to expire naturally, so whoever reclaims it
// after the deadline schedules another attempt exactly as if this worker had
// crashed. Use it to give up on work in progress; use Complete, Fail, or Skip
// to report a real outcome instead.
func (h *Handle) Close() {
	h.stopLoop()
	h.cancel()
}

// Complete acknowledges success and stops the renew loop. ctx bounds only
// this one HTTP call, exactly like [Client.Complete].
func (h *Handle) Complete(ctx context.Context, result []byte) (CompletionResult, error) {
	defer h.Close()
	return h.client.Complete(ctx, h.scope, h.Lease().ID, result)
}

// Fail reports failure and stops the renew loop.
func (h *Handle) Fail(ctx context.Context, message string) (CompletionResult, error) {
	defer h.Close()
	return h.client.Fail(ctx, h.scope, h.Lease().ID, message)
}

// Skip reports there was nothing to do and stops the renew loop.
func (h *Handle) Skip(ctx context.Context, reason string) (CompletionResult, error) {
	defer h.Close()
	return h.client.Skip(ctx, h.scope, h.Lease().ID, reason)
}
