package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// ErrSkip tells the pool to report a node as skipped rather than failed: the
// worker looked and there was nothing to do. Successors distinguish the two
// through their trigger rule, which is what makes branching work.
var ErrSkip = errors.New("nothing to do")

// Handler is the only thing a host program actually has to write. Everything
// else — claiming, leases, heartbeats, retries, acknowledgement — is the
// library's job.
//
// Returning nil acknowledges success with the returned result. Returning an
// error reports a failed attempt, which the scope's retry policy may turn into
// another one. Returning ErrSkip reports that there was nothing to do.
type Handler func(ctx context.Context, node dw.Node) (result []byte, err error)

// Pool is a set of workers draining one scope, of the shape a host program
// would actually deploy.
//
// It is in this package rather than the library because it is a *policy*
// choice: how many workers, whether to heartbeat, when to stop. Different hosts
// want different answers, and a library that decides for them is a framework.
type Pool struct {
	Manager *dw.Manager
	Scope   dw.Scope

	// Kinds restricts this pool to particular node kinds, which is how one
	// graph feeds several differently-provisioned worker fleets.
	Kinds []string

	// Workers is the concurrency. Zero means one.
	Workers int

	Handle Handler

	// Heartbeat, when set, extends the lease on this interval while a handler
	// is running. Work that can outlast its lease needs it; work that cannot
	// should not pay for it.
	Heartbeat time.Duration

	// LeaseTimeout overrides the scope's default for this pool's claims.
	LeaseTimeout time.Duration

	// StopWhenIdle ends the pool as soon as a claim finds nothing, instead of
	// waiting for more work. Useful for a batch that is known to be finite.
	StopWhenIdle bool

	stats PoolStats
}

// PoolStats counts what a pool did. Every counter is atomic, so a caller may
// read them while the pool is still running.
type PoolStats struct {
	Claimed   atomic.Int64
	Succeeded atomic.Int64
	Failed    atomic.Int64
	Skipped   atomic.Int64
	Retried   atomic.Int64
	Panicked  atomic.Int64
}

// Stats returns the pool's counters.
func (p *Pool) Stats() *PoolStats { return &p.stats }

// Run drains the scope until it is complete, until a claim finds nothing (with
// StopWhenIdle), or until ctx ends. It returns only once every worker has
// stopped, so a caller can read the counters safely afterwards.
func (p *Pool) Run(ctx context.Context) error {
	workers := max(p.Workers, 1)

	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.work(ctx, fmt.Sprintf("worker-%d", i))
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (p *Pool) work(ctx context.Context, name string) error {
	opts := []dw.ClaimOption{dw.AsWorker(name)}
	if len(p.Kinds) > 0 {
		opts = append(opts, dw.OfKind(p.Kinds...))
	}
	if p.LeaseTimeout > 0 {
		opts = append(opts, dw.WithLeaseTimeout(p.LeaseTimeout))
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		lease, err := p.Manager.TryClaim(ctx, p.Scope, opts...)
		switch {
		case errors.Is(err, dw.ErrNoWork):
			if p.StopWhenIdle {
				return nil
			}
			// Nothing ready right now. If the scope is finished there will
			// never be anything again, so check before waiting.
			done, cerr := p.Manager.IsComplete(ctx, p.Scope)
			if cerr != nil || done {
				return nil //nolint:nilerr // a closed or finished scope ends the pool, it is not a failure
			}
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
			continue
		case errors.Is(err, dw.ErrClosed), errors.Is(err, context.Canceled):
			return nil
		case err != nil:
			return fmt.Errorf("%s: claim: %w", name, err)
		}

		p.stats.Claimed.Add(1)
		if err := p.run(ctx, lease); err != nil {
			return err
		}
	}
}

// run executes one node and reports the outcome, keeping the lease alive
// underneath if the pool asked for heartbeats.
func (p *Pool) run(ctx context.Context, lease dw.Lease) error {
	stop := p.heartbeat(ctx, lease)
	result, handlerErr := p.safely(ctx, lease.Node)
	stop()

	switch {
	case errors.Is(handlerErr, ErrSkip):
		p.stats.Skipped.Add(1)
		return p.report(p.Manager.Skip(ctx, lease, handlerErr.Error()))

	case handlerErr != nil:
		outcome, err := p.Manager.Nack(ctx, lease, handlerErr)
		if outcome.Retrying {
			p.stats.Retried.Add(1)
		} else {
			p.stats.Failed.Add(1)
		}
		return p.report(err)

	default:
		p.stats.Succeeded.Add(1)
		return p.report(p.Manager.Ack(ctx, lease, result))
	}
}

// report swallows the two outcomes that are ordinary rather than exceptional: a
// lease that was superseded while the handler ran (someone else owns the node
// now, and saying so twice helps nobody) and a manager that is shutting down.
func (p *Pool) report(err error) error {
	switch {
	case err == nil,
		errors.Is(err, dw.ErrLeaseMismatch),
		errors.Is(err, dw.ErrClosed),
		errors.Is(err, context.Canceled):
		return nil
	default:
		return err
	}
}

// safely runs the handler and converts a panic into a failed attempt. A worker
// that panics on one node should lose that node, not take down the fleet.
func (p *Pool) safely(ctx context.Context, node dw.Node) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			p.stats.Panicked.Add(1)
			err = fmt.Errorf("handler panicked: %v", r) //nolint:err113 // the panic value is the message
		}
	}()
	return p.Handle(ctx, node)
}

// heartbeat keeps a lease alive while a handler runs, and returns a function
// that stops it. It exists because the alternative — asking for a lease long
// enough to cover the worst case — means a crashed worker's node is stuck for
// that worst case too.
func (p *Pool) heartbeat(ctx context.Context, lease dw.Lease) (stop func()) {
	if p.Heartbeat <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(p.Heartbeat)
		defer ticker.Stop()
		current := lease
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				extended, err := p.Manager.Extend(ctx, current, p.LeaseTimeout)
				if err != nil {
					// The lease is gone. Stop renewing; the handler's own
					// acknowledgement will be refused by the fencing check,
					// which is the correct outcome.
					return
				}
				current = extended
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}
