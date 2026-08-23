package client

import (
	"context"
	"log/slog"
	"time"

	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Tuning that is not exposed per call because a work handler should not have
// to think about it: minHeartbeatInterval keeps a very short lease from
// turning into a heartbeat busy-loop, and the two default*Timeout constants
// bound the RPCs that must never inherit the poll's or the handler's own
// context (see package doc comment).
const (
	minHeartbeatInterval       = 500 * time.Millisecond
	defaultHeartbeatRPCTimeout = 5 * time.Second
	defaultAckTimeout          = 10 * time.Second
	defaultLeaseDuration       = 30 * time.Second
	defaultPollTimeout         = 30 * time.Second
)

// outcomeKind distinguishes what a [Handler] decided.
type outcomeKind uint8

const (
	outcomeComplete outcomeKind = iota
	outcomeFail
	outcomeSkip
)

// Outcome is what a [Handler] returns: exactly one of Complete, Fail, or
// Skip, mirroring dagworker.Manager's own three-way Ack/Nack/Skip split
// (claim.go) — a skip is a distinct, non-retryable branch primitive, never a
// kind of failure.
type Outcome struct {
	kind    outcomeKind
	result  []byte
	message string
}

// Complete reports success, storing result on the node.
func Complete(result []byte) Outcome { return Outcome{kind: outcomeComplete, result: result} }

// Fail reports that the attempt did not succeed. Whether that schedules a
// retry or ends the node is the scope's retry policy, never this call's
// decision.
func Fail(message string) Outcome { return Outcome{kind: outcomeFail, message: message} }

// Skip reports that there was nothing to do. It is terminal on the first
// report — the server will not retry it — so use it only when a retry would
// reach the identical conclusion.
func Skip(reason string) Outcome { return Outcome{kind: outcomeSkip, message: reason} }

// Handler does the actual work for one claimed node. ctx is canceled when
// the [Worker]'s Run context ends (a host shutting down), never on any
// per-RPC timeout — a handler that must itself prove liveness to the server
// calls nothing directly; the Worker's own heartbeat goroutine does that
// independently, on its own short-lived contexts, while the handler runs.
type Handler func(ctx context.Context, node *pb.Node) Outcome

// Worker runs the claim → heartbeat → complete/fail/skip loop for one
// execution slot. Run one Worker per slot a host wants filled concurrently —
// each Worker keeps exactly one ClaimNode call outstanding at a time, which
// is dagworker's whole capacity signal (docs/research/13-grpc-worker-protocol.md
// §4): a host wanting N-way concurrency runs N Workers, not one Worker
// claiming N nodes.
type Worker struct {
	client pb.WorkerServiceClient
	scope  string
	cfg    workerConfig
}

type workerConfig struct {
	workerID            string
	kinds               []string
	leaseDuration       time.Duration
	pollTimeout         time.Duration
	heartbeatInterval   time.Duration
	heartbeatRPCTimeout time.Duration
	ackTimeout          time.Duration
	logger              *slog.Logger
	onReportError       func(error)
}

func defaultWorkerConfig() workerConfig {
	return workerConfig{
		leaseDuration:       defaultLeaseDuration,
		pollTimeout:         defaultPollTimeout,
		heartbeatRPCTimeout: defaultHeartbeatRPCTimeout,
		ackTimeout:          defaultAckTimeout,
		logger:              slog.New(slog.DiscardHandler),
	}
}

// WorkerOption configures a [Worker].
type WorkerOption interface{ apply(*workerConfig) }

type workerOptionFunc func(*workerConfig)

func (f workerOptionFunc) apply(c *workerConfig) { f(c) }

// WithWorkerID sets the claimant identity reported for observability. It has
// no bearing on correctness, matching dagworker.AsWorker's own doc comment:
// the right to complete a node comes from the lease's fencing epoch, never
// from an asserted identity.
func WithWorkerID(id string) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) { c.workerID = id })
}

// WithKinds restricts claims to these ready-set partitions. Empty (the
// default) claims from any kind.
func WithKinds(kinds ...string) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) { c.kinds = kinds })
}

// WithLeaseDuration sets the lease length requested on every claim and
// heartbeat. The server clamps it to the scope's configured bounds.
func WithLeaseDuration(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) {
		if d > 0 {
			c.leaseDuration = d
		}
	})
}

// WithPollTimeout sets how long each ClaimNode call may block server-side
// before returning empty. The gRPC call's own deadline is derived from this
// with a margin, never equal to it, so a slow network does not race the
// server's own bound.
func WithPollTimeout(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) {
		if d > 0 {
			c.pollTimeout = d
		}
	})
}

// WithHeartbeatInterval overrides the computed heartbeat cadence (normally
// half of whatever the lease's current deadline implies) with a fixed one.
func WithHeartbeatInterval(d time.Duration) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) { c.heartbeatInterval = d })
}

// WithLogger sets where this Worker logs a failed heartbeat or a failed
// completion report. The default discards everything.
func WithLogger(l *slog.Logger) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) {
		if l != nil {
			c.logger = l
		}
	})
}

// WithOnReportError registers a callback for when the final
// CompleteNode/FailNode/SkipNode call itself fails — the one outcome a
// Handler cannot see, since it already returned by the time this call is
// made on its own fresh context.
func WithOnReportError(fn func(error)) WorkerOption {
	return workerOptionFunc(func(c *workerConfig) { c.onReportError = fn })
}

// NewWorker builds a Worker over an existing connection (see [Dial]) for the
// given scope.
func NewWorker(conn grpc.ClientConnInterface, scope string, opts ...WorkerOption) *Worker {
	cfg := defaultWorkerConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	return &Worker{client: pb.NewWorkerServiceClient(conn), scope: scope, cfg: cfg}
}

// Run polls for work and drives handle for each node claimed, until ctx ends
// or a ClaimNode call itself fails (as opposed to timing out with no work,
// which is not an error and simply polls again).
//
// ctx governs the poll loop and is inherited, via context.WithCancel, only
// by the Handler's own context — never by the heartbeat calls this method
// makes while the handler runs, and never by the final completion report
// once it returns. Those get their own short-lived contexts rooted at
// context.Background(), which is what keeps a long-running job's
// acknowledgement from silently expiring alongside whatever deadline ctx
// happens to carry (docs/research/13-grpc-worker-protocol.md §7's "WRONG"
// example, avoided structurally rather than by caller discipline).
func (w *Worker) Run(ctx context.Context, handle Handler) error {
	for {
		lease, err := w.claimOnce(ctx)
		if err != nil {
			return err
		}
		if lease == nil {
			continue // poll_timeout elapsed with nothing ready; long-poll again
		}
		w.runLease(ctx, lease, handle)
	}
}

func (w *Worker) claimOnce(ctx context.Context) (*pb.Lease, error) {
	// The RPC's own deadline is the requested poll_timeout plus a margin for
	// network latency and the server's own clamp — never exactly equal to
	// it, or an ordinary slow round trip looks identical to a hung server.
	callCtx, cancel := context.WithTimeout(ctx, w.cfg.pollTimeout+w.cfg.heartbeatRPCTimeout)
	defer cancel()

	resp, err := w.client.ClaimNode(callCtx, &pb.ClaimNodeRequest{
		Scope:         w.scope,
		WorkerId:      w.cfg.workerID,
		Kinds:         w.cfg.kinds,
		LeaseDuration: durationpb.New(w.cfg.leaseDuration),
		PollTimeout:   durationpb.New(w.cfg.pollTimeout),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetLease(), nil
}

func (w *Worker) runLease(ctx context.Context, lease *pb.Lease, handle Handler) {
	// Derived from Run's own ctx so a caller cancelling it reaches the
	// handler — but carries no deadline of its own, and is never the source
	// of the lease's actual deadline, which lives entirely in the server's
	// storage and is enforced there regardless of what this context does.
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := make(chan struct{})
	done := make(chan struct{})
	// heartbeatLoop deliberately takes no ctx: each ExtendLease call it makes
	// roots its own short timeout at context.Background(), never at workCtx
	// or ctx — see its doc comment.
	//nolint:contextcheck // see comment above
	go func() {
		defer close(done)
		w.heartbeatLoop(lease, stop)
	}()

	outcome := handle(workCtx, lease.GetNode())

	close(stop)
	<-done

	//nolint:contextcheck // report deliberately takes no ctx: the final
	// acknowledgement must survive workCtx/ctx already having ended, so it
	// roots its own timeout at context.Background() — see Run's doc comment.
	w.report(lease.GetTaskToken(), outcome)
}

// heartbeatLoop extends the lease until stop is closed. Every ExtendLease
// call is made on its own context.Background()-rooted timeout: a heartbeat
// must not inherit workCtx (a handler-triggered cancellation should not race
// a heartbeat already in flight) and must not inherit Run's ctx either (the
// two are independent RPC deadlines by design, see docs/research/13-grpc-worker-protocol.md §7).
func (w *Worker) heartbeatLoop(lease *pb.Lease, stop <-chan struct{}) {
	deadline := lease.GetLeaseExpiresAt().AsTime()
	token := lease.GetTaskToken()

	for {
		wait := w.cfg.heartbeatInterval
		if wait <= 0 {
			wait = time.Until(deadline) / 2
		}
		if wait < minHeartbeatInterval {
			wait = minHeartbeatInterval
		}

		timer := time.NewTimer(wait)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}

		//nolint:contextcheck // deliberately rooted at Background, not workCtx/ctx — see func doc
		hbCtx, cancel := context.WithTimeout(context.Background(), w.cfg.heartbeatRPCTimeout)
		resp, err := w.client.ExtendLease(hbCtx, &pb.ExtendLeaseRequest{
			TaskToken:          token,
			RequestedExtension: durationpb.New(w.cfg.leaseDuration),
		})
		cancel()
		if err != nil {
			w.cfg.logger.Warn("dagworker/grpc client: heartbeat failed", "error", err)
			// Keep trying rather than aborting the handler: a transient
			// network error should not stop heartbeating, and a genuinely
			// superseded lease (ABORTED/lease-superseded) will surface at
			// the final report call below regardless.
			continue
		}
		if ts := resp.GetLeaseExpiresAt(); ts != nil {
			deadline = ts.AsTime()
		}
	}
}

func (w *Worker) report(token []byte, outcome Outcome) {
	//nolint:contextcheck // deliberately rooted at Background — see Run's doc comment
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.ackTimeout)
	defer cancel()

	var err error
	switch outcome.kind {
	case outcomeComplete:
		_, err = w.client.CompleteNode(ctx, &pb.CompleteNodeRequest{TaskToken: token, Result: outcome.result})
	case outcomeFail:
		_, err = w.client.FailNode(ctx, &pb.FailNodeRequest{TaskToken: token, Message: outcome.message})
	case outcomeSkip:
		_, err = w.client.SkipNode(ctx, &pb.SkipNodeRequest{TaskToken: token, Reason: outcome.message})
	}
	if err != nil {
		w.cfg.logger.Error("dagworker/grpc client: reporting outcome failed", "error", err)
		if w.cfg.onReportError != nil {
			w.cfg.onReportError(err)
		}
	}
}
