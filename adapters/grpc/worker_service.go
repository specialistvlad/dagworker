package grpcadapter

import (
	"context"
	"errors"
	"time"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
)

// workerServer implements pb.WorkerServiceServer over a Manager. It is kept
// unexported and separate from controlServer (docs/spec/02-adapter-contract.md
// and docs/research/13-grpc-worker-protocol.md §11 both call for the two
// services to be independently authorizable — a worker's credential should
// never carry a claim that reaches ControlService RPCs, even by accident of
// implementation, so the two are not merely different interfaces on one type).
type workerServer struct {
	pb.UnimplementedWorkerServiceServer

	mgr      *dw.Manager
	cfg      serverConfig
	shutdown <-chan struct{}
}

// pollTimeout resolves a requested duration against the server's configured
// bounds. Zero (indistinguishable on the wire from "field omitted", see
// serverConfig's doc comment) becomes the server default; anything above the
// ceiling is clamped down to it — the adapter contract's "long-polling is
// bounded server-side" rule, enforced in exactly one place.
func (w *workerServer) pollTimeout(requested time.Duration) time.Duration {
	switch {
	case requested <= 0:
		return w.cfg.defaultPollTimeout
	case requested > w.cfg.maxPollTimeout:
		return w.cfg.maxPollTimeout
	default:
		return requested
	}
}

// ClaimNode implements the long-poll dispatch RPC. It is the one handler in
// this package that must never let the RPC's own deadline leak into the
// lease it grants: pollCtx bounds only how long this call may block, and the
// Lease it returns carries its own, independent deadline computed by the
// Manager's storage backend (see this module's package doc and
// docs/research/13-grpc-worker-protocol.md §7 for why conflating the two is
// the single most common mistake in a poll-then-work client).
func (w *workerServer) ClaimNode(ctx context.Context, req *pb.ClaimNodeRequest) (*pb.ClaimNodeResponse, error) {
	scope := dw.Scope(req.GetScope())
	opts := claimOptionsFromProto(req)

	pollCtx, cancel := context.WithTimeout(ctx, w.pollTimeout(durationFromProto(req.GetPollTimeout())))
	defer cancel()

	type outcome struct {
		lease dw.Lease
		err   error
	}
	// Buffered so the goroutine can never block on a send nobody will read:
	// the shutdown branch below returns without waiting for it, and letting
	// it leak a blocked goroutine would defeat the whole point of draining
	// promptly.
	resCh := make(chan outcome, 1)
	go func() {
		lease, err := w.mgr.Claim(pollCtx, scope, opts...)
		resCh <- outcome{lease, err}
	}()

	select {
	case res := <-resCh:
		return w.claimResponse(ctx, res.lease, res.err)
	case <-w.shutdown:
		// Cancelling here is what makes the goroutine above return promptly
		// instead of running out its full poll window — this is the concrete
		// mechanism behind "draining is prompt" (docs/spec/02-adapter-contract.md).
		cancel()
		return &pb.ClaimNodeResponse{}, nil
	}
}

// claimResponse turns a Claim outcome into the wire response. A timed-out
// poll is an empty, successful response — having no work is ordinary, never
// an error — while a genuine upstream cancellation (the caller hung up or
// its own deadline, distinct from pollCtx's derived one) is reported as such.
func (w *workerServer) claimResponse(ctx context.Context, lease dw.Lease, err error) (*pb.ClaimNodeResponse, error) {
	if err == nil {
		pbLease, convErr := leaseToProto(lease)
		if convErr != nil {
			return nil, mapError(convErr)
		}
		return &pb.ClaimNodeResponse{Lease: pbLease}, nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &pb.ClaimNodeResponse{}, nil
	}
	if errors.Is(err, context.Canceled) {
		if ctx.Err() != nil {
			return nil, mapError(ctx.Err())
		}
		return &pb.ClaimNodeResponse{}, nil
	}
	return nil, mapError(err)
}

func claimOptionsFromProto(req *pb.ClaimNodeRequest) []dw.ClaimOption {
	opts := make([]dw.ClaimOption, 0, 3)
	if len(req.GetKinds()) > 0 {
		opts = append(opts, dw.OfKind(req.GetKinds()...))
	}
	if req.GetWorkerId() != "" {
		opts = append(opts, dw.AsWorker(req.GetWorkerId()))
	}
	if d := durationFromProto(req.GetLeaseDuration()); d > 0 {
		opts = append(opts, dw.WithLeaseTimeout(d))
	}
	return opts
}

// ExtendLease is the heartbeat: it must be called again before the response's
// lease_expires_at or the node times out server-side, independent of this
// RPC's own deadline. Callers must give it a fresh context of its own — never
// the ClaimNode call's — which is a client-side discipline the reference SDK
// in ./client enforces structurally rather than by convention alone.
func (w *workerServer) ExtendLease(ctx context.Context, req *pb.ExtendLeaseRequest) (*pb.ExtendLeaseResponse, error) {
	lease, err := decodeTaskToken(req.GetTaskToken())
	if err != nil {
		return nil, err
	}
	extended, err := w.mgr.Extend(ctx, lease, durationFromProto(req.GetRequestedExtension()))
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.ExtendLeaseResponse{LeaseExpiresAt: timeToProto(extended.Deadline)}, nil
}

// CompleteNode reports success, fenced on the lease task_token encodes: a
// stale token — one whose epoch the node has since moved past — is refused
// with ABORTED/lease-superseded, never silently accepted.
func (w *workerServer) CompleteNode(ctx context.Context, req *pb.CompleteNodeRequest) (*pb.CompleteNodeResponse, error) {
	lease, err := decodeTaskToken(req.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if err := w.mgr.Ack(ctx, lease, req.GetResult()); err != nil {
		return nil, mapError(err)
	}
	return &pb.CompleteNodeResponse{}, nil
}

// FailNode reports that an attempt did not succeed.
//
// will_retry and next_attempt_at are best-effort: dagworker.Manager.Nack
// commits the retry-or-terminate decision atomically but does not return it
// to its caller (see claim.go's Manager.complete, which discards the
// store's CompleteResult), so this handler makes one further, unfenced
// GetNode read to report the node's resulting state. That read is not part
// of the same atomic operation — a second worker could in principle claim
// and complete the node again between the two calls on a retried node — so
// a client that needs the authoritative answer should treat these two
// fields as an optimization, not a promise, and confirm via GetNode or Watch
// when it matters. This is a disclosed limitation of the core Manager API,
// not a bug in this adapter: the fix belongs in dagworker.Manager.Nack's
// signature, which is outside this module.
func (w *workerServer) FailNode(ctx context.Context, req *pb.FailNodeRequest) (*pb.FailNodeResponse, error) {
	lease, err := decodeTaskToken(req.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if err := w.mgr.Nack(ctx, lease, errors.New(req.GetMessage())); err != nil { //nolint:err113 // the worker's own message, not a sentinel
		return nil, mapError(err)
	}

	resp := &pb.FailNodeResponse{}
	if node, getErr := w.mgr.GetNode(ctx, lease.Scope, lease.NodeID); getErr == nil {
		resp.WillRetry = node.Status == dw.StatusNew
	}
	return resp, nil
}

// SkipNode reports that the worker looked and found nothing to do. This is
// terminal on the first report — dagworker.Manager.Skip never retries it —
// so unlike FailNode there is no will_retry to report back.
func (w *workerServer) SkipNode(ctx context.Context, req *pb.SkipNodeRequest) (*pb.SkipNodeResponse, error) {
	lease, err := decodeTaskToken(req.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if err := w.mgr.Skip(ctx, lease, req.GetReason()); err != nil {
		return nil, mapError(err)
	}
	return &pb.SkipNodeResponse{}, nil
}
