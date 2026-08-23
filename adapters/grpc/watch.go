package grpcadapter

import (
	"context"
	"errors"
	"io"
	"sync"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
)

// watchSession is the per-RPC state for one Watch stream: the set of watches
// currently multiplexed on it, and the one mutex every Send must go through,
// since grpc.ServerStream.Send is not safe for concurrent use and each open
// watch pumps events from its own goroutine.
//
// It deliberately holds no context: every method takes the one it needs as a
// parameter, which is what lets the per-watch context differ from the
// stream's own (a single WatchCancelRequest must end one watch, never the
// whole connection).
type watchSession struct {
	stream pb.ControlService_WatchServer

	sendMu sync.Mutex

	mu      sync.Mutex
	watches map[int64]context.CancelFunc

	// wg is waited out before Watch returns, so no pump goroutine ever calls
	// Send after the handler has returned control to grpc-go — using a
	// stream past that point is a use-after-free in gRPC's own terms, not
	// merely a logic bug.
	wg sync.WaitGroup
}

func (s *watchSession) send(resp *pb.WatchResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(resp)
}

// Watch implements the bidirectional event stream: a client multiplexes any
// number of independent watches, each identified by a client-assigned
// watch_id, over one connection, and may cancel one without tearing down the
// stream or any other watch on it — etcd's Watch shape (see watch.proto).
func (c *controlServer) Watch(stream pb.ControlService_WatchServer) error {
	ctx := stream.Context()
	sess := &watchSession{stream: stream, watches: make(map[int64]context.CancelFunc)}
	defer sess.closeAll()

	reqCh, recvErrCh := readWatchRequests(ctx, stream)
	for {
		select {
		case <-c.shutdown:
			return mapError(dw.ErrClosed)
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErrCh:
			if errors.Is(err, io.EOF) {
				// The client will send no further create/cancel requests, but
				// may still be reading events from watches already open;
				// keep serving until ctx itself ends.
				recvErrCh = nil
				continue
			}
			return err
		case req, ok := <-reqCh:
			if !ok {
				continue
			}
			c.dispatchWatchRequest(ctx, sess, req)
		}
	}
}

// readWatchRequests runs stream.Recv in its own goroutine so Watch's main
// loop can select on it alongside shutdown and ctx — Recv itself has no
// context-aware variant to select on directly.
func readWatchRequests(ctx context.Context, stream pb.ControlService_WatchServer) (<-chan *pb.WatchRequest, <-chan error) {
	reqCh := make(chan *pb.WatchRequest)
	errCh := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case reqCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	return reqCh, errCh
}

func (c *controlServer) dispatchWatchRequest(ctx context.Context, sess *watchSession, req *pb.WatchRequest) {
	switch r := req.GetRequest().(type) {
	case *pb.WatchRequest_Create:
		c.createWatch(ctx, sess, r.Create)
	case *pb.WatchRequest_Cancel:
		sess.cancelWatch(r.Cancel.GetWatchId())
	}
}

// closeAll cancels every watch still open on this session and waits for
// their pump goroutines to notice and return, which is what makes it safe
// for Watch to return immediately afterward.
func (s *watchSession) closeAll() {
	s.mu.Lock()
	for _, cancel := range s.watches {
		cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// cancelWatch ends one watch without touching any other. The pump goroutine
// notices wctx canceled, drains, and — finding its entry already removed
// here — sends no further response of its own; this call sends the one
// acknowledgement.
func (s *watchSession) cancelWatch(id int64) {
	s.mu.Lock()
	cancel, ok := s.watches[id]
	if ok {
		delete(s.watches, id)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	cancel()
	_ = s.send(&pb.WatchResponse{WatchId: id, Canceled: true, CancelReason: "canceled by client"})
}

// createWatch opens one watch and, on success, hands its delivery off to a
// pump goroutine tracked by sess.wg.
func (c *controlServer) createWatch(ctx context.Context, sess *watchSession, req *pb.WatchCreateRequest) {
	id := req.GetWatchId()
	scope := dw.Scope(req.GetScope())
	if scope == "" {
		_ = sess.send(&pb.WatchResponse{WatchId: id, Canceled: true, CancelReason: "scope must not be empty"})
		return
	}

	sess.mu.Lock()
	if old, exists := sess.watches[id]; exists {
		old()
	}
	sess.mu.Unlock()

	wctx, cancel := context.WithCancel(ctx)
	opts := dw.SubscribeOptions{
		Scope: scope,
		// All three kinds: transitions and creations carry a Node and map to
		// WATCH_EVENT_KIND_TRANSITION/CREATED; EventReady is the scope-wide
		// wake signal and maps to WORK_AVAILABLE with no Node (see
		// watchEventToProto and watch.proto's WatchEventKind doc).
		Kinds:   []dw.EventKind{dw.EventCreated, dw.EventTransition, dw.EventReady},
		Durable: req.GetStartRevision() > 0 || req.GetReplay(),
		From:    dw.Cursor(req.GetStartRevision()),
		Replay:  req.GetReplay(),
	}
	sub, err := c.mgr.Subscribe(wctx, opts)
	if err != nil {
		cancel()
		c.sendWatchOpenError(ctx, sess, id, scope, err)
		return
	}

	sess.mu.Lock()
	sess.watches[id] = cancel
	sess.mu.Unlock()

	if err := sess.send(&pb.WatchResponse{WatchId: id, Created: true}); err != nil {
		cancel()
		return
	}

	sess.wg.Add(1)
	go pumpWatch(sess, id, dw.NodeID(req.GetNodeIdFilter()), sub)
}

// sendWatchOpenError reports why Subscribe refused to open the watch.
// compacted_revision is filled from the scope's current cursor, which is the
// best resync point this adapter can offer: dagworker.ErrCursorExpired is a
// bare sentinel and does not carry the exact boundary that was compacted
// away, so "the fresh revision to resync from" is reported instead of "the
// last revision you could have resumed from" — the two differ, but the
// client-side recovery (re-read current state, then watch again from here)
// is identical either way.
func (c *controlServer) sendWatchOpenError(ctx context.Context, sess *watchSession, id int64, scope dw.Scope, err error) {
	resp := &pb.WatchResponse{WatchId: id, Canceled: true, CancelReason: err.Error()}
	if errors.Is(err, dw.ErrCursorExpired) {
		if stats, statsErr := c.mgr.Stats(ctx, scope); statsErr == nil {
			resp.CompactedRevision = uint64(stats.Cursor)
		}
	}
	_ = sess.send(resp)
}

// pumpWatch forwards one subscription's events until it ends, then reports
// why — unless the watch was already explicitly canceled, in which case
// cancelWatch already sent the one notification this watch_id gets.
func pumpWatch(sess *watchSession, id int64, filter dw.NodeID, sub *dw.Subscription) {
	defer sess.wg.Done()

	for ev := range sub.Events() {
		if filter != "" && ev.NodeID != filter {
			continue
		}
		resp := &pb.WatchResponse{WatchId: id, Events: []*pb.WatchEvent{watchEventToProto(ev)}}
		if err := sess.send(resp); err != nil {
			sess.mu.Lock()
			delete(sess.watches, id)
			sess.mu.Unlock()
			return
		}
	}

	sess.mu.Lock()
	_, stillOpen := sess.watches[id]
	if stillOpen {
		delete(sess.watches, id)
	}
	sess.mu.Unlock()
	if !stillOpen {
		return
	}
	reason := ""
	if err := sub.Err(); err != nil {
		reason = err.Error()
	}
	_ = sess.send(&pb.WatchResponse{WatchId: id, Canceled: true, CancelReason: reason})
}

// watchEventToProto is an exhaustive switch over dagworker.EventKind on
// purpose: a fourth kind added to the core later must fail this package's
// build (via the exhaustive linter) rather than silently falling through to
// whatever the default case used to mean.
func watchEventToProto(ev dw.Event) *pb.WatchEvent {
	switch ev.Kind {
	case dw.EventReady:
		// The scope-wide wake signal carries no node: it is a doorbell, not
		// a delivery (see dagworker.EventReady's own doc comment).
		return &pb.WatchEvent{
			Revision: uint64(ev.Cursor),
			Kind:     pb.WatchEventKind_WATCH_EVENT_KIND_WORK_AVAILABLE,
			Gap:      ev.Gap,
		}
	case dw.EventCreated:
		return watchTransitionEvent(ev, pb.WatchEventKind_WATCH_EVENT_KIND_CREATED)
	case dw.EventTransition:
		return watchTransitionEvent(ev, pb.WatchEventKind_WATCH_EVENT_KIND_TRANSITION)
	default:
		return watchTransitionEvent(ev, pb.WatchEventKind_WATCH_EVENT_KIND_UNSPECIFIED)
	}
}

func watchTransitionEvent(ev dw.Event, kind pb.WatchEventKind) *pb.WatchEvent {
	return &pb.WatchEvent{
		Revision:       uint64(ev.Cursor),
		Kind:           kind,
		PreviousStatus: statusToProto(ev.From),
		Gap:            ev.Gap,
		Node: &pb.Node{
			Scope:   string(ev.Scope),
			Id:      string(ev.NodeID),
			Kind:    ev.NodeKind,
			Status:  statusToProto(ev.To),
			Reason:  reasonToProto(ev.Reason),
			Message: ev.Message,
			Attempt: ev.Attempt,
			Seq:     uint64(ev.Seq),
		},
	}
}
