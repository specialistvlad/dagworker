package grpcadapter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordingScopeAuth allows one scope and records everything it was asked
// about, so a test can assert not only that a call was refused but that the
// authorizer was consulted at all -- the failure this whole facet exists to
// prevent is a method that silently skips the check.
type recordingScopeAuth struct {
	allow string

	mu   sync.Mutex
	seen []string
}

func (a *recordingScopeAuth) Authorize(context.Context, string) error { return nil }

func (a *recordingScopeAuth) AuthorizeScope(_ context.Context, _, scope string) error {
	a.mu.Lock()
	a.seen = append(a.seen, scope)
	a.mu.Unlock()
	if scope != a.allow {
		return status.Errorf(codes.PermissionDenied, "not permitted in scope %q", scope)
	}
	return nil
}

func (a *recordingScopeAuth) scopes() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen...)
}

// TestScopeAuthorizerGuardsEveryUnaryMethod is the point of the facet: a policy
// of "this caller may reach tenant-a and nowhere else" must hold on every RPC,
// including the four worker calls that name a node by lease rather than by
// scope, where the scope has to come out of the task token.
func TestScopeAuthorizerGuardsEveryUnaryMethod(t *testing.T) {
	t.Parallel()

	auth := &recordingScopeAuth{allow: "allowed"}
	h := newHarness(t, grpcadapter.WithAuthorizer(auth))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A well-formed task token naming a scope the policy forbids, minted
	// through a permissive server so that the only thing under test below is
	// the scope check and not the claim.
	forbiddenToken := mustTokenFor(t, "forbidden", "n")

	calls := map[string]func() error{
		"ClaimNode": func() error {
			_, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{Scope: "forbidden"})
			return err
		},
		"AddNodes": func() error {
			_, err := h.control.AddNodes(ctx, &pb.AddNodesRequest{Scope: "forbidden"})
			return err
		},
		"GetNode": func() error {
			_, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "forbidden", NodeId: "n"})
			return err
		},
		"ScopeStats": func() error {
			_, err := h.control.ScopeStats(ctx, &pb.ScopeStatsRequest{Scope: "forbidden"})
			return err
		},
		"CancelScope": func() error {
			_, err := h.control.CancelScope(ctx, &pb.CancelScopeRequest{Scope: "forbidden"})
			return err
		},
		"Seal": func() error {
			_, err := h.control.Seal(ctx, &pb.SealRequest{Scope: "forbidden"})
			return err
		},
		// The four that carry a task token rather than a scope field.
		"CompleteNode": func() error {
			_, err := h.worker.CompleteNode(ctx, &pb.CompleteNodeRequest{TaskToken: forbiddenToken})
			return err
		},
		"FailNode": func() error {
			_, err := h.worker.FailNode(ctx, &pb.FailNodeRequest{TaskToken: forbiddenToken})
			return err
		},
		"SkipNode": func() error {
			_, err := h.worker.SkipNode(ctx, &pb.SkipNodeRequest{TaskToken: forbiddenToken})
			return err
		},
		"ExtendLease": func() error {
			_, err := h.worker.ExtendLease(ctx, &pb.ExtendLeaseRequest{TaskToken: forbiddenToken})
			return err
		},
	}

	for name, call := range calls {
		if got := status.Code(call()); got != codes.PermissionDenied {
			t.Errorf("%s targeting a forbidden scope: got %v, want PermissionDenied", name, got)
		}
	}

	// And the permitted scope still works, so the policy is a filter rather
	// than a wall.
	if _, err := h.control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: "allowed", Nodes: []*pb.NewNode{{Id: "ok"}},
	}); err != nil {
		t.Fatalf("AddNodes in the permitted scope: %v", err)
	}
}

// mustTokenFor claims a node and returns its task token.
func mustTokenFor(t *testing.T, scope, id string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Claiming requires passing the scope check, so this uses a harness whose
	// authorizer permits everything -- the token only has to be well-formed.
	plain := newHarness(t)
	if _, err := plain.control.AddNodes(ctx, &pb.AddNodesRequest{
		Scope: scope, Nodes: []*pb.NewNode{{Id: id}},
	}); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}
	res, err := plain.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{Scope: scope})
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	if res.GetLease() == nil {
		t.Fatal("claim returned no lease")
	}
	return res.GetLease().GetTaskToken()
}

// TestScopeAuthorizerChecksEveryWatchCreate is the case a first-message-only
// check would miss. Watch multiplexes: one stream carries many creates, each
// naming its own scope.
func TestScopeAuthorizerChecksEveryWatchCreate(t *testing.T) {
	t.Parallel()

	auth := &recordingScopeAuth{allow: "allowed"}
	h := newHarness(t, grpcadapter.WithAuthorizer(auth))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := h.control.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// First create is permitted.
	if err := stream.Send(&pb.WatchRequest{
		Request: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Scope: "allowed"}},
	}); err != nil {
		t.Fatalf("Send(allowed): %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv after the permitted create: %v", err)
	}

	// Second create, on the same already-authorized stream, targets a scope
	// the policy forbids. It must be refused.
	if err := stream.Send(&pb.WatchRequest{
		Request: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Scope: "forbidden"}},
	}); err != nil {
		t.Fatalf("Send(forbidden): %v", err)
	}
	for {
		_, err := stream.Recv()
		if err == nil {
			continue // an event from the first watch; keep reading
		}
		if got := status.Code(err); got != codes.PermissionDenied {
			t.Fatalf("second create on a forbidden scope: got %v (%v), want PermissionDenied", got, err)
		}
		break
	}

	if seen := auth.scopes(); len(seen) < 2 {
		t.Fatalf("the authorizer saw %v; both creates should have been checked", seen)
	}
}

// TestScopeAuthorizerIsOptional: an Authorizer that does not implement the
// facet keeps working untouched, which is what makes this non-breaking.
func TestScopeAuthorizerIsOptional(t *testing.T) {
	t.Parallel()

	h := newHarness(t, grpcadapter.WithAuthorizer(grpcadapter.AuthorizerFunc(
		func(context.Context, string) error { return nil })))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "any", NodeId: "n"}); status.Code(err) != codes.NotFound {
		t.Fatalf("a plain Authorizer no longer reaches the handler: %v", err)
	}
}
