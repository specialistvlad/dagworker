package grpcadapter

import (
	"testing"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// noScope is the shape a future RPC might have: a request that names its scope
// in some way scopeOfRequest does not know about.
type noScope struct{}

func TestScopeOfRequest(t *testing.T) {
	t.Parallel()

	token, err := encodeTaskToken(dw.Lease{Scope: "from-token", NodeID: "n", Epoch: 1})
	if err != nil {
		t.Fatalf("encodeTaskToken: %v", err)
	}

	t.Run("a scope field", func(t *testing.T) {
		t.Parallel()
		got, err := ScopeOfRequest(&pb.GetNodeRequest{Scope: "direct", NodeId: "n"})
		if err != nil || got != "direct" {
			t.Fatalf("got %q, %v", got, err)
		}
	})

	t.Run("a task token", func(t *testing.T) {
		t.Parallel()
		got, err := ScopeOfRequest(&pb.CompleteNodeRequest{TaskToken: token})
		if err != nil || got != "from-token" {
			t.Fatalf("got %q, %v", got, err)
		}
	})

	t.Run("a malformed task token is refused", func(t *testing.T) {
		t.Parallel()
		if _, err := ScopeOfRequest(&pb.CompleteNodeRequest{TaskToken: []byte("nonsense")}); err == nil {
			t.Fatal("a token that does not decode yielded a scope")
		}
	})

	// The one that matters. An RPC added later whose scope lives somewhere new
	// must fail loudly, because a scope authorizer that silently does not run
	// on one method is worse than no scope authorizer at all.
	t.Run("an unrecognised request fails closed", func(t *testing.T) {
		t.Parallel()
		_, err := ScopeOfRequest(noScope{})
		if err == nil {
			t.Fatal("an unrecognised request was waved through instead of refused")
		}
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("got %v, want Internal", got)
		}
	})
}
