package grpcadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	grpcadapter "github.com/specialistvlad/dagworker/adapters/grpc"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// withToken returns a context carrying an Authorization header, the way any
// gRPC client attaches a bearer credential.
func withToken(ctx context.Context, value string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", value)
}

// TestAuthorizerGuardsEveryMethod calls every RPC both services expose and
// asserts none of them is reachable without a credential. The point is not
// that each call would otherwise succeed — several would fail on their
// arguments — but that authorization is decided before any of that.
func TestAuthorizerGuardsEveryMethod(t *testing.T) {
	t.Parallel()

	h := newHarness(t, grpcadapter.WithAuthorizer(grpcadapter.BearerToken("secret")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	calls := map[string]func() error{
		"ClaimNode": func() error {
			_, err := h.worker.ClaimNode(ctx, &pb.ClaimNodeRequest{Scope: "s"})
			return err
		},
		"CompleteNode": func() error {
			_, err := h.worker.CompleteNode(ctx, &pb.CompleteNodeRequest{TaskToken: []byte("x")})
			return err
		},
		"FailNode": func() error {
			_, err := h.worker.FailNode(ctx, &pb.FailNodeRequest{TaskToken: []byte("x")})
			return err
		},
		"ExtendLease": func() error {
			_, err := h.worker.ExtendLease(ctx, &pb.ExtendLeaseRequest{TaskToken: []byte("x")})
			return err
		},
		"AddNodes": func() error {
			_, err := h.control.AddNodes(ctx, &pb.AddNodesRequest{Scope: "s"})
			return err
		},
		"GetNode": func() error {
			_, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "s", NodeId: "n"})
			return err
		},
		"Watch": func() error {
			stream, err := h.control.Watch(ctx)
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		},
	}

	for name, call := range calls {
		if got := status.Code(call()); got != codes.Unauthenticated {
			t.Errorf("%s without a credential: got %v, want Unauthenticated", name, got)
		}
	}
}

func TestAuthorizerRejections(t *testing.T) {
	t.Parallel()

	h := newHarness(t, grpcadapter.WithAuthorizer(grpcadapter.BearerToken("right")))

	cases := []struct {
		name   string
		header string
		want   codes.Code
	}{
		{"no header", "", codes.Unauthenticated},
		{"empty bearer", "Bearer ", codes.Unauthenticated},
		{"wrong scheme", "Basic cm9vdDpyb290", codes.Unauthenticated},
		{"wrong token", "Bearer wrong", codes.PermissionDenied},
		{"token prefix of the real one", "Bearer righ", codes.PermissionDenied},
		{"right token", "Bearer right", codes.OK},
		{"scheme is case-insensitive", "bearer right", codes.OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.header != "" {
				ctx = withToken(ctx, tc.header)
			}
			_, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "s", NodeId: "absent"})
			got := status.Code(err)
			if tc.want == codes.OK {
				// Authorized: the call reaches the handler, which reports the
				// node genuinely is not there. Anything else means the
				// credential was rejected.
				if got != codes.NotFound {
					t.Fatalf("got %v (%v), want the call to reach the handler", got, err)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("got %v (%v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestBearerTokenWithNoTokensRejectsEverything(t *testing.T) {
	t.Parallel()

	// A credential set that degrades to "allow everything" is the one outcome
	// this must never have.
	for _, tokens := range [][]string{nil, {}, {""}, {"", ""}} {
		a := grpcadapter.BearerToken(tokens...)
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Bearer anything"))
		if got := status.Code(a.Authorize(ctx, "/x/y")); got != codes.PermissionDenied {
			t.Fatalf("BearerToken(%q) returned %v", tokens, got)
		}
	}
}

func TestAuthorizerErrorsFailClosed(t *testing.T) {
	t.Parallel()

	// An Authorizer that returns something outside the taxonomy has hit a bug
	// or an outage in whatever it consults. That must deny, and must not echo
	// its own reasoning to the caller.
	const leak = "identity service unreachable: dial tcp 10.0.0.7:443"
	h := newHarness(t, grpcadapter.WithAuthorizer(grpcadapter.AuthorizerFunc(
		func(context.Context, string) error { return errors.New(leak) })))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.control.GetNode(withToken(ctx, "Bearer whatever"),
		&pb.GetNodeRequest{Scope: "s", NodeId: "n"})

	st, _ := status.FromError(err)
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("got %v, want PermissionDenied", st.Code())
	}
	if st.Message() == leak {
		t.Fatalf("the authorizer's own error reached the caller: %q", st.Message())
	}
}

func TestAuthorizerSeesTheMethod(t *testing.T) {
	t.Parallel()

	// A policy that lets workers claim but not seal needs the method name, so
	// it has to arrive in its fully-qualified form.
	seen := make(chan string, 1)
	h := newHarness(t, grpcadapter.WithAuthorizer(grpcadapter.AuthorizerFunc(
		func(_ context.Context, method string) error {
			select {
			case seen <- method:
			default:
			}
			return nil
		})))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "s", NodeId: "n"})

	select {
	case got := <-seen:
		if got != "/dagworker.v1.ControlService/GetNode" {
			t.Fatalf("authorizer saw method %q", got)
		}
	default:
		t.Fatal("the authorizer never ran")
	}
}

func TestNoAuthorizerLeavesTheServerOpen(t *testing.T) {
	t.Parallel()

	// The documented default, asserted so that changing it is a deliberate act
	// with a failing test attached rather than a silent one.
	h := newHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := h.control.GetNode(ctx, &pb.GetNodeRequest{Scope: "s", NodeId: "n"})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("got %v, want the unauthenticated call to reach the handler", got)
	}
}
