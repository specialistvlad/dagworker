package grpcadapter

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Authorizer decides whether a call may proceed. It runs as the innermost
// interceptor before the handler, so every method is covered by construction
// — including ones added to the proto later, which is the reason it is not a
// per-handler check.
//
// Return nil to allow. Return a *status.Status error with codes.Unauthenticated
// or codes.PermissionDenied to reject with that code; any other error is
// treated as a denial and reported as PermissionDenied, because an authorizer
// that fails for a reason it did not anticipate must fail closed.
//
// fullMethod is the "/dagworker.v1.WorkerService/ClaimNode" form. Credentials
// arrive in the context: [metadata.FromIncomingContext] for headers and
// [google.golang.org/grpc/peer.FromContext] for the transport's TLS state, so
// a policy can key on a bearer token, an mTLS subject, or both, without this
// package modelling authorization for a deployment it knows nothing about.
//
// The implementation must be safe for concurrent use and must not block: it
// runs on every call, including a ClaimNode long poll a fleet of workers is
// holding open.
type Authorizer interface {
	Authorize(ctx context.Context, fullMethod string) error
}

// ScopeAuthorizer is the optional second half of [Authorizer], discovered by
// type assertion the way the storage port discovers [dagworker.Lister] and its
// siblings. An Authorizer that also implements it is asked, for every call,
// which scope that call targets and whether this caller may reach it.
//
// It exists because a scope is an isolation boundary for data and cost, and
// without this it could not be one for access. `fullMethod` is
// "/dagworker.v1.WorkerService/ClaimNode" and nothing more, so an Authorizer
// alone can say "this caller may claim" but never "this caller may claim from
// tenant-a and nowhere else" -- a policy the HTTP adapter can express today by
// reading the scope out of the request path. Per-scope policy was therefore
// writable on one adapter and impossible on the other.
//
// The scope is extracted before the handler runs: from the request's own scope
// field where it has one, and from the task token for the four worker calls
// that identify a node by lease rather than by name. A request whose scope
// cannot be determined is REJECTED, not waved through -- see scopeOfRequest.
//
// For the streaming Watch, every WatchCreateRequest is checked as it arrives,
// not merely the first. One stream multiplexes many watches and each names its
// own scope, so checking the first would authorize the rest by accident.
//
// Implementations must be safe for concurrent use and must not block.
type ScopeAuthorizer interface {
	AuthorizeScope(ctx context.Context, fullMethod, scope string) error
}

// AuthorizerFunc adapts a plain function to [Authorizer].
type AuthorizerFunc func(ctx context.Context, fullMethod string) error

// Authorize implements [Authorizer].
func (f AuthorizerFunc) Authorize(ctx context.Context, fullMethod string) error {
	return f(ctx, fullMethod)
}

// BearerToken returns an [Authorizer] that accepts a call carrying
// "authorization: Bearer <t>" for any t in tokens, and rejects everything
// else. It is shared-secret authentication and nothing more: every holder of
// a token is the same principal with access to every scope, which is the
// honest floor for a service whose trust model is cooperative workers
// (ADR-0035) — it establishes that a caller is one of ours, which is what
// stops an unauthenticated peer on the same network from claiming and
// completing other people's work.
//
// It is not an authorization model, and it is not confidential on its own:
// a bearer token on a plaintext connection is readable by anything on the
// path, so this belongs behind TLS. A deployment with real identities should
// implement [Authorizer] over mTLS peer certificates or its own token service.
//
// Comparison is constant-time over a SHA-256 digest. An empty token, and a
// call with no tokens at all, are rejected: a credential set that accidentally
// evaluates to "allow everything" is the one outcome this must never have.
func BearerToken(tokens ...string) Authorizer { //nolint:ireturn // returning the interface is the point: the concrete type is an implementation detail callers must not name
	digests := make([][32]byte, 0, len(tokens))
	for _, t := range tokens {
		if t == "" {
			continue
		}
		digests = append(digests, sha256.Sum256([]byte(t)))
	}
	return AuthorizerFunc(func(ctx context.Context, _ string) error {
		presented, ok := bearerCredential(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "no bearer token")
		}
		sum := sha256.Sum256([]byte(presented))
		match := 0
		for i := range digests {
			// Every digest is compared rather than returning on the first
			// match, so response time does not depend on which token was sent.
			match |= subtle.ConstantTimeCompare(sum[:], digests[i][:])
		}
		if match != 1 {
			return status.Error(codes.PermissionDenied, "token not recognised")
		}
		return nil
	})
}

// bearerCredential pulls the credential out of the incoming "authorization"
// metadata. gRPC lowercases metadata keys; the scheme is matched
// case-insensitively as RFC 9110 §11.1 requires.
func bearerCredential(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	for _, v := range md.Get("authorization") {
		scheme, rest, found := strings.Cut(v, " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			continue
		}
		if rest = strings.TrimSpace(rest); rest != "" {
			return rest, true
		}
	}
	return "", false
}

// authUnaryInterceptor rejects a call the authorizer does not allow.
func authUnaryInterceptor(a Authorizer) grpc.UnaryServerInterceptor {
	scoped, _ := a.(ScopeAuthorizer)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := a.Authorize(ctx, info.FullMethod); err != nil {
			return nil, authStatus(err)
		}
		if scoped != nil {
			scope, err := scopeOfRequest(req)
			if err != nil {
				return nil, err
			}
			if err := scoped.AuthorizeScope(ctx, info.FullMethod, scope); err != nil {
				return nil, authStatus(err)
			}
		}
		return handler(ctx, req)
	}
}

// scopeOfRequest names the scope a unary request targets.
//
// Every unary request in this API identifies one, in one of two shapes: a
// `scope` field, or a task token, which is a marshalled TaskToken carrying the
// scope of the lease it names. Both are handled here rather than by a table of
// method names, so a new RPC of either shape is covered the day it is added.
//
// A request of NEITHER shape is refused. That is the whole point: an RPC added
// later that names its scope some third way must fail loudly rather than skip
// the check, because a scope authorizer that silently does not run on one
// method is worse than no scope authorizer at all.
func scopeOfRequest(req any) (string, error) {
	switch r := req.(type) {
	case interface{ GetScope() string }:
		return r.GetScope(), nil
	case interface{ GetTaskToken() []byte }:
		lease, err := decodeTaskToken(r.GetTaskToken())
		if err != nil {
			return "", err
		}
		return string(lease.Scope), nil
	default:
		return "", status.Errorf(codes.Internal,
			"scope authorization is configured but the scope of a %T cannot be determined", req)
	}
}

// authStreamInterceptor is authUnaryInterceptor's counterpart for Watch.
func authStreamInterceptor(a Authorizer) grpc.StreamServerInterceptor {
	scoped, _ := a.(ScopeAuthorizer)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// ss.Context() is the stream's own context, which is the only one a
		// StreamServerInterceptor is given; there is no ctx parameter to
		// propagate instead.
		//nolint:contextcheck // the stream's context is the context here
		if err := a.Authorize(ss.Context(), info.FullMethod); err != nil {
			return authStatus(err)
		}
		if scoped != nil {
			ss = &scopeCheckedStream{ServerStream: ss, method: info.FullMethod, authorizer: scoped}
		}
		return handler(srv, ss)
	}
}

// scopeCheckedStream authorizes the scope named by each inbound message.
//
// It checks every message rather than only the first because Watch multiplexes:
// one stream carries many WatchCreateRequests and each names its own scope, so
// authorizing the first would authorize the rest by accident. A message that
// names no scope -- a WatchCancelRequest, which refers to a watch id that was
// already authorized when it was created -- passes through unchecked.
type scopeCheckedStream struct {
	grpc.ServerStream
	method     string
	authorizer ScopeAuthorizer
}

func (s *scopeCheckedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	scope, named := scopeOfStreamMessage(m)
	if !named {
		return nil
	}
	if err := s.authorizer.AuthorizeScope(s.Context(), s.method, scope); err != nil {
		return authStatus(err)
	}
	return nil
}

// scopeOfStreamMessage reports the scope an inbound stream message targets, and
// whether it named one at all.
//
// Unlike the unary path this cannot fail closed on an unrecognised shape: a
// stream carries control messages that legitimately name no scope, and refusing
// those would break Watch's own cancel. Adding a streaming RPC therefore means
// extending this function, which is why the adapter contract says so.
func scopeOfStreamMessage(m any) (string, bool) {
	if r, ok := m.(interface{ GetCreate() *pb.WatchCreateRequest }); ok {
		if c := r.GetCreate(); c != nil {
			return c.GetScope(), true
		}
		return "", false
	}
	if r, ok := m.(interface{ GetScope() string }); ok {
		return r.GetScope(), true
	}
	return "", false
}

// authStatus normalises whatever an Authorizer returned into a rejection.
//
// An error that is already Unauthenticated or PermissionDenied passes through
// as the authorizer wrote it. Anything else — a plain error, a status with
// some unrelated code — becomes PermissionDenied with fixed text: the
// authorizer's own message is the deployment's internal reasoning about its
// identities, and returning it to a caller that just failed to authenticate
// turns every custom Authorizer into an oracle.
func authStatus(err error) error {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied:
			return err
		case codes.OK, codes.Canceled, codes.Unknown, codes.InvalidArgument,
			codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists,
			codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted,
			codes.OutOfRange, codes.Unimplemented, codes.Internal,
			codes.Unavailable, codes.DataLoss:
		}
	}
	return status.Error(codes.PermissionDenied, "not authorized")
}
