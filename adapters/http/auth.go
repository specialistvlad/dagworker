package httpadapter

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"strings"
)

// The two errors an [Authorizer] returns. They exist in this package rather
// than in the core module because authentication is a property of a network
// surface, and the core module has none (ADR-0037): a Manager reached through
// a Go function call is already inside the caller's trust boundary.
var (
	// ErrUnauthenticated means the request carried no usable credential. It
	// maps to 401 with a WWW-Authenticate challenge.
	ErrUnauthenticated = errors.New("dagworker/http: unauthenticated")

	// ErrPermissionDenied means the credential was understood and is not
	// allowed to do this. It maps to 403, deliberately with no challenge:
	// retrying with the same credential cannot help.
	ErrPermissionDenied = errors.New("dagworker/http: permission denied")
)

// Authorizer decides whether a request may proceed. It runs before routing,
// so every endpoint is covered by construction — including ones added later,
// which is the whole reason it is not a per-handler check.
//
// Return nil to allow. Return [ErrUnauthenticated] or [ErrPermissionDenied]
// to reject with the matching status. Any other error is treated as a denial
// and reported as 403: an authorizer that fails for a reason it did not
// anticipate must fail closed, never open.
//
// The request is the whole input on purpose. Method, path, headers, TLS peer
// certificates and the scope segment are all reachable from it, so a policy
// can be as coarse as "is this token in the set" or as fine as "this identity
// may claim in this scope but not seal it" without this package having to
// model authorization itself — which it cannot do well for a deployment it
// knows nothing about. [RequestScope] extracts the scope a request addresses.
//
// The implementation must be safe for concurrent use and must not block: it
// is on the path of every request, including a claim long-poll that a fleet
// of workers is holding open.
type Authorizer interface {
	Authorize(r *http.Request) error
}

// AuthorizerFunc adapts a plain function to [Authorizer].
type AuthorizerFunc func(r *http.Request) error

// Authorize implements [Authorizer].
func (f AuthorizerFunc) Authorize(r *http.Request) error { return f(r) }

// RequestScope returns the scope segment of the request's path, or "" for a
// request that does not address one (the OpenAPI document, the scope
// collection). It reads the routing pattern's own wildcard where there is one
// and falls back to parsing the path, so it is usable from an [Authorizer],
// which runs before routing has assigned any path values.
func RequestScope(r *http.Request) string {
	if v := r.PathValue("scope"); v != "" {
		return v
	}
	// /v1/scopes/{scope}/... — and /v1/scopes/{scope}:{verb} for the
	// item-level custom methods, which routes.go captures as one segment.
	const prefix = "/v1/scopes/"
	p, ok := strings.CutPrefix(r.URL.Path, prefix)
	if !ok {
		return ""
	}
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	if i := strings.LastIndexByte(p, ':'); i > 0 {
		p = p[:i]
	}
	return p
}

// BearerToken returns an [Authorizer] that accepts a request carrying
// "Authorization: Bearer <t>" for any t in tokens, and rejects everything
// else. It is shared-secret authentication and nothing more: every holder of
// a token is the same principal, with full access to every scope. That is the
// honest floor for a service whose trust model is cooperative workers
// (ADR-0035) — it establishes that a caller is one of ours, which is what
// stops an unauthenticated peer on the same network from claiming and
// completing other people's work.
//
// It is not an authorization model. A deployment that needs per-scope or
// per-operation rules, rotation, revocation, or an audit trail against a real
// identity should implement [Authorizer] over whatever already issues its
// identities. This function exists so that "we have not built that yet" does
// not mean "the port is open".
//
// Comparison is constant-time over a SHA-256 digest, so neither the token's
// length nor its content leaks through timing. An empty token, and a call with
// no tokens at all, are rejected: a credential set that accidentally
// evaluates to "allow everything" is the one outcome this must never have.
func BearerToken(tokens ...string) Authorizer { //nolint:ireturn // returning the interface is the point: the concrete type is an implementation detail callers must not name
	digests := make([][32]byte, 0, len(tokens))
	for _, t := range tokens {
		if t == "" {
			continue
		}
		digests = append(digests, sha256.Sum256([]byte(t)))
	}
	return AuthorizerFunc(func(r *http.Request) error {
		presented, ok := bearerCredential(r.Header.Get("Authorization"))
		if !ok {
			return fmt.Errorf("%w: no bearer token", ErrUnauthenticated)
		}
		sum := sha256.Sum256([]byte(presented))
		match := 0
		for i := range digests {
			// Every digest is compared, not just up to the first match: an
			// early return would make the response time depend on which token
			// was presented.
			match |= subtle.ConstantTimeCompare(sum[:], digests[i][:])
		}
		if match != 1 {
			return fmt.Errorf("%w: token not recognised", ErrPermissionDenied)
		}
		return nil
	})
}

// bearerCredential pulls the credential out of an Authorization header value.
// The scheme is matched case-insensitively, as RFC 9110 §11.1 requires.
func bearerCredential(header string) (string, bool) {
	scheme, rest, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	return rest, true
}

// authMiddleware rejects a request the authorizer does not allow, before it
// reaches the router. A nil authorizer allows everything, which is what makes
// an unauthenticated Server possible at all — appropriate for a loopback
// listener inside a trusted process, and the reason cmd/dagworkerd refuses to
// bind a non-loopback address without one.
func (s *Server) authMiddleware(a Authorizer) middleware {
	return func(next http.Handler) http.Handler {
		if a == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := a.Authorize(r); err != nil {
				s.writeAuthProblem(w, r, err)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writeAuthProblem renders a rejection. The detail is fixed text, never the
// authorizer's own error: that error is the deployment's internal reasoning
// about its own identities, and echoing it to an unauthenticated caller turns
// every custom Authorizer into an oracle. It is logged instead, where an
// operator can read it.
func (s *Server) writeAuthProblem(w http.ResponseWriter, r *http.Request, err error) {
	status, slug, title, detail := http.StatusForbidden, "permission-denied",
		"Permission Denied", "the presented credential may not perform this request"
	if errors.Is(err, ErrUnauthenticated) {
		status, slug, title, detail = http.StatusUnauthorized, "unauthenticated",
			"Unauthenticated", "a credential is required"
		w.Header().Set("WWW-Authenticate", `Bearer realm="dagworker"`)
	}
	s.opts.logger.InfoContext(r.Context(), "dagworker/http: request rejected",
		"method", r.Method, "path", r.URL.Path, "status", status, "reason", err.Error())
	writeJSON(w, status, problemContentType, problem{
		Type:     s.opts.problemBaseURI + slug,
		Title:    title,
		Status:   status,
		Detail:   detail,
		Instance: r.URL.Path,
	})
}
