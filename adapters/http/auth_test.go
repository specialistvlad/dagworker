package httpadapter

import (
	"errors"
	"net/http"
	"testing"
)

// authedRequest performs one request with an optional Authorization header.
func authedRequest(t *testing.T, method, url, authorization string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func TestAuthorizerGuardsEveryRoute(t *testing.T) {
	t.Parallel()

	// Every route the server exposes, as a method and a path that would
	// otherwise reach a handler. The point is not that each one works — other
	// tests cover that — but that none of them is reachable without a
	// credential, including the OpenAPI document and the routes that do not
	// name a scope.
	routes := []struct{ method, path string }{
		{http.MethodGet, "/v1/scopes"},
		{http.MethodPut, "/v1/scopes/s"},
		{http.MethodGet, "/v1/scopes/s"},
		{http.MethodPost, "/v1/scopes/s:seal"},
		{http.MethodPut, "/v1/scopes/s/nodes/n"},
		{http.MethodGet, "/v1/scopes/s/nodes/n"},
		{http.MethodDelete, "/v1/scopes/s/nodes/n"},
		{http.MethodGet, "/v1/scopes/s/nodes"},
		{http.MethodPost, "/v1/scopes/s/nodes:claim"},
		{http.MethodPost, "/v1/scopes/s/nodes/n:cancel"},
		{http.MethodPut, "/v1/scopes/s/edges/a..b"},
		{http.MethodDelete, "/v1/scopes/s/edges/a..b"},
		{http.MethodGet, "/v1/scopes/s/leases/tok"},
		{http.MethodPost, "/v1/scopes/s/leases/tok:complete"},
		{http.MethodGet, "/v1/scopes/s/events"},
		{http.MethodGet, "/openapi.yaml"},
	}

	base, _, shutdown := testServer(t, WithAuthorizer(BearerToken("secret")))
	defer shutdown()

	for _, rt := range routes {
		resp := authedRequest(t, rt.method, base+rt.path, "")
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s %s without a credential: got %d, want 401",
				rt.method, rt.path, resp.StatusCode)
		}
		if got := resp.Header.Get("WWW-Authenticate"); got == "" {
			t.Errorf("%s %s: 401 with no WWW-Authenticate challenge", rt.method, rt.path)
		}
	}
}

func TestAuthorizerRejections(t *testing.T) {
	t.Parallel()

	base, _, shutdown := testServer(t, WithAuthorizer(BearerToken("right")))
	// t.Cleanup, not defer: the subtests below are parallel, so they run after
	// this function returns and a deferred shutdown would close the listener
	// out from under every one of them.
	t.Cleanup(shutdown)

	cases := []struct {
		name       string
		header     string
		wantStatus int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"empty bearer", "Bearer ", http.StatusUnauthorized},
		{"wrong scheme", "Basic cm9vdDpyb290", http.StatusUnauthorized},
		{"bearer with no space", "Bearerright", http.StatusUnauthorized},
		{"wrong token", "Bearer wrong", http.StatusForbidden},
		{"token prefix of the real one", "Bearer righ", http.StatusForbidden},
		{"token with the real one as a prefix", "Bearer rightish", http.StatusForbidden},
		{"right token", "Bearer right", http.StatusOK},
		{"scheme is case-insensitive", "bearer right", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := authedRequest(t, http.MethodGet, base+"/v1/scopes", tc.header)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("got %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusForbidden && resp.Header.Get("WWW-Authenticate") != "" {
				t.Error("403 carried a challenge; retrying the same token cannot help")
			}
		})
	}
}

func TestBearerTokenWithNoTokensRejectsEverything(t *testing.T) {
	t.Parallel()

	// A credential set that degrades to "allow everything" is the one outcome
	// this must never have: an operator who configures an empty token file
	// must get a closed door, not an open one.
	for _, tokens := range [][]string{nil, {}, {""}, {"", ""}} {
		a := BearerToken(tokens...)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://x/v1/scopes", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Authorization", "Bearer anything")
		if err := a.Authorize(req); !errors.Is(err, ErrPermissionDenied) {
			t.Fatalf("BearerToken(%q) allowed a request: %v", tokens, err)
		}
	}
}

func TestAuthorizerErrorsFailClosed(t *testing.T) {
	t.Parallel()

	// An Authorizer that returns something outside the taxonomy has hit a bug
	// or an outage in whatever it consults. That must deny, and must not echo
	// its own reasoning to the caller.
	const leak = "identity service unreachable: dial tcp 10.0.0.7:443"
	base, _, shutdown := testServer(t, WithAuthorizer(
		AuthorizerFunc(func(*http.Request) error { return errors.New(leak) })))
	defer shutdown()

	resp := authedRequest(t, http.MethodGet, base+"/v1/scopes", "Bearer whatever")
	defer func() { _ = resp.Body.Close() }() // decodeBody closes too; a second Close is a no-op
	var body map[string]any
	decodeBody(t, resp, &body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("got %d, want 403", resp.StatusCode)
	}
	if detail, _ := body["detail"].(string); detail == leak {
		t.Fatalf("the authorizer's own error reached the caller: %q", detail)
	}
}

func TestAuthorizerSeesTheScope(t *testing.T) {
	t.Parallel()

	// A per-scope policy is the first thing anyone writes past a shared
	// secret, so the scope must be recoverable before routing has run.
	cases := []struct{ path, want string }{
		{"/v1/scopes", ""},
		{"/v1/scopes/alpha", "alpha"},
		{"/v1/scopes/alpha:seal", "alpha"},
		{"/v1/scopes/alpha/nodes/n", "alpha"},
		{"/v1/scopes/alpha/nodes/n:cancel", "alpha"},
		{"/v1/scopes/alpha/events", "alpha"},
		{"/openapi.yaml", ""},
	}
	for _, tc := range cases {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://x"+tc.path, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		if got := RequestScope(req); got != tc.want {
			t.Errorf("RequestScope(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestNoAuthorizerLeavesTheServerOpen(t *testing.T) {
	t.Parallel()

	// The documented default, asserted so that changing it is a deliberate
	// act with a failing test attached rather than a silent one.
	base, _, shutdown := testServer(t)
	defer shutdown()

	resp := authedRequest(t, http.MethodGet, base+"/v1/scopes", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
}
