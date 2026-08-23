package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http" //nolint:depguard // this file IS the HTTP adapter; core-no-network in .golangci.yml targets the core module (ADR-0037), not adapters/*
	"net/url"
	"strings"
	"time"
)

// Client talks to one dagworker HTTP/JSON server.
//
// A Client is safe for concurrent use: every method is a single request/
// response round trip with no shared mutable state beyond the underlying
// [*net/http.Client], which already promises the same.
type Client struct {
	baseURL       string
	http          *http.Client
	renewTimeout  time.Duration
	authorization string
}

// Option configures a [Client].
type Option interface{ apply(*Client) }

type optionFunc func(*Client)

func (f optionFunc) apply(c *Client) { f(c) }

// WithHTTPClient replaces the underlying [*net/http.Client] — set this to
// configure TLS, a proxy, or a custom Transport. The default is
// [http.DefaultClient].
func WithHTTPClient(hc *http.Client) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(c *Client) {
		if hc != nil {
			c.http = hc
		}
	})
}

// WithBearerToken makes every request carry
// "Authorization: Bearer <token>", which is what a server configured with
// httpadapter.BearerToken expects.
//
// A bearer token is readable by anything on the path, so a baseURL of
// "http://..." that is not loopback hands the credential to the network. Use
// https, or a unix socket via a custom Transport ([WithHTTPClient]). An empty
// token is ignored rather than sent as an empty header, so a missing
// environment variable produces the server's own 401 rather than a malformed
// request.
func WithBearerToken(token string) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(c *Client) {
		if token != "" {
			c.authorization = "Bearer " + token
		}
	})
}

// WithRenewTimeout bounds each individual background renew call a [Handle]
// makes. It is deliberately independent of any deadline the caller used for
// [Client.Claim]: a renew call happens on the Handle's own schedule, long
// after Claim returned.
func WithRenewTimeout(d time.Duration) Option { //nolint:ireturn // ADR-0027 functional-option pattern (see .golangci.yml's own dagworker.Option/memory.Option allowance); this Option is the identical opaque-interface shape for this module
	return optionFunc(func(c *Client) {
		if d > 0 {
			c.renewTimeout = d
		}
	})
}

// New returns a Client talking to baseURL, e.g. "http://localhost:8080/v1".
// A trailing slash is tolerated and stripped.
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL:      strings.TrimRight(baseURL, "/"),
		http:         http.DefaultClient,
		renewTimeout: 10 * time.Second,
	}
	for _, o := range opts {
		if o != nil {
			o.apply(c)
		}
	}
	return c
}

// Problem is an RFC 9457 Problem Details object, decoded from a non-2xx
// response's application/problem+json body (adapter contract §3).
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
}

// StatusError is returned for any response the server did not answer with a
// 2xx or a 204. It carries the decoded [Problem] so a caller can branch on
// Problem.Type — e.g. distinguishing "lease-superseded" (never retry: the
// work may already be redone) from "shutting-down" (retry against another
// instance) — the same way a gRPC client branches on a status code.
type StatusError struct {
	StatusCode int
	Problem    Problem
}

func (e *StatusError) Error() string {
	if e.Problem.Title != "" {
		return fmt.Sprintf("dagworker: %s (http %d, %s)", e.Problem.Title, e.StatusCode, e.Problem.Type)
	}
	return fmt.Sprintf("dagworker: unexpected http status %d", e.StatusCode)
}

// do performs one request/response round trip, marshaling body as the
// request JSON and unmarshaling into out when the response carries one. It
// returns the raw status code so callers that care about 204-vs-200 (Claim,
// the events poll fallback) can branch on it directly.
func (c *Client) do(ctx context.Context, method, path string, body, out any) (int, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("dagworker: encoding request body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return 0, fmt.Errorf("dagworker: building request: %w", err)
	}
	if rdr != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.authorization != "" {
		req.Header.Set("Authorization", c.authorization)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("dagworker: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("dagworker: reading response body: %w", err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		var p Problem
		_ = json.Unmarshal(data, &p) // best-effort: a malformed or absent body still yields a usable StatusError
		return resp.StatusCode, &StatusError{StatusCode: resp.StatusCode, Problem: p}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("dagworker: decoding response body: %w", err)
		}
	}
	return resp.StatusCode, nil
}

func scopePath(scope string) string { return "/scopes/" + url.PathEscape(scope) }

// leasePath is not escaped beyond scope: a lease token is base64url
// (encoding/base64's RawURLEncoding alphabet, [A-Za-z0-9_-]), which is
// already URL-safe unescaped by construction — see the server's
// leaseid.go.
func leasePath(scope, leaseID, verb string) string {
	return scopePath(scope) + "/leases/" + leaseID + ":" + verb
}

// --------------------------------------------------------------- nodes

// Node is a node snapshot as returned by the server, with the payload
// already base64-decoded back to raw bytes.
type Node struct {
	Name, Scope, ID, Kind   string
	Status, Reason, Message string
	Attempt                 uint32
	Priority                int16
	Trigger                 string
	Payload                 []byte
	Labels                  map[string]string
	Seq                     uint64
	CreatedAt, UpdatedAt    time.Time
}

type nodeJSON struct {
	Name            string            `json:"name"`
	Scope           string            `json:"scope"`
	ID              string            `json:"id"`
	Kind            string            `json:"kind"`
	Status          string            `json:"status"`
	Reason          string            `json:"reason"`
	Message         string            `json:"message"`
	Attempt         uint32            `json:"attempt"`
	Priority        int16             `json:"priority"`
	Trigger         string            `json:"trigger"`
	PayloadEncoding string            `json:"payload_encoding"`
	Payload         string            `json:"payload"`
	Labels          map[string]string `json:"labels"`
	Seq             uint64            `json:"seq"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

func (n nodeJSON) toNode() (Node, error) {
	var payload []byte
	if n.Payload != "" {
		var err error
		if payload, err = base64.StdEncoding.DecodeString(n.Payload); err != nil {
			return Node{}, fmt.Errorf("dagworker: decoding node payload: %w", err)
		}
	}
	return Node{
		Name: n.Name, Scope: n.Scope, ID: n.ID, Kind: n.Kind,
		Status: n.Status, Reason: n.Reason, Message: n.Message,
		Attempt: n.Attempt, Priority: n.Priority, Trigger: n.Trigger,
		Payload: payload, Labels: n.Labels, Seq: n.Seq,
		CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt,
	}, nil
}

// CreateNodeOptions configures [Client.CreateNode].
type CreateNodeOptions struct {
	Kind         string
	Payload      []byte
	Priority     int16
	Trigger      string
	MaxAttempts  uint32
	Dependencies []string
	Labels       map[string]string
}

// CreateNode creates a node with a client-chosen ID. Re-creating an existing
// node with a byte-identical definition is a no-op (200), per the core
// library's own idempotent-PUT contract; a different definition under the
// same ID is a 409 [StatusError].
func (c *Client) CreateNode(ctx context.Context, scope, id string, opts CreateNodeOptions) (Node, error) {
	body := map[string]any{}
	if opts.Kind != "" {
		body["kind"] = opts.Kind
	}
	if len(opts.Payload) > 0 {
		body["payload_encoding"] = "base64"
		body["payload"] = base64.StdEncoding.EncodeToString(opts.Payload)
	}
	if opts.Priority != 0 {
		body["priority"] = opts.Priority
	}
	if opts.Trigger != "" {
		body["trigger"] = opts.Trigger
	}
	if opts.MaxAttempts != 0 {
		body["max_attempts"] = opts.MaxAttempts
	}
	if len(opts.Dependencies) > 0 {
		body["dependencies"] = opts.Dependencies
	}
	if len(opts.Labels) > 0 {
		body["labels"] = opts.Labels
	}

	var n nodeJSON
	if _, err := c.do(ctx, http.MethodPut, scopePath(scope)+"/nodes/"+url.PathEscape(id), body, &n); err != nil {
		return Node{}, err
	}
	return n.toNode()
}

// GetNode fetches one node.
func (c *Client) GetNode(ctx context.Context, scope, id string) (Node, error) {
	var n nodeJSON
	if _, err := c.do(ctx, http.MethodGet, scopePath(scope)+"/nodes/"+url.PathEscape(id), nil, &n); err != nil {
		return Node{}, err
	}
	return n.toNode()
}

// --------------------------------------------------------------- claim

// ClaimOptions configures [Client.Claim].
type ClaimOptions struct {
	// WorkerID identifies the claimant for observability. It has no bearing on
	// correctness (the core library's own stance — node.go).
	WorkerID string
	// MaxNodes caps how many leases one call may return. Zero means one.
	MaxNodes int
	// LeaseTimeout is the requested lease duration. Zero uses the scope's
	// configured default.
	LeaseTimeout time.Duration
	// Wait is how long to let the server hold the connection open waiting for
	// work. It is clamped server-side to 60s regardless of what is asked here
	// (adapter contract §2).
	Wait time.Duration
	// Kinds restricts the claim to these ready-set partitions. Empty claims
	// from any kind.
	Kinds []string
}

type claimRequestJSON struct {
	WorkerID     string   `json:"worker_id,omitempty"`
	MaxNodes     int      `json:"max_nodes,omitempty"`
	LeaseSeconds int64    `json:"lease_seconds,omitempty"`
	Wait         string   `json:"wait,omitempty"`
	Kinds        []string `json:"kinds,omitempty"`
}

// Lease is a granted lease as returned by :claim. ID is the opaque token
// every subsequent :complete/:fail/:skip/:renew call on this lease presents
// back verbatim.
type Lease struct {
	ID           string
	FencingEpoch uint64
	Node         Node
	Deadline     time.Time
}

type leaseJSON struct {
	LeaseID       string    `json:"lease_id"`
	FencingEpoch  uint64    `json:"fencing_epoch"`
	Node          nodeJSON  `json:"node"`
	LeaseDeadline time.Time `json:"lease_deadline"`
}

type claimResponseJSON struct {
	Leases []leaseJSON `json:"leases"`
}

// Claim performs one blocking-query claim call. A 204 (nothing ready before
// Wait elapsed) is not an error — it returns a nil slice and a nil error, and
// a caller polling in a loop should treat both outcomes identically: check
// ctx, call Claim again.
func (c *Client) Claim(ctx context.Context, scope string, opts ClaimOptions) ([]Lease, error) {
	req := claimRequestJSON{WorkerID: opts.WorkerID, MaxNodes: opts.MaxNodes, Kinds: opts.Kinds}
	if opts.LeaseTimeout > 0 {
		req.LeaseSeconds = int64(opts.LeaseTimeout / time.Second)
	}
	if opts.Wait > 0 {
		req.Wait = opts.Wait.String()
	}

	var resp claimResponseJSON
	status, err := c.do(ctx, http.MethodPost, scopePath(scope)+"/nodes:claim", req, &resp)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	leases := make([]Lease, len(resp.Leases))
	for i, l := range resp.Leases {
		n, err := l.Node.toNode()
		if err != nil {
			return nil, err
		}
		leases[i] = Lease{ID: l.LeaseID, FencingEpoch: l.FencingEpoch, Node: n, Deadline: l.LeaseDeadline}
	}
	return leases, nil
}

// --------------------------------------------------------------- lease acks

// CompletionResult reports a node's state after :complete, :fail, or :skip.
type CompletionResult struct {
	Node          string
	Status        string
	Reason        string
	CompletedAt   time.Time
	Retrying      bool
	NextAttemptAt *time.Time
}

type completionResultJSON struct {
	Node          string     `json:"node"`
	Status        string     `json:"status"`
	Reason        string     `json:"reason"`
	CompletedAt   time.Time  `json:"completed_at"`
	Retrying      bool       `json:"retrying"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
}

// toResult converts via a plain type conversion rather than a field-by-field
// literal: completionResultJSON and CompletionResult have identical field
// names, types, and order by design (only the json tags differ, which struct
// conversion ignores), so the conversion is exact and breaks at compile time
// the moment the two shapes diverge.
func (j completionResultJSON) toResult() CompletionResult {
	return CompletionResult(j)
}

// Complete acknowledges success. It is fenced on the epoch packed into
// leaseID (see the server's leaseid.go): a lease superseded by a later claim
// — because this worker stalled past its deadline — is refused with a 409
// [StatusError] wrapping the "lease-superseded" problem type, never silently
// accepted.
//
// ctx bounds only this HTTP call. It must not be a context captured from the
// original [Client.Claim] call: that call already returned, and its
// deadline has nothing to do with how long finishing the work should take —
// exactly the mistake docs/spec/02-adapter-contract.md §2 exists to name.
func (c *Client) Complete(ctx context.Context, scope, leaseID string, result []byte) (CompletionResult, error) {
	body := map[string]string{}
	if len(result) > 0 {
		body["result_encoding"] = "base64"
		body["result"] = base64.StdEncoding.EncodeToString(result)
	}
	var out completionResultJSON
	if _, err := c.do(ctx, http.MethodPost, leasePath(scope, leaseID, "complete"), body, &out); err != nil {
		return CompletionResult{}, err
	}
	return out.toResult(), nil
}

// Fail reports that the attempt failed. Whether this ends the node or
// schedules another attempt is the scope's retry policy, not this call's
// decision (mirroring [dagworker.Manager.Nack]'s own doc comment).
func (c *Client) Fail(ctx context.Context, scope, leaseID, message string) (CompletionResult, error) {
	body := map[string]string{}
	if message != "" {
		body["message"] = message
	}
	var out completionResultJSON
	if _, err := c.do(ctx, http.MethodPost, leasePath(scope, leaseID, "fail"), body, &out); err != nil {
		return CompletionResult{}, err
	}
	return out.toResult(), nil
}

// Skip reports there was nothing to do. It is terminal on the first report:
// a retry would reach the same conclusion (mirroring
// [dagworker.Manager.Skip]'s own doc comment).
func (c *Client) Skip(ctx context.Context, scope, leaseID, reason string) (CompletionResult, error) {
	body := map[string]string{}
	if reason != "" {
		body["reason"] = reason
	}
	var out completionResultJSON
	if _, err := c.do(ctx, http.MethodPost, leasePath(scope, leaseID, "skip"), body, &out); err != nil {
		return CompletionResult{}, err
	}
	return out.toResult(), nil
}

// RenewResult reports a lease's new deadline after :renew.
type RenewResult struct {
	LeaseID      string
	FencingEpoch uint64
	Deadline     time.Time
}

type renewResultJSON struct {
	LeaseID       string    `json:"lease_id"`
	FencingEpoch  uint64    `json:"fencing_epoch"`
	LeaseDeadline time.Time `json:"lease_deadline"`
}

// Renew extends a lease's deadline before it expires — a heartbeat, not a
// completion. See [Handle] for the automatic version of this call, and its
// doc comment for why the automatic version deliberately does not take a
// caller-supplied context the way this direct call does.
func (c *Client) Renew(ctx context.Context, scope, leaseID string, leaseTimeout time.Duration) (RenewResult, error) {
	body := map[string]int64{}
	if leaseTimeout > 0 {
		body["lease_seconds"] = int64(leaseTimeout / time.Second)
	}
	var out renewResultJSON
	if _, err := c.do(ctx, http.MethodPost, leasePath(scope, leaseID, "renew"), body, &out); err != nil {
		return RenewResult{}, err
	}
	return RenewResult{LeaseID: out.LeaseID, FencingEpoch: out.FencingEpoch, Deadline: out.LeaseDeadline}, nil
}
