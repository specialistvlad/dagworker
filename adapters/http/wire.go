package httpadapter

import (
	"encoding/base64"
	"fmt"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
)

// encodingBase64 is the only payload_encoding this server accepts today.
// Naming the field explicitly, rather than assuming base64, is what lets a
// future "base64+zstd" or an out-of-band blob reference join it without
// breaking every existing client (docs/research/14 §8).
const encodingBase64 = "base64"

// nodeWire is a node as it appears on the wire. It mirrors [dagworker.Node]
// field for field, with durations and enums rendered as the text forms their
// Go types already know how to produce.
type nodeWire struct {
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	ID               string            `json:"id"`
	Kind             string            `json:"kind,omitempty"`
	Status           dagworker.Status  `json:"status"`
	Reason           dagworker.Reason  `json:"reason"`
	Message          string            `json:"message,omitempty"`
	Attempt          uint32            `json:"attempt"`
	Priority         int16             `json:"priority"`
	Trigger          string            `json:"trigger"`
	RetryMaxAttempts uint32            `json:"retry_max_attempts,omitempty"`
	RetryBaseDelay   string            `json:"retry_base_delay,omitempty"`
	RetryMaxDelay    string            `json:"retry_max_delay,omitempty"`
	PayloadEncoding  string            `json:"payload_encoding,omitempty"`
	Payload          string            `json:"payload,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	Seq              uint64            `json:"seq"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

// resourceName builds the AIP-122 resource name a node or scope is addressed
// by. It is informational on the wire (clients route by URL, not by parsing
// this), included because dossier 14's examples treat it as part of the
// resource's own identity.
func resourceName(scope dagworker.Scope, id dagworker.NodeID) string {
	return fmt.Sprintf("scopes/%s/nodes/%s", scope, id)
}

func nodeToWire(n dagworker.Node) nodeWire {
	w := nodeWire{
		Name:      resourceName(n.Scope, n.ID),
		Scope:     string(n.Scope),
		ID:        string(n.ID),
		Kind:      n.Kind,
		Status:    n.Status,
		Reason:    n.Reason,
		Message:   n.Message,
		Attempt:   n.Attempt,
		Priority:  n.Priority,
		Trigger:   n.Trigger.String(),
		Labels:    n.Labels,
		Seq:       uint64(n.Seq),
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
	if n.Retry.MaxAttempts > 0 {
		w.RetryMaxAttempts = n.Retry.MaxAttempts
	}
	if n.Retry.BaseDelay > 0 {
		w.RetryBaseDelay = n.Retry.BaseDelay.String()
	}
	if n.Retry.MaxDelay > 0 {
		w.RetryMaxDelay = n.Retry.MaxDelay.String()
	}
	if len(n.Payload) > 0 {
		w.PayloadEncoding = encodingBase64
		w.Payload = base64.StdEncoding.EncodeToString(n.Payload)
	}
	return w
}

// nodeETag is the node's optimistic-concurrency validator. It is the node's
// own Seq, not a content hash: Seq is already the monotonic counter storage
// bumps on every write, so this adds no second source of truth for "did this
// change" (docs/research/14 §4).
func nodeETag(n dagworker.Node) string {
	return fmt.Sprintf(`"v%d"`, uint64(n.Seq))
}

// createNodeRequest is the PUT body for creating a node.
type createNodeRequest struct {
	Kind            string            `json:"kind,omitempty"`
	PayloadEncoding string            `json:"payload_encoding,omitempty"`
	Payload         string            `json:"payload,omitempty"`
	Priority        int16             `json:"priority,omitempty"`
	Trigger         string            `json:"trigger,omitempty"`
	MaxAttempts     uint32            `json:"max_attempts,omitempty"`
	RetryBaseDelay  string            `json:"retry_base_delay,omitempty"`
	RetryMaxDelay   string            `json:"retry_max_delay,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Dependencies    []string          `json:"dependencies,omitempty"`
}

// decodePayload turns the request's declared encoding and payload string into
// raw bytes. An empty encoding with an empty payload is a node with no
// payload, not an error — plenty of graphs carry work entirely in the node ID
// and its kind.
func decodePayload(encoding, payload string) ([]byte, error) {
	if payload == "" {
		return nil, nil
	}
	if encoding != encodingBase64 {
		return nil, fmt.Errorf("%w: payload_encoding %q is not supported, only %q is",
			dagworker.ErrInvalidArgument, encoding, encodingBase64)
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: payload is not valid base64: %v", dagworker.ErrInvalidArgument, err)
	}
	return raw, nil
}

// toNodeSpec builds the domain spec the Manager expects. It is the one place
// the request's loosely-typed strings are turned into the closed enums the
// core validates, so a caller error is reported once, consistently, rather
// than surfacing as a different message from whichever layer happens to
// notice it.
func (req createNodeRequest) toNodeSpec(id dagworker.NodeID) (dagworker.NodeSpec, error) {
	payload, err := decodePayload(req.PayloadEncoding, req.Payload)
	if err != nil {
		return dagworker.NodeSpec{}, err
	}
	trigger, err := triggerFromWire(req.Trigger)
	if err != nil {
		return dagworker.NodeSpec{}, err
	}
	baseDelay, err := parseDuration(req.RetryBaseDelay)
	if err != nil {
		return dagworker.NodeSpec{}, fmt.Errorf("%w: retry_base_delay: %v", dagworker.ErrInvalidArgument, err)
	}
	maxDelay, err := parseDuration(req.RetryMaxDelay)
	if err != nil {
		return dagworker.NodeSpec{}, fmt.Errorf("%w: retry_max_delay: %v", dagworker.ErrInvalidArgument, err)
	}
	deps := make([]dagworker.NodeID, len(req.Dependencies))
	for i, d := range req.Dependencies {
		deps[i] = dagworker.NodeID(d)
	}
	return dagworker.NodeSpec{
		ID:       id,
		Kind:     req.Kind,
		Payload:  payload,
		Labels:   req.Labels,
		Priority: req.Priority,
		Trigger:  trigger,
		Retry: dagworker.RetryPolicy{
			MaxAttempts: req.MaxAttempts,
			BaseDelay:   baseDelay,
			MaxDelay:    maxDelay,
		},
		Deps: deps,
	}, nil
}

// triggerFromWire parses the wire's trigger name. The accepted strings are
// exactly [dagworker.TriggerRule.String]'s outputs, so a value this server
// produces on a GET always round-trips back through a later PUT.
func triggerFromWire(s string) (dagworker.TriggerRule, error) {
	switch s {
	case "", "all_success":
		return dagworker.TriggerAllSuccess, nil
	case "all_done":
		return dagworker.TriggerAllDone, nil
	case "none_failed":
		return dagworker.TriggerNoneFailed, nil
	case "none_failed_min_one_success":
		return dagworker.TriggerNoneFailedMinOneSuccess, nil
	case "always":
		return dagworker.TriggerAlways, nil
	default:
		return 0, fmt.Errorf("%w: unknown trigger %q", dagworker.ErrInvalidArgument, s)
	}
}

// cascadeFromWire parses the ?cascade= query parameter on DELETE node.
func cascadeFromWire(s string) (dagworker.CascadePolicy, error) {
	switch s {
	case "", "reject":
		return dagworker.CascadeReject, nil
	case "detach":
		return dagworker.CascadeDetach, nil
	case "fail":
		return dagworker.CascadeFail, nil
	default:
		return 0, fmt.Errorf("%w: unknown cascade policy %q", dagworker.ErrInvalidArgument, s)
	}
}

// eventKindFromWire parses one entry of the ?event_kinds= filter.
func eventKindFromWire(s string) (dagworker.EventKind, error) {
	switch s {
	case "created":
		return dagworker.EventCreated, nil
	case "transition":
		return dagworker.EventTransition, nil
	case "ready":
		return dagworker.EventReady, nil
	default:
		return 0, fmt.Errorf("%w: unknown event kind %q", dagworker.ErrInvalidArgument, s)
	}
}

// eventKindToWire names an event kind for the SSE "event:" field and the
// NDJSON "kind" member. Distinct from [dagworker.EventKind.String] because
// the wire uses dossier 14's dotted names ("node.status"), which read better
// as an EventSource.addEventListener argument than the Go-side "transition".
func eventKindToWire(k dagworker.EventKind) string {
	switch k {
	case dagworker.EventCreated:
		return "node.created"
	case dagworker.EventTransition:
		return "node.status"
	case dagworker.EventReady:
		return "work.available"
	default:
		return k.String()
	}
}

// parseDuration parses a Go duration string, treating "" as zero rather than
// an error — every duration field on this wire means "use the default" at
// zero, matching config.go's own zero-means-fallback convention.
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// formatDuration is parseDuration's inverse for responses: zero renders as
// the empty string rather than "0s", so a field a caller left unset does not
// come back looking like an explicit choice.
func formatDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

// scopeConfigWire is a [dagworker.ScopeConfig] rendered for the wire.
type scopeConfigWire struct {
	DefaultLeaseTimeout string `json:"default_lease_timeout,omitempty"`
	MinLeaseTimeout     string `json:"min_lease_timeout,omitempty"`
	MaxLeaseTimeout     string `json:"max_lease_timeout,omitempty"`
	MaxAttempts         uint32 `json:"max_attempts,omitempty"`
	RetryBaseDelay      string `json:"retry_base_delay,omitempty"`
	RetryMaxDelay       string `json:"retry_max_delay,omitempty"`
	TerminalRetention   string `json:"terminal_retention,omitempty"`
	MaxSubscriberLag    string `json:"max_subscriber_lag,omitempty"`
	MaxInFlight         uint32 `json:"max_in_flight,omitempty"`
	PayloadCap          int    `json:"payload_cap,omitempty"`
	MaxBatchSize        int    `json:"max_batch_size,omitempty"`
	SweepBatchSize      int    `json:"sweep_batch_size,omitempty"`
	SweepInterval       string `json:"sweep_interval,omitempty"`
	PartitionCount      uint32 `json:"partition_count,omitempty"`
}

func scopeConfigToWire(c dagworker.ScopeConfig) scopeConfigWire {
	return scopeConfigWire{
		DefaultLeaseTimeout: formatDuration(c.DefaultLeaseTimeout),
		MinLeaseTimeout:     formatDuration(c.MinLeaseTimeout),
		MaxLeaseTimeout:     formatDuration(c.MaxLeaseTimeout),
		MaxAttempts:         c.MaxAttempts,
		RetryBaseDelay:      formatDuration(c.RetryBaseDelay),
		RetryMaxDelay:       formatDuration(c.RetryMaxDelay),
		TerminalRetention:   formatDuration(c.TerminalRetention),
		MaxSubscriberLag:    formatDuration(c.MaxSubscriberLag),
		MaxInFlight:         c.MaxInFlight,
		PayloadCap:          c.PayloadCap,
		MaxBatchSize:        c.MaxBatchSize,
		SweepBatchSize:      c.SweepBatchSize,
		SweepInterval:       formatDuration(c.SweepInterval),
		PartitionCount:      c.PartitionCount,
	}
}

func (w scopeConfigWire) toDomain() (dagworker.ScopeConfig, error) {
	var cfg dagworker.ScopeConfig
	var err error
	if cfg.DefaultLeaseTimeout, err = parseDuration(w.DefaultLeaseTimeout); err != nil {
		return cfg, fmt.Errorf("%w: default_lease_timeout: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.MinLeaseTimeout, err = parseDuration(w.MinLeaseTimeout); err != nil {
		return cfg, fmt.Errorf("%w: min_lease_timeout: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.MaxLeaseTimeout, err = parseDuration(w.MaxLeaseTimeout); err != nil {
		return cfg, fmt.Errorf("%w: max_lease_timeout: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.RetryBaseDelay, err = parseDuration(w.RetryBaseDelay); err != nil {
		return cfg, fmt.Errorf("%w: retry_base_delay: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.RetryMaxDelay, err = parseDuration(w.RetryMaxDelay); err != nil {
		return cfg, fmt.Errorf("%w: retry_max_delay: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.TerminalRetention, err = parseDuration(w.TerminalRetention); err != nil {
		return cfg, fmt.Errorf("%w: terminal_retention: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.MaxSubscriberLag, err = parseDuration(w.MaxSubscriberLag); err != nil {
		return cfg, fmt.Errorf("%w: max_subscriber_lag: %v", dagworker.ErrInvalidArgument, err)
	}
	if cfg.SweepInterval, err = parseDuration(w.SweepInterval); err != nil {
		return cfg, fmt.Errorf("%w: sweep_interval: %v", dagworker.ErrInvalidArgument, err)
	}
	cfg.MaxAttempts = w.MaxAttempts
	cfg.MaxInFlight = w.MaxInFlight
	cfg.PayloadCap = w.PayloadCap
	cfg.MaxBatchSize = w.MaxBatchSize
	cfg.SweepBatchSize = w.SweepBatchSize
	cfg.PartitionCount = w.PartitionCount
	return cfg, nil
}

// scopeStatsWire is a [dagworker.ScopeStats] rendered for the wire.
type scopeStatsWire struct {
	Total      uint64 `json:"total"`
	Blocked    uint64 `json:"blocked"`
	Scheduled  uint64 `json:"scheduled"`
	Ready      uint64 `json:"ready"`
	InProgress uint64 `json:"in_progress"`
	Succeeded  uint64 `json:"succeeded"`
	Failed     uint64 `json:"failed"`
	Sealed     bool   `json:"sealed"`
	Complete   bool   `json:"complete"`
	Cursor     uint64 `json:"cursor"`
}

func scopeStatsToWire(s dagworker.ScopeStats) scopeStatsWire {
	return scopeStatsWire{
		Total:      s.Total,
		Blocked:    s.Blocked,
		Scheduled:  s.Scheduled,
		Ready:      s.Ready,
		InProgress: s.InProgress,
		Succeeded:  s.Succeeded,
		Failed:     s.Failed,
		Sealed:     s.Sealed,
		Complete:   s.Complete,
		Cursor:     uint64(s.Cursor),
	}
}

// scopeWire is the GET .../scopes/{scope} response: policy and live counters
// together, since a caller asking "what is this scope" almost always wants
// both in the same round trip.
type scopeWire struct {
	Name   string          `json:"name"`
	Config scopeConfigWire `json:"config"`
	Stats  scopeStatsWire  `json:"stats"`
}

type listScopesResponse struct {
	Scopes []string `json:"scopes"`
}

type listNodesResponse struct {
	Nodes         []nodeWire `json:"nodes"`
	NextPageToken string     `json:"next_page_token,omitempty"`
}

// claimRequest is the POST .../nodes:claim body.
type claimRequest struct {
	WorkerID     string   `json:"worker_id,omitempty"`
	MaxNodes     int      `json:"max_nodes,omitempty"`
	LeaseSeconds int64    `json:"lease_seconds,omitempty"`
	Wait         string   `json:"wait,omitempty"`
	Kinds        []string `json:"kinds,omitempty"`
}

type leaseWire struct {
	LeaseID       string    `json:"lease_id"`
	FencingEpoch  uint64    `json:"fencing_epoch"`
	Node          nodeWire  `json:"node"`
	LeaseDeadline time.Time `json:"lease_deadline"`
}

func leaseToWire(l dagworker.Lease) leaseWire {
	return leaseWire{
		LeaseID:       encodeLeaseID(l.NodeID, l.Epoch),
		FencingEpoch:  l.Epoch,
		Node:          nodeToWire(l.Node),
		LeaseDeadline: l.Deadline,
	}
}

type claimResponse struct {
	Leases []leaseWire `json:"leases"`
}

// completeRequest is the POST .../leases/{lease}:complete body.
type completeRequest struct {
	ResultEncoding string `json:"result_encoding,omitempty"`
	Result         string `json:"result,omitempty"`
}

// failRequest is the POST .../leases/{lease}:fail body.
type failRequest struct {
	Message string `json:"message,omitempty"`
}

// skipRequest is the POST .../leases/{lease}:skip body.
type skipRequest struct {
	Reason string `json:"reason,omitempty"`
}

// renewRequest is the POST .../leases/{lease}:renew body.
type renewRequest struct {
	LeaseSeconds int64 `json:"lease_seconds,omitempty"`
}

type completeResponse struct {
	Node          string           `json:"node"`
	Status        dagworker.Status `json:"status"`
	Reason        dagworker.Reason `json:"reason,omitempty"`
	CompletedAt   time.Time        `json:"completed_at"`
	Retrying      bool             `json:"retrying,omitempty"`
	NextAttemptAt *time.Time       `json:"next_attempt_at,omitempty"`
}

type renewResponse struct {
	LeaseID       string    `json:"lease_id"`
	FencingEpoch  uint64    `json:"fencing_epoch"`
	LeaseDeadline time.Time `json:"lease_deadline"`
}

type leaseInspectResponse struct {
	LeaseID       string    `json:"lease_id"`
	Node          string    `json:"node"`
	FencingEpoch  uint64    `json:"fencing_epoch"`
	Active        bool      `json:"active"`
	LeaseDeadline time.Time `json:"lease_deadline,omitempty"`
}
