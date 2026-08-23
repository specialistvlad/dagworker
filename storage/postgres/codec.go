package postgres

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	dw "github.com/specialistvlad/dagworker"
)

// querier is the subset of pgxpool.Pool and pgx.Tx this package calls
// through. Reads that are equally correct inside or outside a transaction —
// GetNode, ListNodes, ScopeStats — accept it directly so the same code path
// serves both; mutations always take a pgx.Tx explicitly at their call site,
// so the atomicity boundary is visible in every mutating method's signature
// rather than implicit in which querier happened to be passed.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// nodeColumnNames is the exact, ordered column list every SELECT and
// RETURNING in this package uses, so scanNode is the single place that
// column order and struct field order must agree. It is a slice, not a bare
// string constant, so a joined query can prefix every name with a table
// alias (nodeColumnsAliased) without hand-duplicating the list.
var nodeColumnNames = []string{
	"id", "rank", "scope", "node_id", "kind", "status", "reason", "message", "phase",
	"attempt", "epoch", "priority", "trigger_rule", "retry_max_attempts", "retry_base_delay_ns", "retry_max_delay_ns",
	"payload", "result", "labels", "worker", "seq", "fifo",
	"deps_unsatisfied", "deps_succeeded", "deps_skipped", "deps_failed",
	"deadline", "ready_at", "created_at", "updated_at",
}

// nodeColumns is nodeColumnNames joined for a plain, unaliased SELECT.
var nodeColumns = strings.Join(nodeColumnNames, ", ")

// nodeColumnsAliased is nodeColumnNames joined with every name prefixed by
// alias — for a SELECT that joins dagw.nodes against dagw.edges and must
// disambiguate which relation's columns it wants.
func nodeColumnsAliased(alias string) string {
	out := make([]string, len(nodeColumnNames))
	for i, c := range nodeColumnNames {
		out[i] = alias + "." + c
	}
	return strings.Join(out, ", ")
}

// nodeRow is one row of dagw.nodes, decoded into Go-native types. It carries
// both the public [dagworker.Node] fields and the internal scheduling state
// (phase, rank, fifo, dep counters) that only this package and [Store.Inspect]
// ever see — the same split [memory]'s nodeRec makes, for the same reason:
// a snapshot handed to a caller and the state a scheduling decision needs are
// different shapes.
type nodeRow struct {
	ID       int64
	Rank     int64
	Scope    string
	NodeID   string
	Kind     string
	Status   dw.Status
	Reason   dw.Reason
	Message  string
	Phase    dw.Phase
	Attempt  uint32
	Epoch    uint64
	Priority int16
	Trigger  dw.TriggerRule

	RetryMaxAttempts uint32
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration

	Payload []byte
	Result  []byte
	Labels  map[string]string
	Worker  string

	Seq  dw.Seq
	Fifo int64

	Deps dw.DepCounts

	Deadline *time.Time
	ReadyAt  *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// scanRow is the minimal interface both pgx.Row (QueryRow) and pgx.Rows
// (Query, mid-iteration) satisfy, so scanNode serves either call shape.
type scanRow interface {
	Scan(dest ...any) error
}

// scanNode decodes one row shaped like nodeColumns. Every enum-shaped column
// is scanned into a plain sized integer first and cast afterward: pgx's
// generic scan path does not know about dagworker's named integer types, and
// scanning into an intermediate of the exact wire width keeps the cast a
// compile-time-checked truncation rather than a silent one.
func scanNode(row scanRow) (nodeRow, error) {
	var (
		n                                       nodeRow
		status, reason, phase, trig             int16
		attempt                                 int32
		epoch                                   int64
		retryMaxAttempts                        int32
		retryBaseNS, retryMaxNS                 int64
		depsUnsat, depsSucc, depsSkip, depsFail int32
		labelsRaw                               []byte
	)
	err := row.Scan(
		&n.ID, &n.Rank, &n.Scope, &n.NodeID, &n.Kind, &status, &reason, &n.Message, &phase,
		&attempt, &epoch, &n.Priority, &trig, &retryMaxAttempts, &retryBaseNS, &retryMaxNS,
		&n.Payload, &n.Result, &labelsRaw, &n.Worker, &n.Seq, &n.Fifo,
		&depsUnsat, &depsSucc, &depsSkip, &depsFail,
		&n.Deadline, &n.ReadyAt, &n.CreatedAt, &n.UpdatedAt,
	)
	if err != nil {
		return nodeRow{}, err
	}
	n.Status = dw.Status(narrowU8(status))
	n.Reason = dw.Reason(narrowU8(reason))
	n.Phase = dw.Phase(narrowU8(phase))
	n.Trigger = dw.TriggerRule(narrowU8(trig))
	n.Attempt = narrowU32(attempt)
	n.Epoch = narrowU64(epoch)
	n.RetryMaxAttempts = narrowU32(retryMaxAttempts)
	n.RetryBaseDelay = time.Duration(retryBaseNS)
	n.RetryMaxDelay = time.Duration(retryMaxNS)
	n.Deps = dw.DepCounts{
		Unsatisfied: narrowU32(depsUnsat),
		Succeeded:   narrowU32(depsSucc),
		Skipped:     narrowU32(depsSkip),
		Failed:      narrowU32(depsFail),
	}
	if len(labelsRaw) > 0 {
		if err := json.Unmarshal(labelsRaw, &n.Labels); err != nil {
			return nodeRow{}, err
		}
	}
	return n, nil
}

// snapshot converts a nodeRow into the public, immutable [dagworker.Node]
// value a caller receives. Payload and Labels are already independent copies
// (pgx allocates a fresh []byte per scan and json.Unmarshal a fresh map), so
// no further defensive copy is needed the way memory's snapshot needs one.
func (n nodeRow) snapshot() dw.Node {
	return dw.Node{
		Scope:    dw.Scope(n.Scope),
		ID:       dw.NodeID(n.NodeID),
		Kind:     n.Kind,
		Status:   n.Status,
		Reason:   n.Reason,
		Message:  n.Message,
		Attempt:  n.Attempt,
		Priority: n.Priority,
		Trigger:  n.Trigger,
		Retry: dw.RetryPolicy{
			MaxAttempts: n.RetryMaxAttempts,
			BaseDelay:   n.RetryBaseDelay,
			MaxDelay:    n.RetryMaxDelay,
		},
		Payload:   n.Payload,
		Labels:    n.Labels,
		Seq:       n.Seq,
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

// retryEffective folds this node's own [dagworker.RetryPolicy] over the
// scope's resolved defaults, field by field — the exact rule
// memory's effectiveRetry implements, reproduced here so the two backends
// cannot drift on how a per-node override composes with the scope policy.
func (n nodeRow) retryEffective(cfg dw.ScopeConfig) (attempts uint32, base, maxDelay time.Duration) {
	attempts = cfg.MaxAttempts
	if n.RetryMaxAttempts > 0 {
		attempts = n.RetryMaxAttempts
	}
	base, maxDelay = cfg.RetryBaseDelay, cfg.RetryMaxDelay
	if n.RetryBaseDelay > 0 {
		base = n.RetryBaseDelay
	}
	if n.RetryMaxDelay > 0 {
		maxDelay = n.RetryMaxDelay
	}
	return attempts, base, maxDelay
}

// labelsJSON marshals a label map for storage, returning nil for an empty
// map so the column reads back as SQL NULL rather than the literal string
// "{}" — nodeRow.Labels then decodes back to a nil map, matching
// [dagworker.Node]'s documented zero meaning.
func labelsJSON(labels map[string]string) ([]byte, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	return json.Marshal(labels)
}

// scopeRow is one row of dagw.scopes, decoded into
// [dagworker.ScopeConfig] plus the sealed flag and incremental counters.
type scopeRow struct {
	Scope  string
	Sealed bool
	Cfg    dw.ScopeConfig
	Stats  dw.ScopeStats
}

const scopeColumns = `scope, sealed,
	default_lease_timeout_ns, min_lease_timeout_ns, max_lease_timeout_ns,
	max_attempts, retry_base_delay_ns, retry_max_delay_ns,
	terminal_retention_ns, max_subscriber_lag_ns, max_in_flight,
	payload_cap, max_batch_size, sweep_batch_size, sweep_interval_ns, partition_count,
	stat_total, stat_blocked, stat_scheduled, stat_ready, stat_in_progress, stat_succeeded, stat_failed`

func scanScope(row scanRow) (scopeRow, error) {
	var (
		s                                                      scopeRow
		defLeaseNS, minLeaseNS, maxLeaseNS                     int64
		maxAttempts                                            int32
		retryBaseNS, retryMaxNS                                int64
		retentionNS, subLagNS                                  int64
		maxInFlight, payloadCap, maxBatch, sweepBatch, partCnt int32
		sweepIntervalNS                                        int64
	)
	err := row.Scan(
		&s.Scope, &s.Sealed,
		&defLeaseNS, &minLeaseNS, &maxLeaseNS,
		&maxAttempts, &retryBaseNS, &retryMaxNS,
		&retentionNS, &subLagNS, &maxInFlight,
		&payloadCap, &maxBatch, &sweepBatch, &sweepIntervalNS, &partCnt,
		&s.Stats.Total, &s.Stats.Blocked, &s.Stats.Scheduled, &s.Stats.Ready,
		&s.Stats.InProgress, &s.Stats.Succeeded, &s.Stats.Failed,
	)
	if err != nil {
		return scopeRow{}, err
	}
	s.Cfg = dw.ScopeConfig{
		DefaultLeaseTimeout: time.Duration(defLeaseNS),
		MinLeaseTimeout:     time.Duration(minLeaseNS),
		MaxLeaseTimeout:     time.Duration(maxLeaseNS),
		MaxAttempts:         narrowU32(maxAttempts),
		RetryBaseDelay:      time.Duration(retryBaseNS),
		RetryMaxDelay:       time.Duration(retryMaxNS),
		TerminalRetention:   time.Duration(retentionNS),
		MaxSubscriberLag:    time.Duration(subLagNS),
		MaxInFlight:         narrowU32(maxInFlight),
		PayloadCap:          int(payloadCap),
		MaxBatchSize:        int(maxBatch),
		SweepBatchSize:      int(sweepBatch),
		SweepInterval:       time.Duration(sweepIntervalNS),
		PartitionCount:      narrowU32(partCnt),
	}
	s.Stats.Sealed = s.Sealed
	return s, nil
}

// PostgreSQL's integer types are wider than the domain types they carry, so
// every read narrows. These narrow safely.
//
// The store wrote all of these itself and the schema constrains them, so in
// practice they are in range. Saturating rather than wrapping still matters: a
// wrapped value is a small plausible number that quietly does the wrong thing,
// while a saturated one is visibly wrong. A MaxAttempts that wraps to 2
// disables retries and looks deliberate.
func narrowU8[T ~int16 | ~int32 | ~int64](v T) uint8 {
	switch {
	case v < 0:
		return 0
	case int64(v) > math.MaxUint8:
		return math.MaxUint8
	default:
		return uint8(v) //nolint:gosec // bounded by the cases above
	}
}

func narrowU32[T ~int32 | ~int64 | ~int](v T) uint32 {
	switch {
	case v < 0:
		return 0
	case int64(v) > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(v) //nolint:gosec // bounded by the cases above
	}
}

func narrowU64[T ~int32 | ~int64 | ~int | ~uint32](v T) uint64 {
	if int64(v) < 0 {
		return 0
	}
	return uint64(v) //nolint:gosec // bounded by the check above
}

// widenI64 goes the other way, for a value on its way into a bigint column.
func widenI64[T ~uint64 | ~uint32](v T) int64 {
	if uint64(v) > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v) //nolint:gosec // bounded by the check above
}

// narrowI32 fits a value into an integer column.
func narrowI32[T ~uint32 | ~uint64 | ~int | ~int64 | ~int32](v T) int32 {
	switch {
	case int64(v) < math.MinInt32:
		return math.MinInt32
	case int64(v) > math.MaxInt32:
		return math.MaxInt32
	default:
		return int32(v) //nolint:gosec // bounded by the cases above
	}
}
