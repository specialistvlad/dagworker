package redis

import (
	"encoding/json"
	"math"
	"strconv"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// Absolute instants and durations are both stored in whole milliseconds — see
// the nowMs doc comment in lua_prelude.go for why nanoseconds cannot be used
// for the absolute side of that pair. durToMs/msToDur are the one place a
// caller-supplied time.Duration crosses that boundary; every place this
// backend must not reimplement dagworker's own duration math (Resolved,
// ClampLease, Backoff's window) always calls the real function first, in full
// nanosecond precision, and only converts the *result* to milliseconds here.
func durToMs(d time.Duration) int64  { return int64(d / time.Millisecond) }
func msToDur(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// cfgFields are the hash field names of a scope's {scope}:cfg key. sealed
// lives alongside the ScopeConfig fields in the same hash but is never
// touched by SetScopeConfig — only Seal writes it — mirroring the in-memory
// backend keeping "sealed" a separate bool next to, not inside, its cfg.
const (
	fSealed              = "sealed"
	fDefaultLeaseMs      = "defaultLeaseMs"
	fMinLeaseMs          = "minLeaseMs"
	fMaxLeaseMs          = "maxLeaseMs"
	fMaxAttempts         = "maxAttempts"
	fRetryBaseMs         = "retryBaseMs"
	fRetryMaxMs          = "retryMaxMs"
	fTerminalRetentionMs = "terminalRetentionMs"
	fMaxSubscriberLagMs  = "maxSubscriberLagMs"
	fMaxInFlight         = "maxInFlight"
	fPayloadCap          = "payloadCap"
	fMaxBatchSize        = "maxBatchSize"
	fSweepBatchSize      = "sweepBatchSize"
	fSweepIntervalMs     = "sweepIntervalMs"
	fPartitionCount      = "partitionCount"
)

// cfgToHash builds the field/value pairs SetScopeConfig writes. It
// deliberately omits fSealed: sealing is Seal's job alone.
func cfgToHash(cfg dw.ScopeConfig) map[string]any {
	return map[string]any{
		fDefaultLeaseMs:      durToMs(cfg.DefaultLeaseTimeout),
		fMinLeaseMs:          durToMs(cfg.MinLeaseTimeout),
		fMaxLeaseMs:          durToMs(cfg.MaxLeaseTimeout),
		fMaxAttempts:         cfg.MaxAttempts,
		fRetryBaseMs:         durToMs(cfg.RetryBaseDelay),
		fRetryMaxMs:          durToMs(cfg.RetryMaxDelay),
		fTerminalRetentionMs: durToMs(cfg.TerminalRetention),
		fMaxSubscriberLagMs:  durToMs(cfg.MaxSubscriberLag),
		fMaxInFlight:         cfg.MaxInFlight,
		fPayloadCap:          cfg.PayloadCap,
		fMaxBatchSize:        cfg.MaxBatchSize,
		fSweepBatchSize:      cfg.SweepBatchSize,
		fSweepIntervalMs:     durToMs(cfg.SweepInterval),
		fPartitionCount:      cfg.PartitionCount,
	}
}

func hashToCfg(m map[string]string) dw.ScopeConfig {
	return dw.ScopeConfig{
		DefaultLeaseTimeout: msToDur(atoi64(m[fDefaultLeaseMs])),
		MinLeaseTimeout:     msToDur(atoi64(m[fMinLeaseMs])),
		MaxLeaseTimeout:     msToDur(atoi64(m[fMaxLeaseMs])),
		MaxAttempts:         narrowU32(atoi64(m[fMaxAttempts])),
		RetryBaseDelay:      msToDur(atoi64(m[fRetryBaseMs])),
		RetryMaxDelay:       msToDur(atoi64(m[fRetryMaxMs])),
		TerminalRetention:   msToDur(atoi64(m[fTerminalRetentionMs])),
		MaxSubscriberLag:    msToDur(atoi64(m[fMaxSubscriberLagMs])),
		MaxInFlight:         narrowU32(atoi64(m[fMaxInFlight])),
		PayloadCap:          int(atoi64(m[fPayloadCap])),
		MaxBatchSize:        int(atoi64(m[fMaxBatchSize])),
		SweepBatchSize:      int(atoi64(m[fSweepBatchSize])),
		SweepInterval:       msToDur(atoi64(m[fSweepIntervalMs])),
		PartitionCount:      narrowU32(atoi64(m[fPartitionCount])),
	}
}

func atoi64(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func atou64(s string) uint64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// encodeLabels renders labels as a canonical string so specMatches (Lua) can
// compare two encodings byte-for-byte. Go's encoding/json sorts map keys
// alphabetically, which is what makes the encoding canonical; the empty
// string is reserved to mean "no labels" so nil and an empty, non-nil map
// compare equal, matching maps.Equal's own treatment of the two.
func encodeLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	b, _ := json.Marshal(labels)
	return string(b)
}

func decodeLabels(s string) map[string]string {
	if s == "" {
		return nil
	}
	var m map[string]string
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

// decodeEffect turns one row of a script's EFFECTS array — an 11-element
// {id, kind, from, to, reason, message, attempt, nodeKind, seq, cursor, atMs}
// tuple, exactly the field order recordEvent (lua_prelude.go) pushes — back
// into a dagworker.Effect.
func decodeEffect(row []any) dw.Effect {
	return dw.Effect{
		NodeID:   dw.NodeID(toStr(row[0])),
		Kind:     dw.EventKind(narrowU8(toInt(row[1]))),
		From:     dw.Status(narrowU8(toInt(row[2]))),
		To:       dw.Status(narrowU8(toInt(row[3]))),
		Reason:   dw.Reason(narrowU8(toInt(row[4]))),
		Message:  toStr(row[5]),
		Attempt:  narrowU32(toInt(row[6])),
		NodeKind: toStr(row[7]),
		Seq:      dw.Seq(narrowU64(toInt(row[8]))),
		Cursor:   dw.Cursor(narrowU64(toInt(row[9]))),
		At:       msToTime(toInt(row[10])),
	}
}

func decodeEffects(raw any) []dw.Effect {
	rows, ok := raw.([]any)
	if !ok || len(rows) == 0 {
		return nil
	}
	out := make([]dw.Effect, 0, len(rows))
	for _, r := range rows {
		row, ok := r.([]any)
		if !ok {
			continue
		}
		out = append(out, decodeEffect(row))
	}
	return out
}

// toStr and toInt normalize the two shapes go-redis hands back from EVAL:
// bulk strings decode as Go strings, integers decode as int64.
func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	default:
		return ""
	}
}

func toInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

// hgetallMap converts go-redis's flat []any HGETALL reply (as returned
// through EVAL, alternating field, value, field, value, ...) into a map.
func hgetallMap(flat []any) map[string]string {
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[toStr(flat[i])] = toStr(flat[i+1])
	}
	return m
}

// nodeFromHash reconstructs a dagworker.Node from its hot hash and blob hash,
// both decoded via hgetallMap. Payload/Result are left nil when empty,
// matching the in-memory reference's own zero-value convention.
func nodeFromHash(scope dw.Scope, id dw.NodeID, n, b map[string]string) dw.Node {
	node := dw.Node{
		Scope:     scope,
		ID:        id,
		Kind:      n["kind"],
		Status:    dw.Status(narrowU8(atoi64(n["status"]))),
		Reason:    dw.Reason(narrowU8(atoi64(n["reason"]))),
		Message:   n["message"],
		Attempt:   narrowU32(atoi64(n["attempt"])),
		Priority:  narrowI16(atoi64(n["priority"])),
		Trigger:   dw.TriggerRule(narrowU8(atoi64(n["trigger"]))),
		Seq:       dw.Seq(atou64(n["seq"])),
		CreatedAt: msToTime(atoi64(n["createdAt"])),
		UpdatedAt: msToTime(atoi64(n["updatedAt"])),
	}
	node.Retry = dw.RetryPolicy{
		MaxAttempts: narrowU32(atoi64(n["retryMaxAttempts"])),
		BaseDelay:   msToDur(atoi64(n["retryBaseMs"])),
		MaxDelay:    msToDur(atoi64(n["retryMaxMs"])),
	}
	if p := b["payload"]; p != "" {
		node.Payload = []byte(p)
	}
	node.Labels = decodeLabels(b["labels"])
	return node
}

// Redis replies arrive as int64 whatever the field's real width. These narrow
// them safely.
//
// The store wrote every one of these values itself, so in practice they are in
// range by construction. Clamping rather than wrapping matters anyway: if a key
// is ever corrupted, shared with another writer, or migrated from a future
// version, a saturated value is visibly wrong while a wrapped one is a small
// plausible number that silently does the wrong thing -- a MaxAttempts that
// wraps to 2 disables retries and looks deliberate.
func narrowU32(v int64) uint32 {
	switch {
	case v < 0:
		return 0
	case v > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(v)
	}
}

func narrowU8(v int64) uint8 {
	switch {
	case v < 0:
		return 0
	case v > math.MaxUint8:
		return math.MaxUint8
	default:
		return uint8(v)
	}
}

func narrowI16(v int64) int16 {
	switch {
	case v < math.MinInt16:
		return math.MinInt16
	case v > math.MaxInt16:
		return math.MaxInt16
	default:
		return int16(v)
	}
}

func narrowU64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// widenU64 goes the other way, for a value about to be sent to Lua, where a
// number is a float64 and anything above 2^53 is not exactly representable.
func widenU64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
