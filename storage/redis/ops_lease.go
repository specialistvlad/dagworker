package redis

import (
	"context"
	"fmt"
	"time"

	dw "github.com/specialistvlad/dagworker"
)

// Claim implements [dagworker.Store]. The lease duration is clamped by the
// real [dagworker.ScopeConfig.ClampLease] in Go, in full nanosecond
// precision, before the result is converted to milliseconds and handed to
// the script — never reimplemented in Lua — because ClampLease is a pure
// function of the resolved config and the requested duration, with no
// server-side state to race against.
func (s *Store) Claim(ctx context.Context, req dw.ClaimRequest) (dw.ClaimResult, error) {
	if s.isClosed() {
		return dw.ClaimResult{}, dw.ErrClosed
	}
	if req.Scope == "" {
		return dw.ClaimResult{}, fmt.Errorf("%w: scope must not be empty", dw.ErrInvalidArgument)
	}
	cfg, err := s.resolvedConfig(ctx, req.Scope)
	if err != nil {
		return dw.ClaimResult{}, err
	}
	leaseFor := cfg.ClampLease(req.Timeout)
	max := req.Max
	if max < 1 {
		max = 1
	}

	argv := make([]any, 0, 5+len(req.Kinds))
	argv = append(argv, len(req.Kinds))
	for _, k := range req.Kinds {
		argv = append(argv, k)
	}
	argv = append(argv, max, durToMs(leaseFor), cfg.SweepBatchSize, cfg.MaxInFlight)

	header, effects, err := s.runScript(ctx, s.scripts.claim, req.Scope, argv...)
	if err != nil {
		return dw.ClaimResult{}, err
	}

	res := dw.ClaimResult{Effects: effects}
	for _, row := range header {
		tuple, ok := row.([]any)
		if !ok || len(tuple) != 5 {
			continue
		}
		id := dw.NodeID(toStr(tuple[0]))
		epoch := narrowU64(toInt(tuple[1]))
		deadlineMs := toInt(tuple[2])
		nodeFlat, _ := tuple[3].([]any)
		blobFlat, _ := tuple[4].([]any)
		node := nodeFromHash(req.Scope, id, hgetallMap(nodeFlat), hgetallMap(blobFlat))
		res.Leases = append(res.Leases, dw.Lease{
			Scope:    req.Scope,
			NodeID:   id,
			Epoch:    epoch,
			Deadline: msToTime(deadlineMs),
			Node:     node,
		})
	}
	return res, nil
}

// Complete implements [dagworker.Store].
func (s *Store) Complete(ctx context.Context, req dw.CompleteRequest) (dw.CompleteResult, error) {
	if s.isClosed() {
		return dw.CompleteResult{}, dw.ErrClosed
	}
	if !req.Lease.Valid() {
		return dw.CompleteResult{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	cfg, err := s.resolvedConfig(ctx, req.Lease.Scope)
	if err != nil {
		return dw.CompleteResult{}, err
	}

	success := 0
	if req.Success {
		success = 1
	}
	header, effects, err := s.runScript(ctx, s.scripts.complete, req.Lease.Scope,
		string(req.Lease.NodeID), widenU64(req.Lease.Epoch), success,
		int64(req.Reason), req.Message, req.Result, cfg.PayloadCap)
	if err != nil {
		return dw.CompleteResult{}, err
	}

	var out dw.CompleteResult
	out.Effects = effects
	if len(header) == 2 {
		out.Retrying = toInt(header[0]) == 1
		if ms := toInt(header[1]); ms > 0 {
			out.NextAttemptAt = msToTime(ms)
		}
	}
	return out, nil
}

// Extend implements [dagworker.Store].
func (s *Store) Extend(ctx context.Context, req dw.ExtendRequest) (time.Time, error) {
	if s.isClosed() {
		return time.Time{}, dw.ErrClosed
	}
	if !req.Lease.Valid() {
		return time.Time{}, fmt.Errorf("%w: lease is not well formed", dw.ErrInvalidArgument)
	}
	cfg, err := s.resolvedConfig(ctx, req.Lease.Scope)
	if err != nil {
		return time.Time{}, err
	}
	leaseFor := cfg.ClampLease(req.Timeout)

	header, _, err := s.runScript(ctx, s.scripts.extend, req.Lease.Scope,
		string(req.Lease.NodeID), widenU64(req.Lease.Epoch), durToMs(leaseFor))
	if err != nil {
		return time.Time{}, err
	}
	if len(header) != 1 {
		return time.Time{}, fmt.Errorf("redis: Extend: unexpected reply %#v", header)
	}
	return msToTime(toInt(header[0])), nil
}

// Sweep implements [dagworker.Store].
func (s *Store) Sweep(ctx context.Context, scope dw.Scope, limit int) (dw.SweepResult, error) {
	if s.isClosed() {
		return dw.SweepResult{}, dw.ErrClosed
	}
	if limit <= 0 {
		cfg, err := s.resolvedConfig(ctx, scope)
		if err != nil {
			return dw.SweepResult{}, err
		}
		limit = cfg.SweepBatchSize
	}
	header, effects, err := s.runScript(ctx, s.scripts.sweep, scope, limit)
	if err != nil {
		return dw.SweepResult{}, err
	}
	var res dw.SweepResult
	res.Effects = effects
	if len(header) == 2 {
		res.Reclaimed = int(toInt(header[0]))
		res.More = toInt(header[1]) == 1
	}
	return res, nil
}
