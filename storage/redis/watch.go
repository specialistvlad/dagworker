package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	dw "github.com/specialistvlad/dagworker"
)

// blockInterval bounds each XREAD BLOCK call inside pumpEvents. It exists so
// the loop wakes on its own to notice ctx cancellation or store closure even
// if nothing is ever published again; it does not add latency to real
// delivery, because Redis wakes a blocked XREAD the instant a matching XADD
// commits, well inside this window.
const blockInterval = 2 * time.Second

// anyReadyPollInterval is WaitForWork's safety-net poll period. A Doorbell
// wakeup is advisory in the strongest sense (Doorbell doc comment): a missed
// Pub/Sub message — the classic fire-and-forget failure mode — costs one
// poll interval of latency here, never a wrong answer, so a modest periodic
// re-check is cheap insurance against exactly that failure mode rather than
// a correctness requirement.
const anyReadyPollInterval = 2 * time.Second

// Watch implements [dagworker.DurableEventStream]. The guarantee is
// at-least-once within the Stream's retention window (MAXLEN ~20000 per
// scope, applied by every XADD in the Lua prelude): every event appended
// after the given cursor is delivered in cursor order, and a subscriber
// whose cursor has fallen out of that window is told so with
// ErrCursorExpired rather than handed a stream with a silent hole in it.
func (s *Store) Watch(ctx context.Context, req dw.WatchRequest) (<-chan dw.Event, error) {
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	if req.Scope == "" {
		return nil, dw.ErrUnsupported
	}
	s.registerScope(ctx, req.Scope)

	startID, err := s.watchStartID(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make(chan dw.Event)
	go s.pumpEvents(ctx, req.Scope, startID, out)
	return out, nil
}

// watchStartID resolves the three ways WatchRequest can choose a starting
// point (docs comment on WatchRequest in event.go) into an exclusive Stream
// ID XREAD can resume from: entries with ID strictly greater than this are
// delivered. Every entry's own ID is "<cursor>-0" (recordEvent in the Lua
// prelude), so this is the same integer arithmetic as the in-memory
// reference's Cursor handling, expressed as a Stream ID string instead of a
// slice index.
func (s *Store) watchStartID(ctx context.Context, req dw.WatchRequest) (string, error) {
	scope := req.Scope
	switch {
	case req.From > 0:
		oldest, err := s.rdb.XRangeN(ctx, s.keyEvents(scope), "-", "+", 1).Result()
		if err != nil && err != goredis.Nil {
			return "", fmt.Errorf("redis: Watch: %w", err)
		}
		if len(oldest) > 0 {
			if parseStreamCursor(oldest[0].ID) > uint64(req.From)+1 {
				return "", dw.ErrCursorExpired
			}
		} else {
			cur, cerr := s.rdb.Get(ctx, s.keyCursor(scope)).Result()
			if cerr != nil && cerr != goredis.Nil {
				return "", fmt.Errorf("redis: Watch: %w", cerr)
			}
			if atou64(cur) > uint64(req.From) {
				return "", dw.ErrCursorExpired
			}
		}
		return strconv.FormatUint(uint64(req.From), 10) + "-0", nil
	case req.Replay:
		return "0", nil
	default:
		cur, err := s.rdb.Get(ctx, s.keyCursor(scope)).Result()
		if err != nil && err != goredis.Nil {
			return "", fmt.Errorf("redis: Watch: %w", err)
		}
		if cur == "" {
			cur = "0"
		}
		return cur + "-0", nil
	}
}

// pumpEvents blocks on XREAD in a loop, translating each Stream entry back
// into a dagworker.Event and delivering it in order. It closes out when the
// context ends, the store closes, or the connection reports an error other
// than the ordinary "nothing new within this Block window" timeout.
func (s *Store) pumpEvents(ctx context.Context, scope dw.Scope, startID string, out chan<- dw.Event) {
	defer close(out)
	lastID := startID
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		default:
		}

		res, err := s.rdb.XRead(ctx, &goredis.XReadArgs{
			Streams: []string{s.keyEvents(scope), lastID},
			Block:   blockInterval,
			Count:   256,
		}).Result()
		if err != nil {
			if err == goredis.Nil {
				continue // ordinary block timeout: loop, re-check ctx/closed above
			}
			return // context canceled, connection lost, or store closing
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				select {
				case out <- decodeStreamEvent(scope, msg):
					lastID = msg.ID
				case <-ctx.Done():
					return
				case <-s.closed:
					return
				}
			}
		}
	}
}

func decodeStreamEvent(scope dw.Scope, msg goredis.XMessage) dw.Event {
	v := msg.Values
	return dw.Event{
		Kind:     dw.EventKind(atoi64(valStr(v["kind"]))),
		Scope:    scope,
		NodeID:   dw.NodeID(valStr(v["id"])),
		Seq:      dw.Seq(atou64(valStr(v["seq"]))),
		Cursor:   dw.Cursor(parseStreamCursor(msg.ID)),
		From:     dw.Status(atoi64(valStr(v["from"]))),
		To:       dw.Status(atoi64(valStr(v["to"]))),
		Reason:   dw.Reason(atoi64(valStr(v["reason"]))),
		Message:  valStr(v["message"]),
		Attempt:  uint32(atoi64(valStr(v["attempt"]))),
		NodeKind: valStr(v["nodeKind"]),
		At:       msToTime(atoi64(valStr(v["at"]))),
	}
}

func valStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// parseStreamCursor extracts the cursor integer from a "<cursor>-0" Stream
// entry ID.
func parseStreamCursor(id string) uint64 {
	ms, _, _ := strings.Cut(id, "-")
	n, _ := strconv.ParseUint(ms, 10, 64)
	return n
}

// WaitForWork implements [dagworker.Doorbell]. It checks whether work is
// already available before subscribing at all (otherwise a node that became
// ready between the caller's last failed claim and this call would be
// missed until the next publish), then waits on the scope's Pub/Sub bell —
// rung by makeReady in the Lua prelude every time a node becomes claimable —
// falling back to a periodic re-check as insurance against a dropped
// message, exactly as the Doorbell doc comment's own reasoning allows.
func (s *Store) WaitForWork(ctx context.Context, scope dw.Scope, kinds []string) error {
	if s.isClosed() {
		return dw.ErrClosed
	}
	ready, err := s.anyReady(ctx, scope, kinds)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	sub := s.rdb.Subscribe(ctx, s.keyBell(scope))
	defer func() { _ = sub.Close() }()
	ch := sub.Channel()

	ticker := time.NewTicker(anyReadyPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ch:
			return nil
		case <-ticker.C:
			if ready, err := s.anyReady(ctx, scope, kinds); err == nil && ready {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			return dw.ErrClosed
		}
	}
}

func (s *Store) anyReady(ctx context.Context, scope dw.Scope, kinds []string) (bool, error) {
	if len(kinds) == 0 {
		v, err := s.rdb.HGet(ctx, s.keyStats(scope), "Ready").Result()
		if err != nil && err != goredis.Nil {
			return false, fmt.Errorf("redis: WaitForWork: %w", err)
		}
		return atoi64(v) > 0, nil
	}
	for _, k := range kinds {
		n, err := s.rdb.ZCard(ctx, s.keyReady(scope, k)).Result()
		if err != nil && err != goredis.Nil {
			return false, fmt.Errorf("redis: WaitForWork: %w", err)
		}
		if n > 0 {
			return true, nil
		}
	}
	return false, nil
}
