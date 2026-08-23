package file

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// Store is the durable, server-less backend: the graph in memory, every
// mutation appended to an fsynced log, replayed on start.
//
// It is one process at a time. The log is a durability mechanism, not a
// coordination protocol, so [Store.Capabilities] sets CapDurableStorage and
// deliberately not CapCrossProcess -- pointing two processes at one directory
// would have them replay each other's history and then diverge in silence,
// which is the failure ADR-0016's capability honesty rule exists to prevent.
type Store struct {
	dir  string
	mem  *memory.Store
	log  *log
	gate *gate

	// mu serialises mutations. The log is a single append-ordered file and a
	// record's readings are meaningless out of order, so the recorder is
	// single-writer by construction rather than by accident.
	mu     sync.Mutex
	closed bool
}

// Option configures a [Store].
type Option interface{ apply(*config) }

type config struct {
	clock    dw.Clock
	jitter   func(int64) int64
	defaults dw.ScopeConfig
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// WithClock replaces the clock. The backend owns the clock for every deadline
// comparison (ADR-0008), and the log records what it returned, so an injected
// clock is recorded and replayed like any other.
func WithClock(c dw.Clock) Option { //nolint:ireturn // ADR-0027's opaque functional-option shape, as in memory.Option
	return optionFunc(func(cfg *config) {
		if c != nil {
			cfg.clock = c
		}
	})
}

// WithScopeDefaults sets the configuration a scope has before SetScopeConfig.
func WithScopeDefaults(sc dw.ScopeConfig) Option { //nolint:ireturn // ADR-0027's opaque functional-option shape, as in memory.Option
	return optionFunc(func(cfg *config) { cfg.defaults = sc })
}

// Open loads the store at dir, creating it if absent, and replays whatever the
// last process left behind.
//
// A torn trailing record -- the ordinary result of a crash mid-append -- is
// detected by its checksum, truncated, and reported through the returned
// Recovery. Everything before it is intact and is replayed.
func Open(ctx context.Context, dir string, opts ...Option) (*Store, *Recovery, error) {
	cfg := config{clock: dw.SystemClock{}, jitter: defaultJitter}
	for _, o := range opts {
		if o == nil {
			return nil, nil, fmt.Errorf("%w: nil option", dw.ErrInvalidArgument)
		}
		o.apply(&cfg)
	}
	if dir == "" {
		return nil, nil, fmt.Errorf("%w: directory must not be empty", dw.ErrInvalidArgument)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("file: creating %s: %w", dir, err)
	}
	if err := syncDir(dir); err != nil {
		return nil, nil, err
	}

	lg, err := openLog(logPath(dir))
	if err != nil {
		return nil, nil, err
	}
	records, discarded, err := lg.readAll()
	if err != nil {
		_ = lg.close()
		return nil, nil, err
	}

	mem, g, err := replay(ctx, records, cfg)
	if err != nil {
		_ = lg.close()
		return nil, nil, err
	}

	return &Store{dir: dir, mem: mem, log: lg, gate: g},
		&Recovery{Records: len(records), DiscardedBytes: discarded}, nil
}

// Recovery reports what Open found on disk. A non-zero DiscardedBytes is the
// expected shape of a crash mid-append, not an error -- but it is reported
// rather than swallowed, because a host that sees it repeatedly is being told
// something about its shutdown path.
type Recovery struct {
	Records        int
	DiscardedBytes int64
}

// replay rebuilds the in-memory state by re-running each recorded command
// against the readings it originally consumed, then hands the same store over
// to the live clock.
//
// One store, not two: the backend takes its clock at construction, so the state
// replay produces has to stay in the store that produced it. The gate changes
// mode instead.
func replay(ctx context.Context, records []record, cfg config) (*memory.Store, *gate, error) {
	g := newGate(cfg.clock, cfg.jitter)
	mem := memory.New(
		memory.WithClock(g),
		memory.WithJitter(g.Jitter),
		memory.WithScopeDefaults(cfg.defaults),
	)
	for i, r := range records {
		g.feed(r.Readings)
		if err := apply(ctx, mem, r); err != nil {
			_ = mem.Close(ctx)
			return nil, nil, fmt.Errorf("file: replaying record %d (op %d, scope %q): %w",
				i, r.Op, r.Scope, err)
		}
		if g.drifted() {
			_ = mem.Close(ctx)
			return nil, nil, fmt.Errorf(
				"file: record %d (op %d) consumed more readings than the log recorded: "+
					"the log was written by a build whose in-memory backend reads the clock "+
					"differently, and replaying it would produce a state that looks plausible "+
					"and is wrong", i, r.Op)
		}
	}
	g.golive()
	return mem, g, nil
}

// apply re-runs one recorded command. Split in two along the same line the
// storage port is: mutations that change the graph's shape, and mutations that
// move a node through the lease protocol.
func apply(ctx context.Context, m *memory.Store, r record) error {
	if done, err := applyGraph(ctx, m, r); done {
		return err
	}
	return applyLease(ctx, m, r)
}

// applyGraph handles the shape-changing commands, reporting whether it
// recognised the op at all.
func applyGraph(ctx context.Context, m *memory.Store, r record) (bool, error) {
	switch r.Op {
	case opSetScopeConfig:
		if r.Config == nil {
			return true, fmt.Errorf("%w: SetScopeConfig record with no config", dw.ErrInvalidArgument)
		}
		return true, m.SetScopeConfig(ctx, dw.Scope(r.Scope), *r.Config)
	case opSeal:
		return true, m.Seal(ctx, dw.Scope(r.Scope))
	case opAddNodes:
		_, err := m.AddNodes(ctx, dw.Scope(r.Scope), r.Specs)
		return true, err
	case opAddEdges:
		_, err := m.AddEdges(ctx, dw.Scope(r.Scope), r.Edges)
		return true, err
	case opRemoveEdges:
		_, err := m.RemoveEdges(ctx, dw.Scope(r.Scope), r.Edges)
		return true, err
	case opRemoveNode:
		_, err := m.RemoveNode(ctx, dw.Scope(r.Scope), dw.NodeID(r.NodeID), r.Policy)
		return true, err
	case opCancel:
		ids := make([]dw.NodeID, len(r.IDs))
		for i, s := range r.IDs {
			ids[i] = dw.NodeID(s)
		}
		_, err := m.Cancel(ctx, dw.Scope(r.Scope), ids)
		return true, err
	case opCancelScope:
		_, err := m.CancelScope(ctx, dw.Scope(r.Scope))
		return true, err
	case opClaim, opComplete, opExtend, opSweep:
		// The lease protocol's half. Named rather than left to the default so
		// that adding an op forces a decision here as well as there.
		return false, nil
	default:
		return false, nil
	}
}

// applyLease handles the commands that move a node through the lease protocol.
func applyLease(ctx context.Context, m *memory.Store, r record) error {
	switch r.Op {
	case opClaim:
		if r.Claim == nil {
			return fmt.Errorf("%w: Claim record with no request", dw.ErrInvalidArgument)
		}
		_, err := m.Claim(ctx, *r.Claim)
		return err
	case opComplete:
		if r.Done == nil {
			return fmt.Errorf("%w: Complete record with no request", dw.ErrInvalidArgument)
		}
		_, err := m.Complete(ctx, *r.Done)
		return err
	case opExtend:
		if r.Extend == nil {
			return fmt.Errorf("%w: Extend record with no request", dw.ErrInvalidArgument)
		}
		_, err := m.Extend(ctx, *r.Extend)
		return err
	case opSweep:
		_, err := m.Sweep(ctx, dw.Scope(r.Scope), r.Limit)
		return err
	case opSetScopeConfig, opSeal, opAddNodes, opAddEdges, opRemoveEdges, opRemoveNode,
		opCancel, opCancelScope:
		// applyGraph claimed these already; arriving here is a routing bug in
		// apply rather than anything the log did.
		return fmt.Errorf("%w: op %d was routed to the lease half", dw.ErrInvalidArgument, r.Op)
	default:
		return fmt.Errorf("%w: unknown log op %d", dw.ErrInvalidArgument, r.Op)
	}
}

// defaultJitter draws uniformly from [0, n). Not a security decision: it only
// decorrelates retry backoff across a fleet, the same stance the in-memory
// backend takes.
func defaultJitter(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return rand.Int64N(n) //nolint:gosec // scheduling jitter, not a secret
}
