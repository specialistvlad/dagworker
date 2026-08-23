// Package postgres implements the dagworker storage port on top of
// PostgreSQL, using github.com/jackc/pgx/v5. It is the cross-process,
// durable-storage backend: CapDurableStorage and CapCrossProcess are both
// set, because unlike the in-memory reference implementation its whole
// purpose is to let independent processes compete for the same graph.
//
// Claim is implemented as one CTE-chained statement over a partial index on
// the ready set, using SELECT ... FOR UPDATE SKIP LOCKED, so that two
// processes racing for work never observe or lock each other's candidate
// rows (docs/research/04-postgres-backend.md §1, §13, §14.1). Every deadline
// this package writes is computed by PostgreSQL's own clock_timestamp(),
// never by the calling process's clock (ADR-0008): a client-computed
// deadline is never sent to storage, and clock_timestamp() — never now(),
// which freezes at transaction start — is the only time source this
// package's SQL ever reads.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	dw "github.com/specialistvlad/dagworker"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// DefaultPollInterval bounds how long Watch and WaitForWork wait between
// re-reading storage when no LISTEN/NOTIFY wakeup arrives. NOTIFY is a
// latency hint, never the source of truth (dossier 04 §3.4): a dropped
// connection, a missed notification, or a notifier that has not connected
// yet must never turn into a missed event, only into a slower one.
const DefaultPollInterval = 500 * time.Millisecond

// notifyChannel is the single LISTEN/NOTIFY channel every Store instance
// shares. Payloads carry only a scope name — well under NOTIFY's 8000-byte
// ceiling — never node data, so a listener always re-reads the row or the
// events table rather than trusting the payload (dossier 04 §3, point 2).
const notifyChannel = "dagw_events"

// Store is a PostgreSQL-backed implementation of [dagworker.Store].
type Store struct {
	pool     *pgxpool.Pool
	ownPool  bool
	defaults dw.ScopeConfig

	pollInterval time.Duration
	jitter       func(n int64) int64

	notifier *notifier

	migrateOnce sync.Once
	migrateErr  error

	closed    chan struct{}
	closeOnce sync.Once
}

// Option configures a [Store].
type Option interface{ apply(*Store) }

type optionFunc func(*Store)

func (f optionFunc) apply(s *Store) { f(s) }

// WithScopeDefaults sets the [dagworker.ScopeConfig] applied to scopes that
// have no stored configuration of their own — mirrors the in-memory
// backend's option of the same name so a host can swap backends without
// re-deriving its defaults.
func WithScopeDefaults(cfg dw.ScopeConfig) Option {
	return optionFunc(func(s *Store) { s.defaults = cfg })
}

// WithPollInterval overrides [DefaultPollInterval], the upper bound on how
// long Watch and WaitForWork wait for a LISTEN/NOTIFY wakeup before falling
// back to re-reading storage directly.
func WithPollInterval(d time.Duration) Option {
	return optionFunc(func(s *Store) {
		if d > 0 {
			s.pollInterval = d
		}
	})
}

// withJitter replaces the randomness used for retry backoff. Unexported: it
// exists for this package's own conformance test, which needs a
// deterministic schedule the way dagstoretest's memory harness does: tests
// import the harness's FakeClock but this backend cannot accept an injected
// Clock at all (PostgreSQL owns its own clock unconditionally), so a
// deterministic jitter is the only lever conformance timing tests have.
func withJitter(fn func(n int64) int64) Option {
	return optionFunc(func(s *Store) {
		if fn != nil {
			s.jitter = fn
		}
	})
}

// New wraps an existing pool. It never blocks on the network and never
// returns an error, so a host that already owns pool lifecycle management
// can construct a Store synchronously; the schema is applied lazily, once,
// on the first call that touches storage. Open, below, applies it eagerly
// instead, so a bad DSN or missing privilege fails fast at construction
// rather than on a caller's first request.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{
		pool:         pool,
		pollInterval: DefaultPollInterval,
		jitter: func(n int64) int64 {
			if n <= 0 {
				return 0
			}
			return rand.Int64N(n)
		},
		closed: make(chan struct{}),
	}
	for _, o := range opts {
		o.apply(s)
	}
	s.notifier = newNotifier(pool, notifyChannel)
	return s
}

// Open dials a fresh pool from dsn, applies the schema migration, and starts
// the shared LISTEN/NOTIFY connection. The pool is closed by [Store.Close].
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	s := New(pool, opts...)
	s.ownPool = true
	if err := s.ensureMigrated(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	s.notifier.start()
	return s, nil
}

// Capabilities implements [dagworker.CapabilityReporter]. Every optional
// facet this package implements is reported: PostgreSQL is the durable,
// cross-process backend, so both CapDurableStorage and CapCrossProcess are
// set — the two capability bits the in-memory reference implementation
// never sets.
func (s *Store) Capabilities() dw.Capabilities {
	return dw.Capabilities(dw.CapList | dw.CapDurableEvents | dw.CapDoorbell |
		dw.CapCollect | dw.CapDurableStorage | dw.CapCrossProcess)
}

func (s *Store) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// Close implements [dagworker.Store]. It is idempotent and unblocks every
// in-flight Watch and WaitForWork rather than leaving their goroutines
// parked, matching the in-memory backend's shutdown contract exactly.
func (s *Store) Close(context.Context) error {
	s.closeOnce.Do(func() {
		close(s.closed)
		if s.notifier != nil {
			s.notifier.stop()
		}
		if s.ownPool {
			s.pool.Close()
		}
	})
	return nil
}

// ensureMigrated applies the embedded schema exactly once per Store, caching
// the result so a New-constructed store pays the cost on its first real call
// rather than on every one.
func (s *Store) ensureMigrated(ctx context.Context) error {
	s.migrateOnce.Do(func() { s.migrateErr = s.migrate(ctx) })
	return s.migrateErr
}

// migrate applies every embedded migration not yet recorded, in filename
// order, each inside its own transaction. Re-running it is always safe: the
// bootstrap DDL and every migration file are themselves idempotent (CREATE
// ... IF NOT EXISTS throughout), and schema_migrations additionally makes
// each file a no-op the second time it is considered.
func (s *Store) migrate(ctx context.Context) error {
	bootstrap := `
CREATE SCHEMA IF NOT EXISTS dagw;
CREATE TABLE IF NOT EXISTS dagw.schema_migrations (
    version     text PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);`
	if _, err := s.pool.Exec(ctx, bootstrap); err != nil {
		return fmt.Errorf("postgres: bootstrap migration tracking: %w", err)
	}

	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("postgres: read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		if err := s.applyMigration(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, name string) error {
	var applied bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM dagw.schema_migrations WHERE version = $1)`, name,
	).Scan(&applied)
	if err != nil {
		return fmt.Errorf("postgres: check migration %s: %w", name, err)
	}
	if applied {
		return nil
	}

	body, err := migrationFS.ReadFile("migrations/" + name)
	if err != nil {
		return fmt.Errorf("postgres: read migration %s: %w", name, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin migration %s: %w", name, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, string(body)); err != nil {
		return fmt.Errorf("postgres: apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO dagw.schema_migrations (version) VALUES ($1)`, name,
	); err != nil {
		return fmt.Errorf("postgres: record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit migration %s: %w", name, err)
	}
	return nil
}

// Scopes implements [dagworker.Store].
func (s *Store) Scopes(ctx context.Context) ([]dw.Scope, error) {
	if err := s.ensureMigrated(ctx); err != nil {
		return nil, err
	}
	if s.isClosed() {
		return nil, dw.ErrClosed
	}
	rows, err := s.pool.Query(ctx, `SELECT scope FROM dagw.scopes`)
	if err != nil {
		return nil, fmt.Errorf("postgres: Scopes: %w", err)
	}
	defer rows.Close()

	var out []dw.Scope
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("postgres: Scopes: scan: %w", err)
		}
		out = append(out, dw.Scope(name))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: Scopes: %w", err)
	}
	return out, nil
}

var (
	_ dw.Store              = (*Store)(nil)
	_ dw.Lister             = (*Store)(nil)
	_ dw.Doorbell           = (*Store)(nil)
	_ dw.DurableEventStream = (*Store)(nil)
	_ dw.Collector          = (*Store)(nil)
	_ dw.CapabilityReporter = (*Store)(nil)
)
