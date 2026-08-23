package postgres_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
	"github.com/specialistvlad/dagworker/storage/postgres"
)

// defaultDSN matches test/e2e/docker-compose.test.yml. DAGWORKER_POSTGRES_DSN
// overrides it for a developer or CI runner pointed at a different instance.
const defaultDSN = "postgres://dagworker:dagworker@127.0.0.1:15432/dagworker?sslmode=disable"

func adminDSN() string {
	if v := os.Getenv("DAGWORKER_POSTGRES_DSN"); v != "" {
		return v
	}
	return defaultDSN
}

// requireIntegration skips the test unless DAGWORKER_INTEGRATION=1, so a
// plain `go test ./...` with no live database configured never fails on a
// connection error — it simply never attempts one.
func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DAGWORKER_INTEGRATION") == "" {
		t.Skip("set DAGWORKER_INTEGRATION=1 to run against a live PostgreSQL instance (see test/e2e/docker-compose.test.yml)")
	}
}

// dsnForDatabase rewrites adminDSN()'s path to point at a different database
// name on the same server, so a freshly created scratch database can be
// dialed with the same host, port, and credentials.
func dsnForDatabase(name string) (string, error) {
	u, err := url.Parse(adminDSN())
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}

// newScratchDatabase creates a throwaway database on the same server the
// suite's admin DSN points at, isolating one test's rows from every other
// test's so subtests can run in parallel without a shared-schema race, and
// returns a DSN for it plus a cleanup that drops it. WITH (FORCE) reaps any
// connection this test's own Store has not yet released, so a slow Close
// never leaves an orphaned database behind.
func newScratchDatabase(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, adminDSN())
	if err != nil {
		t.Fatalf("connect to admin database: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	name := fmt.Sprintf("dagworker_test_%d_%d", os.Getpid(), rand.Int64())
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+quoteIdent(name)); err != nil {
		t.Fatalf("create scratch database %s: %v", name, err)
	}
	t.Cleanup(func() {
		dctx := context.Background()
		conn, err := pgx.Connect(dctx, adminDSN())
		if err != nil {
			t.Logf("cleanup: connect to admin database: %v", err)
			return
		}
		defer func() { _ = conn.Close(dctx) }()
		if _, err := conn.Exec(dctx, `DROP DATABASE IF EXISTS `+quoteIdent(name)+` WITH (FORCE)`); err != nil {
			t.Logf("cleanup: drop scratch database %s: %v", name, err)
		}
	})

	dsn, err := dsnForDatabase(name)
	if err != nil {
		t.Fatalf("build scratch DSN: %v", err)
	}
	return dsn
}

// quoteIdent is a minimal identifier quoter sufficient for the names this
// file itself generates (an ASCII prefix plus digits): it exists so a
// database name never has to be threaded through as a bind parameter, which
// CREATE DATABASE and DROP DATABASE do not accept.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func newHarnessStore(t *testing.T) dw.Store {
	t.Helper()
	dsn := newScratchDatabase(t)
	st, err := postgres.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close(context.Background()) })
	return st
}

// TestConformance runs the shared storage-backend suite. The advance
// function is nil: PostgreSQL owns its clock unconditionally (ADR-0008) and
// nothing in this backend can be driven, so the suite falls back to real,
// short sleeps for the handful of tests that need time to pass.
func TestConformance(t *testing.T) {
	requireIntegration(t)
	t.Parallel()
	dagstoretest.RunConformance(t, dagstoretest.Harness{
		Name: "postgres",
		New: func(t *testing.T) (dw.Store, func(time.Duration)) {
			return newHarnessStore(t), nil
		},
	})
}

// TestCrossProcessClaimIsExclusive is the backend's whole reason to exist:
// two independent *Store values, each with its own pool and its own
// connections, competing for the same rows in the same database — exactly
// the shape of two separate OS processes, which this test simulates without
// actually needing two binaries. SKIP LOCKED is what has to make this true.
func TestCrossProcessClaimIsExclusive(t *testing.T) {
	requireIntegration(t)
	t.Parallel()

	ctx := context.Background()
	dsn := newScratchDatabase(t)

	storeA, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close(ctx) })

	storeB, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close(ctx) })

	const scope = dw.Scope("cross-process")
	const n = 200

	specs := make([]dw.NodeSpec, n)
	for i := range specs {
		specs[i] = dw.NodeSpec{ID: dw.NodeID(fmt.Sprintf("n%04d", i))}
	}
	if _, err := storeA.AddNodes(ctx, scope, specs); err != nil {
		t.Fatalf("AddNodes: %v", err)
	}

	var (
		mu   sync.Mutex
		seen = make(map[dw.NodeID]int, n)
		wg   sync.WaitGroup
	)
	racer := func(st dw.Store, workerID string) {
		defer wg.Done()
		for {
			res, err := st.Claim(ctx, dw.ClaimRequest{
				Scope: scope, Max: 5, Timeout: 30 * time.Second, WorkerID: workerID,
			})
			if err != nil {
				t.Errorf("Claim (%s): %v", workerID, err)
				return
			}
			if len(res.Leases) == 0 {
				return
			}
			mu.Lock()
			for _, l := range res.Leases {
				seen[l.NodeID]++
			}
			mu.Unlock()
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(2)
		go racer(storeA, fmt.Sprintf("A%d", i))
		go racer(storeB, fmt.Sprintf("B%d", i))
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("claimed %d distinct nodes across two stores, want %d", len(seen), n)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("node %q was granted %d times across two independent stores sharing one database", id, count)
		}
	}
}
