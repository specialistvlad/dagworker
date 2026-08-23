//go:build integration

package perf_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/postgres"
	"github.com/specialistvlad/dagworker/test/perf"
)

// TestScopeStorageCost measures what storing `scope` as `text` costs on
// PostgreSQL, per relation, at a short scope name and a long one.
//
// It exists because the answer is a property of the deployment, not of this
// code: the cost is roughly one byte per character of scope name, per row and
// per index entry, so it is near zero for "deploy" and substantial for a UUID.
// A deployment weighing whether its scope names are costing it anything can run
// this with its own names rather than take a general answer on faith.
//
// Measured here at 50,000 nodes with three edges each, comparing a 6-character
// name against a 36-character one:
//
//	total          46.1 MB -> 70.0 MB   (+51.7%)
//	edges + its two indexes              66.3% of the delta
//	nodes_ready_idx                      16 KB either way
//
// Two things that measurement settled. dagw.edges is indeed where the cost
// concentrates -- its row is ~24 bytes of data, so a long scope name is more
// than half of it, and it carries two indexes. And nodes_ready_idx, the hot
// index one would expect to care most, does not participate at all: it is a
// partial index (WHERE phase = 2), so it only ever holds the ready set rather
// than the graph.
//
// Not part of `make benchmark`: it drops and rebuilds the schema, so it is
// opt-in rather than something a routine run does to your database.
//
// See issue #19.
func TestScopeStorageCost(t *testing.T) {
	if os.Getenv("DAGWORKER_SCOPE_SIZING") == "" {
		t.Skip("set DAGWORKER_SCOPE_SIZING=1")
	}
	n := 50_000
	dsn := os.Getenv("DAGWORKER_POSTGRES_DSN")
	ctx := context.Background()

	cases := []struct {
		label string
		scope dw.Scope
	}{
		{"short  6ch", "deploy"},
		{"long  36ch", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"},
	}

	sizes := make([]map[string]int64, len(cases))
	for i, c := range cases {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pool: %v", err)
		}
		if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS dagw CASCADE`); err != nil {
			t.Fatalf("drop: %v", err)
		}
		st, err := postgres.Open(ctx, dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		perf.SeedFanIn(t, st, c.scope, n)
		if _, err := pool.Exec(ctx, `VACUUM ANALYZE dagw.nodes, dagw.edges, dagw.events`); err != nil {
			t.Fatalf("vacuum: %v", err)
		}
		rows, err := pool.Query(ctx, `
SELECT c.relname, c.relkind, pg_relation_size(c.oid)
FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace
WHERE ns.nspname = 'dagw' AND c.relkind IN ('r','i')
ORDER BY c.relname`)
		if err != nil {
			t.Fatalf("sizes: %v", err)
		}
		m := map[string]int64{}
		for rows.Next() {
			var name, kind string
			var b int64
			if err := rows.Scan(&name, &kind, &b); err != nil {
				t.Fatalf("scan: %v", err)
			}
			m[kind+" "+name] = b
		}
		rows.Close()
		sizes[i] = m
		_ = st.Close(ctx)
		pool.Close()
		t.Logf("seeded %s with %d nodes", c.label, n)
	}

	fmt.Printf("\n=== cost of `scope text`, n=%d nodes, 3 edges/node (issue #19) ===\n", n)
	fmt.Printf("%-34s %12s %12s %12s %10s\n", "relation", "short(6)", "long(36)", "delta", "B/row")
	var totalShort, totalLong int64
	seen := map[string]bool{}
	for k := range sizes[0] {
		seen[k] = true
	}
	for k := range sizes[1] {
		seen[k] = true
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		s, l := sizes[0][k], sizes[1][k]
		totalShort += s
		totalLong += l
		rows := int64(n)
		if len(k) > 7 && k[2:7] == "edges" {
			rows = int64(n) * 3
		}
		fmt.Printf("%-34s %12d %12d %+12d %10.2f\n", k, s, l, l-s, float64(l-s)/float64(rows))
	}
	fmt.Printf("%-34s %12d %12d %+12d\n", "TOTAL", totalShort, totalLong, totalLong-totalShort)
	fmt.Printf("\nper node overall: short %.1f B, long %.1f B, delta %.1f B\n\n",
		float64(totalShort)/float64(n), float64(totalLong)/float64(n),
		float64(totalLong-totalShort)/float64(n))
}
