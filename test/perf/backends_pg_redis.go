//go:build integration

package perf

import (
	"context"
	"testing"

	dw "github.com/specialistvlad/dagworker"
	postgresstore "github.com/specialistvlad/dagworker/storage/postgres"
	redisstore "github.com/specialistvlad/dagworker/storage/redis"
)

// integrationBackends returns the database-backed stores for the performance
// suite. Their complexity guards use a wider ratio bound than the in-process
// backend's, because a network round trip dominates the measurement and widens
// the spread -- but the shape of the claim is identical: the cost must not grow
// with the size of the graph.
func integrationBackends() []Backend {
	return []Backend{
		{
			Name:      "redis",
			Networked: true,
			New: func(tb testing.TB) dw.Store {
				tb.Helper()
				st, err := redisstore.Open(context.Background(),
					Env("DAGWORKER_REDIS_ADDR", "127.0.0.1:16379"))
				if err != nil {
					tb.Skipf("redis unreachable: %v", err)
				}
				tb.Cleanup(func() { _ = st.Close(context.Background()) })
				return st
			},
		},
		{
			Name:      "postgres",
			Networked: true,
			New: func(tb testing.TB) dw.Store {
				tb.Helper()
				st, err := postgresstore.Open(context.Background(),
					Env("DAGWORKER_POSTGRES_DSN",
						"postgres://dagworker:dagworker@127.0.0.1:15432/dagworker?sslmode=disable"))
				if err != nil {
					tb.Skipf("postgres unreachable: %v", err)
				}
				tb.Cleanup(func() { _ = st.Close(context.Background()) })
				return st
			},
		},
	}
}
