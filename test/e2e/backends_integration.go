//go:build integration

package e2e

import (
	"context"
	"os"
	"testing"

	dw "github.com/specialistvlad/dagworker"
	postgresstore "github.com/specialistvlad/dagworker/storage/postgres"
	redisstore "github.com/specialistvlad/dagworker/storage/redis"
)

// integrationBackends returns the database-backed stores. They sit behind a
// build tag so the default build of this module does not need the backend
// modules to compile at all.
func integrationBackends() []Backend {
	return []Backend{
		{
			Name:   "redis",
			Shared: true,
			New: func(tb testing.TB) dw.Store {
				tb.Helper()
				st, err := redisstore.Open(context.Background(),
					env("DAGWORKER_REDIS_ADDR", "127.0.0.1:16379"))
				if err != nil {
					tb.Skipf("redis unreachable: %v", err)
				}
				tb.Cleanup(func() { _ = st.Close(context.Background()) })
				return st
			},
		},
		{
			Name:   "postgres",
			Shared: true,
			New: func(tb testing.TB) dw.Store {
				tb.Helper()
				st, err := postgresstore.Open(context.Background(),
					env("DAGWORKER_POSTGRES_DSN",
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

// env reads an environment variable with a fallback, so the suite points at the
// docker compose stack by default and at anything else on request. It lives
// here rather than beside Backends because only the integration build has any
// use for it.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
