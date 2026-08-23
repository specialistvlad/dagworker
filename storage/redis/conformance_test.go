package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/dagstoretest"
)

// redisAddr reads the test server's address, defaulting to the port the
// repository's docker-compose.test.yml binds it on.
func redisAddr() string {
	if a := os.Getenv("DAGWORKER_REDIS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:16379"
}

// skipUnlessIntegration gates every test in this file behind
// DAGWORKER_INTEGRATION=1, per the task's testing-strategy convention
// (ADR-0040 / dagstoretest's own harness discipline): a plain `go test ./...`
// must stay fast and Docker-free, never failing on a missing database.
func skipUnlessIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("DAGWORKER_INTEGRATION") == "" {
		t.Skip("set DAGWORKER_INTEGRATION=1 (and DAGWORKER_REDIS_ADDR if not localhost:16379) to run this test")
	}
}

// randomKeyspace returns a short, high-entropy string for withKeyspace.
// dagstoretest.RunConformance hard-codes the scope name "s" for every
// subtest — it is testing scope-relative behaviour, not scope naming — and
// runs every subtest with t.Parallel() against one shared Redis. Isolation
// therefore has to come from the Store instance, not from the scope string;
// this is that isolation.
func randomKeyspace(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("randomKeyspace: %v", err)
	}
	return hex.EncodeToString(b[:]) + "-"
}

// cleanupKeyspace deletes every key this Store's namespace could have
// touched. KEYS is never used on the production write path (every mutating
// operation is the bounded Lua scripts in lua_scripts.go); it is fine here
// because it runs once, after a single test's small, namespaced dataset.
func cleanupKeyspace(t *testing.T, client goredis.UniversalClient, ns string) {
	t.Helper()
	ctx := context.Background()
	keys, err := client.Keys(ctx, "{"+ns+"*").Result()
	if err != nil || len(keys) == 0 {
		return
	}
	_ = client.Del(ctx, keys...).Err()
}

func TestConformance(t *testing.T) {
	skipUnlessIntegration(t)
	t.Parallel()

	addr := redisAddr()
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("connect to redis at %s: %v", addr, err)
	}

	dagstoretest.RunConformance(t, dagstoretest.Harness{
		Name: "redis",
		New: func(t *testing.T) (dw.Store, func(time.Duration)) {
			t.Helper()
			ns := randomKeyspace(t)
			st := New(client, withKeyspace(ns))
			t.Cleanup(func() {
				_ = st.Close(context.Background())
				cleanupKeyspace(t, client, ns)
			})
			// Redis owns its clock (redis.call('TIME') inside every script);
			// there is no advance function to hand back, per the task's own
			// instruction that this harness's advance function is nil.
			return st, nil
		},
	})
}
