package main

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
	"github.com/specialistvlad/dagworker/storage/postgres"
	"github.com/specialistvlad/dagworker/storage/redis"
)

// storeHandle bundles a [dagworker.Store] with whatever extra resource
// dagworkerd opened alongside it and must therefore close itself.
//
// [redis.New] documents that "the caller retains ownership" of a client
// handed to it — its own Close only closes what it dialed via [redis.Open],
// which has no parameter for the password this daemon reads from a secret
// file. So when dagworkerd builds its own authenticated Redis client, extra
// records how to close it; every other backend leaves extra nil because the
// Store it returns already owns everything it needs to close.
type storeHandle struct {
	store dagworker.Store
	extra func() error
}

// Close closes the store and then, if this daemon opened a resource the
// store does not own, that resource too — in that order, so a partially
// closed client is never asked to serve a store call that outlives it.
func (h storeHandle) Close(ctx context.Context) error {
	err := h.store.Close(ctx)
	if h.extra != nil {
		if extraErr := h.extra(); extraErr != nil && err == nil {
			err = extraErr
		}
	}
	return err
}

// openStore constructs the backend cfg.Store names. dagworkerd is the one
// module allowed to import every backend (doc.go), so backend selection
// lives here and nowhere else — a storage package never learns which of its
// siblings a given deployment chose.
func openStore(ctx context.Context, cfg Config) (storeHandle, error) {
	switch cfg.Store {
	case storeMemory, "":
		return storeHandle{store: memory.New()}, nil
	case storeRedis:
		return openRedisStore(ctx, cfg)
	case storePostgres:
		return openPostgresStore(ctx, cfg)
	default:
		// Unreachable once Config.validate has run, but openStore takes a
		// Config directly rather than a pre-validated one, so it stays
		// correct even if a future caller skips validation.
		return storeHandle{}, fmt.Errorf("dagworkerd: unknown store %q", cfg.Store)
	}
}

// openRedisStore dials Redis directly, rather than through [redis.Open],
// specifically to pass the password read from cfg.RedisPasswordFile —
// [redis.Open] hard-codes a bare address with no credential field, and this
// package may not add one without modifying another module.
func openRedisStore(ctx context.Context, cfg Config) (storeHandle, error) {
	password, err := readSecretFile(cfg.RedisPasswordFile)
	if err != nil {
		return storeHandle{}, err
	}

	client := goredis.NewClient(&goredis.Options{Addr: cfg.RedisAddr, Password: password})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return storeHandle{}, fmt.Errorf("dagworkerd: connecting to redis at %s: %w", cfg.RedisAddr, err)
	}

	return storeHandle{store: redis.New(client), extra: client.Close}, nil
}

// openPostgresStore reads the DSN from cfg.PostgresDSNFile — never from a
// flag or environment variable, since a PostgreSQL DSN ordinarily embeds its
// own credentials — and hands the resulting string to [postgres.Open], which
// dials the pool, applies the embedded schema migration, and starts the
// LISTEN/NOTIFY notifier eagerly, so a bad DSN or a missing privilege fails
// here at startup rather than on a caller's first request.
func openPostgresStore(ctx context.Context, cfg Config) (storeHandle, error) {
	dsn, err := readSecretFile(cfg.PostgresDSNFile)
	if err != nil {
		return storeHandle{}, err
	}
	if dsn == "" {
		return storeHandle{}, fmt.Errorf("dagworkerd: postgres DSN file %q is empty", cfg.PostgresDSNFile)
	}

	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		return storeHandle{}, fmt.Errorf("dagworkerd: opening postgres store: %w", err)
	}
	return storeHandle{store: store}, nil
}
