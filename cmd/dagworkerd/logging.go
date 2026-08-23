package main

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// parseLevel maps the four config-file-and-flag-friendly level names onto
// [slog.Level]. slog's own level type would happily accept an arbitrary
// integer; rejecting anything outside the documented four names here is what
// turns a typo'd --log-level into a startup error instead of a silently
// wrong verbosity.
func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("dagworkerd: unknown log level %q", s)
	}
}

// newLogger builds the one root [*slog.Logger] dagworkerd constructs, from
// which every other logger in the process (the Manager's, each adapter's) is
// derived by explicit passing — never via slog.SetDefault, which would make
// this process's log configuration a hidden global any imported package
// could observe or fight over.
func newLogger(format, level string, w io.Writer) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "text":
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("dagworkerd: unknown log format %q", format)
	}
	return slog.New(h), nil
}

// logStartupConfig logs the effective configuration once, at startup. Every
// secret-shaped field logs only the file PATH it was told to read — never a
// value, because [Config] itself never holds one: the actual secret exists
// only as a local variable inside whichever function dials the backend, for
// exactly as long as that call needs it, which is what makes "never logged"
// true by construction rather than by the discipline of every call site that
// might someday log cfg.
func logStartupConfig(logger *slog.Logger, cfg Config) {
	logger.Info("dagworkerd: starting",
		"store", cfg.Store,
		"redis_addr", cfg.RedisAddr,
		"redis_password_file", cfg.RedisPasswordFile,
		"postgres_dsn_file", cfg.PostgresDSNFile,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
		"admin_addr", cfg.AdminAddr,
		"admin_pprof", cfg.AdminPprof,
		"log_level", cfg.LogLevel,
		"log_format", cfg.LogFormat,
		"shutdown_timeout", cfg.ShutdownTimeout,
	)
}
