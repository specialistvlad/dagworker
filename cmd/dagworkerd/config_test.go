package main

import (
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// noEnv returns a [lookupEnv] that reports every variable absent, for tests
// that want the environment layer to contribute nothing.
func noEnv(string) (string, bool) { return "", false }

// mapEnv turns a plain map into a [lookupEnv], for tests that want to control
// exactly which variables are "set" without touching the real process
// environment — real env vars are process-global, and mutating them would
// make every other test in this package unsafe to run with t.Parallel().
func mapEnv(m map[string]string) lookupEnv {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

// writeFile is a small t.Helper wrapper so every test below reads as "here is
// the file's content" without repeating os.WriteFile's error-handling.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func TestLoadConfig_DefaultsWithNoLayers(t *testing.T) {
	t.Parallel()

	cfg, versionRequested, err := LoadConfig([]string{"--http-addr=:8080"}, noEnv, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if versionRequested {
		t.Fatalf("versionRequested = true, want false")
	}

	switch {
	case cfg.Store != storeMemory:
		t.Errorf("Store = %q, want %q", cfg.Store, storeMemory)
	case cfg.AdminAddr != "127.0.0.1:9090":
		t.Errorf("AdminAddr = %q, want 127.0.0.1:9090", cfg.AdminAddr)
	case cfg.LogLevel != "info":
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	case cfg.LogFormat != "json":
		t.Errorf("LogFormat = %q, want json", cfg.LogFormat)
	case cfg.ShutdownTimeout != 30*time.Second:
		t.Errorf("ShutdownTimeout = %s, want 30s", cfg.ShutdownTimeout)
	case cfg.HTTPAddr != ":8080":
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
}

func TestLoadConfig_EnvOverridesFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "config.yaml")
	writeFile(t, filePath, "store: memory\nlog_level: warn\nhttp_addr: \":9000\"\n")

	env := mapEnv(map[string]string{
		envLogLevel: "debug", // must win over the file's "warn"
	})

	cfg, _, err := LoadConfig([]string{"--config=" + filePath}, env, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug (env must override the file)", cfg.LogLevel)
	}
	if cfg.HTTPAddr != ":9000" {
		t.Errorf("HTTPAddr = %q, want :9000 (file value must survive when env does not mention it)", cfg.HTTPAddr)
	}
}

func TestLoadConfig_FlagOverridesEnvAndFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "config.yaml")
	writeFile(t, filePath, "log_level: warn\nhttp_addr: \":9000\"\n")

	env := mapEnv(map[string]string{
		envLogLevel: "debug",
		envHTTPAddr: ":9001",
	})

	cfg, _, err := LoadConfig(
		[]string{"--config=" + filePath, "--log-level=error"},
		env, io.Discard,
	)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("LogLevel = %q, want error (an explicit flag must win over both env and file)", cfg.LogLevel)
	}
	if cfg.HTTPAddr != ":9001" {
		t.Errorf("HTTPAddr = %q, want :9001 (env must still win over the file when no flag was given)", cfg.HTTPAddr)
	}
}

func TestLoadConfig_ConfigPathItselfObeysPrecedence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	flagFile := filepath.Join(dir, "flag.yaml")
	envFile := filepath.Join(dir, "env.yaml")
	writeFile(t, flagFile, "http_addr: \":7001\"\n")
	writeFile(t, envFile, "http_addr: \":7002\"\n")

	env := mapEnv(map[string]string{envConfigFile: envFile})

	// No --config flag: the env-named file must be used.
	cfg, _, err := LoadConfig(nil, env, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.HTTPAddr != ":7002" {
		t.Errorf("HTTPAddr = %q, want :7002 (from DAGWORKERD_CONFIG's file)", cfg.HTTPAddr)
	}

	// An explicit --config flag must win over DAGWORKERD_CONFIG.
	cfg, _, err = LoadConfig([]string{"--config=" + flagFile}, env, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.HTTPAddr != ":7001" {
		t.Errorf("HTTPAddr = %q, want :7001 (--config must win over DAGWORKERD_CONFIG)", cfg.HTTPAddr)
	}
}

func TestLoadConfig_EmptyEnvValueStillCountsAsSet(t *testing.T) {
	t.Parallel()

	// An operator who exports DAGWORKERD_GRPC_ADDR="" meant something by it;
	// this must not be treated the same as the variable being entirely unset.
	env := mapEnv(map[string]string{envHTTPAddr: ":8080", envGRPCAddr: ""})

	cfg, _, err := LoadConfig(nil, env, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.GRPCAddr != "" {
		t.Errorf("GRPCAddr = %q, want empty", cfg.GRPCAddr)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
}

func TestLoadConfig_SecretIsReadFromFileNeverFromFlagOrEnv(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	secretPath := filepath.Join(dir, "redis.pass")
	const secretValue = "correct-horse-battery-staple"
	writeFile(t, secretPath, secretValue+"\n")

	cfg, _, err := LoadConfig(
		[]string{"--store=redis", "--redis-addr=127.0.0.1:6379", "--redis-password-file=" + secretPath, "--http-addr=:8080"},
		noEnv, io.Discard,
	)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.RedisPasswordFile != secretPath {
		t.Fatalf("RedisPasswordFile = %q, want %q (only the PATH travels through config, never the value)",
			cfg.RedisPasswordFile, secretPath)
	}

	// The path resolves to the real secret when actually read...
	got, err := readSecretFile(cfg.RedisPasswordFile)
	if err != nil {
		t.Fatalf("readSecretFile: %v", err)
	}
	if got != secretValue {
		t.Errorf("readSecretFile = %q, want %q", got, secretValue)
	}

	// ...but logging the effective startup config must never surface it.
	var buf strings.Builder
	logger, err := newLogger("text", "info", &buf)
	if err != nil {
		t.Fatalf("newLogger: %v", err)
	}
	logStartupConfig(logger, cfg)

	logged := buf.String()
	if !strings.Contains(logged, secretPath) {
		t.Errorf("startup log does not mention the configured secret file path %q:\n%s", secretPath, logged)
	}
	if strings.Contains(logged, secretValue) {
		t.Errorf("startup log leaked the secret's VALUE, not just its path:\n%s", logged)
	}
}

func TestLoadConfig_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want error
	}{
		{"unknown store", []string{"--store=sqlite", "--http-addr=:1"}, errUnknownStore},
		{"redis without addr", []string{"--store=redis", "--http-addr=:1"}, errMissingAddr},
		{"postgres without dsn file", []string{"--store=postgres", "--http-addr=:1"}, errMissingDSNFile},
		{"no adapter enabled", nil, errNoAdapter},
		{"bad log level", []string{"--http-addr=:1", "--log-level=verbose"}, errBadLogLevel},
		{"bad log format", []string{"--http-addr=:1", "--log-format=xml"}, errBadLogFormat},
		{"non-positive shutdown timeout", []string{"--http-addr=:1", "--shutdown-timeout=0s"}, errBadShutdown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := LoadConfig(tt.args, noEnv, io.Discard)
			if !errors.Is(err, tt.want) {
				t.Errorf("LoadConfig error = %v, want wrapping %v", err, tt.want)
			}
		})
	}
}

func TestLoadConfig_ValidConfigurationsPass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
	}{
		{"memory with http", []string{"--http-addr=:1"}},
		{"memory with grpc", []string{"--grpc-addr=:1"}},
		{"redis with addr", []string{"--store=redis", "--redis-addr=127.0.0.1:6379", "--http-addr=:1"}},
		{"postgres with dsn file", []string{"--store=postgres", "--postgres-dsn-file=/nonexistent-but-set", "--http-addr=:1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := LoadConfig(tt.args, noEnv, io.Discard); err != nil {
				t.Errorf("LoadConfig: unexpected error: %v", err)
			}
		})
	}
}

func TestLoadConfig_VersionShortCircuitsValidation(t *testing.T) {
	t.Parallel()

	// --version alone would otherwise fail "at least one adapter" validation;
	// it must return before validation ever runs.
	_, versionRequested, err := LoadConfig([]string{"--version"}, noEnv, io.Discard)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !versionRequested {
		t.Errorf("versionRequested = false, want true")
	}
}

func TestLoadConfig_HelpReturnsErrHelp(t *testing.T) {
	t.Parallel()

	_, _, err := LoadConfig([]string{"--help"}, noEnv, io.Discard)
	if !errors.Is(err, flag.ErrHelp) {
		t.Errorf("err = %v, want flag.ErrHelp", err)
	}
}

func TestLoadConfig_UnknownFlagIsAnError(t *testing.T) {
	t.Parallel()

	if _, _, err := LoadConfig([]string{"--not-a-real-flag"}, noEnv, io.Discard); err == nil {
		t.Errorf("LoadConfig: expected an error for an unknown flag")
	}
}

func TestLoadConfig_BadFileIsAnError(t *testing.T) {
	t.Parallel()

	if _, _, err := LoadConfig([]string{"--config=/does/not/exist.yaml"}, noEnv, io.Discard); err == nil {
		t.Errorf("LoadConfig: expected an error for a missing config file")
	}
}

func TestLoadConfig_MalformedFileIsAnError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	writeFile(t, path, "store: [this is not a string\n")

	if _, _, err := LoadConfig([]string{"--config=" + path}, noEnv, io.Discard); err == nil {
		t.Errorf("LoadConfig: expected an error for a malformed config file")
	}
}

func TestLoadConfig_BadEnvDurationIsAnError(t *testing.T) {
	t.Parallel()

	env := mapEnv(map[string]string{envShutdownTimeout: "not-a-duration"})
	if _, _, err := LoadConfig([]string{"--http-addr=:1"}, env, io.Discard); err == nil {
		t.Errorf("LoadConfig: expected an error for a malformed duration env var")
	}
}

func TestLoadConfig_BadEnvBoolIsAnError(t *testing.T) {
	t.Parallel()

	env := mapEnv(map[string]string{envAdminPprof: "sort-of"})
	if _, _, err := LoadConfig([]string{"--http-addr=:1"}, env, io.Discard); err == nil {
		t.Errorf("LoadConfig: expected an error for a malformed bool env var")
	}
}
