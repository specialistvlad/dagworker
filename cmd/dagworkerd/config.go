package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Backend names accepted by --store / DAGWORKERD_STORE / the config file's
// "store" key. Kept as a closed set rather than free text so a typo is
// rejected at startup, not the first time a scope is touched.
const (
	storeMemory   = "memory"
	storeRedis    = "redis"
	storePostgres = "postgres"
)

// Config is dagworkerd's fully resolved configuration: one value per setting,
// with every precedence question already answered. Nothing downstream of
// [LoadConfig] ever needs to know that a value could have come from a flag,
// an environment variable, or a file.
//
// Every secret-shaped setting (a password, a DSN) is a file PATH, never the
// secret's value: a value handed to this process as a flag or an environment
// variable is legible to `docker inspect`, to `/proc/<pid>/environ`, and to
// crash-reporting tooling that dumps environment blocks by default, none of
// which is true of a value read from a file with its own filesystem
// permissions (docs/research/15-daemon-packaging-and-ops.md Part 2 §2.1,
// citing OWASP's Secrets Management Cheat Sheet).
type Config struct {
	// Store selects the storage backend: "memory", "redis", or "postgres".
	Store string

	// RedisAddr is the host:port to dial when Store is "redis". Not a secret:
	// an address names a service, it does not authenticate to one.
	RedisAddr string
	// RedisPasswordFile is the path to a file holding the Redis AUTH
	// password. Empty means no password.
	RedisPasswordFile string

	// PostgresDSNFile is the path to a file holding the full PostgreSQL
	// connection string when Store is "postgres". The whole DSN is treated as
	// a secret because it ordinarily embeds credentials.
	PostgresDSNFile string

	// GRPCAddr is the listen address for the gRPC adapter. Empty disables it.
	GRPCAddr string
	// HTTPAddr is the listen address for the HTTP/JSON adapter. Empty
	// disables it.
	HTTPAddr string

	// AdminAddr is the listen address for /healthz, /readyz, /metrics and
	// pprof — always separate from GRPCAddr/HTTPAddr, per the adapter
	// contract's operational rules: the claim-serving surface and the
	// operator surface have different audiences and must never share a port.
	AdminAddr string
	// AdminPprof gates registering net/http/pprof's handlers on the admin
	// listener. Off by default: a 30-second blocking CPU-profile trigger and
	// full heap dumps are not something every deployment should expose by
	// merely starting the process.
	AdminPprof bool

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogFormat is "json" or "text".
	LogFormat string

	// ShutdownTimeout bounds the whole graceful-shutdown sequence: past this
	// deadline dagworkerd stops waiting for in-flight work to drain and exits
	// anyway, so a stuck handler cannot hang a rolling restart forever.
	ShutdownTimeout time.Duration
}

// defaultConfig returns the conservative baseline every other layer overrides
// piece by piece. It deliberately runs a single, in-process worker pool's
// worth of defaults — memory backend, no network adapter enabled — so that
// starting dagworkerd with no flags at all does something safe rather than
// something surprising.
func defaultConfig() Config {
	return Config{
		Store:           storeMemory,
		AdminAddr:       "127.0.0.1:9090",
		LogLevel:        "info",
		LogFormat:       "json",
		ShutdownTimeout: 30 * time.Second,
	}
}

// overrides is what one configuration layer (file, environment, or flags)
// contributes: a field is nil when that layer did not mention it, which is
// what makes "flag > env > file > default" precedence a simple three-step
// fold over defaultConfig() rather than a tangle of per-field conditionals.
type overrides struct {
	Store             *string
	RedisAddr         *string
	RedisPasswordFile *string
	PostgresDSNFile   *string
	GRPCAddr          *string
	HTTPAddr          *string
	AdminAddr         *string
	AdminPprof        *bool
	LogLevel          *string
	LogFormat         *string
	ShutdownTimeout   *time.Duration
}

// apply overwrites every field of cfg that o actually set, leaving the rest
// exactly as a lower-precedence layer left them.
func (o overrides) apply(cfg *Config) {
	if o.Store != nil {
		cfg.Store = *o.Store
	}
	if o.RedisAddr != nil {
		cfg.RedisAddr = *o.RedisAddr
	}
	if o.RedisPasswordFile != nil {
		cfg.RedisPasswordFile = *o.RedisPasswordFile
	}
	if o.PostgresDSNFile != nil {
		cfg.PostgresDSNFile = *o.PostgresDSNFile
	}
	if o.GRPCAddr != nil {
		cfg.GRPCAddr = *o.GRPCAddr
	}
	if o.HTTPAddr != nil {
		cfg.HTTPAddr = *o.HTTPAddr
	}
	if o.AdminAddr != nil {
		cfg.AdminAddr = *o.AdminAddr
	}
	if o.AdminPprof != nil {
		cfg.AdminPprof = *o.AdminPprof
	}
	if o.LogLevel != nil {
		cfg.LogLevel = *o.LogLevel
	}
	if o.LogFormat != nil {
		cfg.LogFormat = *o.LogFormat
	}
	if o.ShutdownTimeout != nil {
		cfg.ShutdownTimeout = *o.ShutdownTimeout
	}
}

// fileOverrides is the shape of the optional YAML config file. Field names
// mirror [overrides] exactly; only the YAML tags and, for ShutdownTimeout,
// the wire type (a duration string, since YAML has no native duration) exist
// to bridge the two.
type fileOverrides struct {
	Store             *string `yaml:"store"`
	RedisAddr         *string `yaml:"redis_addr"`
	RedisPasswordFile *string `yaml:"redis_password_file"`
	PostgresDSNFile   *string `yaml:"postgres_dsn_file"`
	GRPCAddr          *string `yaml:"grpc_addr"`
	HTTPAddr          *string `yaml:"http_addr"`
	AdminAddr         *string `yaml:"admin_addr"`
	AdminPprof        *bool   `yaml:"admin_pprof"`
	LogLevel          *string `yaml:"log_level"`
	LogFormat         *string `yaml:"log_format"`
	ShutdownTimeout   *string `yaml:"shutdown_timeout"`
}

// toOverrides converts the file's wire shape to the common layer shape,
// parsing the one field (a duration) whose text representation is not
// already what [overrides] wants.
func (f fileOverrides) toOverrides() (overrides, error) {
	o := overrides{
		Store: f.Store, RedisAddr: f.RedisAddr, RedisPasswordFile: f.RedisPasswordFile,
		PostgresDSNFile: f.PostgresDSNFile, GRPCAddr: f.GRPCAddr, HTTPAddr: f.HTTPAddr,
		AdminAddr: f.AdminAddr, AdminPprof: f.AdminPprof, LogLevel: f.LogLevel, LogFormat: f.LogFormat,
	}
	if f.ShutdownTimeout != nil {
		d, err := time.ParseDuration(*f.ShutdownTimeout)
		if err != nil {
			return overrides{}, fmt.Errorf("dagworkerd: config file shutdown_timeout: %w", err)
		}
		o.ShutdownTimeout = &d
	}
	return o, nil
}

// loadFileOverrides reads and parses the YAML config file at path. A path
// that does not name an existing file is always the caller's mistake, never
// treated as "no file configured" — that case is expressed by never calling
// this function at all.
func loadFileOverrides(path string) (overrides, error) {
	data, err := os.ReadFile(path) //nolint:gosec // an operator-supplied config path is exactly what this function reads
	if err != nil {
		return overrides{}, fmt.Errorf("dagworkerd: reading config file %q: %w", path, err)
	}
	var f fileOverrides
	if err := yaml.Unmarshal(data, &f); err != nil {
		return overrides{}, fmt.Errorf("dagworkerd: parsing config file %q: %w", path, err)
	}
	return f.toOverrides()
}

// envKeys maps each setting to its environment variable name, under the
// DAGWORKERD_ prefix every 12-factor-style deployment tool expects to be
// able to override.
const (
	envConfigFile = "DAGWORKERD_CONFIG"
	envStore      = "DAGWORKERD_STORE"
	envRedisAddr  = "DAGWORKERD_REDIS_ADDR"
	// envRedisPasswordFile names the env var carrying a PATH, never the
	// password itself — see [Config]'s doc comment.
	envRedisPasswordFile = "DAGWORKERD_REDIS_PASSWORD_FILE" //nolint:gosec // this is an env var NAME, not a credential value
	envPostgresDSNFile   = "DAGWORKERD_POSTGRES_DSN_FILE"
	envGRPCAddr          = "DAGWORKERD_GRPC_ADDR"
	envHTTPAddr          = "DAGWORKERD_HTTP_ADDR"
	envAdminAddr         = "DAGWORKERD_ADMIN_ADDR"
	envAdminPprof        = "DAGWORKERD_ADMIN_PPROF"
	envLogLevel          = "DAGWORKERD_LOG_LEVEL"
	envLogFormat         = "DAGWORKERD_LOG_FORMAT"
	envShutdownTimeout   = "DAGWORKERD_SHUTDOWN_TIMEOUT"
)

// lookupEnv mirrors [os.LookupEnv]'s (value, present) shape. Tests supply a
// map-backed one instead of the process environment, which is what lets
// config precedence be verified without mutating global process state that
// t.Parallel() subtests would otherwise race on.
type lookupEnv func(key string) (string, bool)

// loadEnvOverrides reads every DAGWORKERD_* variable getenv reports as
// present. A variable that is present but empty still counts as "set" — an
// operator who exports FOO="" meant something by it — the same rule
// [os.LookupEnv] itself draws between "unset" and "empty."
func loadEnvOverrides(getenv lookupEnv) (overrides, error) {
	var o overrides
	if v, ok := getenv(envStore); ok {
		o.Store = &v
	}
	if v, ok := getenv(envRedisAddr); ok {
		o.RedisAddr = &v
	}
	if v, ok := getenv(envRedisPasswordFile); ok {
		o.RedisPasswordFile = &v
	}
	if v, ok := getenv(envPostgresDSNFile); ok {
		o.PostgresDSNFile = &v
	}
	if v, ok := getenv(envGRPCAddr); ok {
		o.GRPCAddr = &v
	}
	if v, ok := getenv(envHTTPAddr); ok {
		o.HTTPAddr = &v
	}
	if v, ok := getenv(envAdminAddr); ok {
		o.AdminAddr = &v
	}
	if v, ok := getenv(envLogLevel); ok {
		o.LogLevel = &v
	}
	if v, ok := getenv(envLogFormat); ok {
		o.LogFormat = &v
	}
	if err := loadEnvBoolDuration(getenv, &o); err != nil {
		return overrides{}, err
	}
	return o, nil
}

// loadEnvBoolDuration handles the two env vars whose value is not already a
// plain string, split out of [loadEnvOverrides] to keep that function under
// this module's own complexity ceiling.
func loadEnvBoolDuration(getenv lookupEnv, o *overrides) error {
	if v, ok := getenv(envAdminPprof); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("dagworkerd: %s: %w", envAdminPprof, err)
		}
		o.AdminPprof = &b
	}
	if v, ok := getenv(envShutdownTimeout); ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("dagworkerd: %s: %w", envShutdownTimeout, err)
		}
		o.ShutdownTimeout = &d
	}
	return nil
}

// flagValues holds every flag.FlagSet variable. Kept as its own type so
// buildFlagOverrides can read it after Parse without threading a dozen
// separate pointers through the call.
type flagValues struct {
	store, redisAddr, redisPasswordFile, postgresDSNFile string
	grpcAddr, httpAddr, adminAddr                        string
	adminPprof                                           bool
	logLevel, logFormat                                  string
	shutdownTimeout                                      time.Duration
	configFile                                           string
	version                                              bool
}

// newFlagSet declares every flag dagworkerd accepts. Defaults here are
// intentionally the zero value, not [defaultConfig]'s values: a flag's
// default must be indistinguishable from "not provided" so [flag.FlagSet.Visit]
// — the only reliable way to ask "did the operator actually pass this?" — can
// tell the two apart. The real defaults are applied once, in [buildConfig],
// beneath every other layer.
func newFlagSet(name string, output io.Writer) (*flag.FlagSet, *flagValues) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(output)
	var v flagValues
	fs.StringVar(&v.configFile, "config", "", "path to a YAML config file (env "+envConfigFile+")")
	fs.StringVar(&v.store, "store", "", "storage backend: memory, redis, or postgres (env "+envStore+")")
	fs.StringVar(&v.redisAddr, "redis-addr", "", "redis host:port, required when --store=redis (env "+envRedisAddr+")")
	fs.StringVar(&v.redisPasswordFile, "redis-password-file", "",
		"path to a file holding the redis AUTH password (env "+envRedisPasswordFile+")")
	fs.StringVar(&v.postgresDSNFile, "postgres-dsn-file", "",
		"path to a file holding the postgres DSN, required when --store=postgres (env "+envPostgresDSNFile+")")
	fs.StringVar(&v.grpcAddr, "grpc-addr", "", "gRPC listen address, e.g. :9443; empty disables it (env "+envGRPCAddr+")")
	fs.StringVar(&v.httpAddr, "http-addr", "", "HTTP/JSON listen address, e.g. :8080; empty disables it (env "+envHTTPAddr+")")
	fs.StringVar(&v.adminAddr, "admin-addr", "", "admin listen address for healthz/readyz/metrics/pprof (env "+envAdminAddr+")")
	fs.BoolVar(&v.adminPprof, "admin-pprof", false, "expose /debug/pprof/* on the admin listener (env "+envAdminPprof+")")
	fs.StringVar(&v.logLevel, "log-level", "", "debug, info, warn, or error (env "+envLogLevel+")")
	fs.StringVar(&v.logFormat, "log-format", "", "json or text (env "+envLogFormat+")")
	fs.DurationVar(&v.shutdownTimeout, "shutdown-timeout", 0, "bound on the graceful shutdown sequence (env "+envShutdownTimeout+")")
	fs.BoolVar(&v.version, "version", false, "print version information and exit")
	return fs, &v
}

// buildFlagOverrides walks only the flags the operator actually passed
// (fs.Visit, not fs.VisitAll), so an unset flag never masks a lower-precedence
// env or file value with its zero default.
func buildFlagOverrides(fs *flag.FlagSet, v *flagValues) overrides {
	var o overrides
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "store":
			o.Store = &v.store
		case "redis-addr":
			o.RedisAddr = &v.redisAddr
		case "redis-password-file":
			o.RedisPasswordFile = &v.redisPasswordFile
		case "postgres-dsn-file":
			o.PostgresDSNFile = &v.postgresDSNFile
		case "grpc-addr":
			o.GRPCAddr = &v.grpcAddr
		case "http-addr":
			o.HTTPAddr = &v.httpAddr
		case "admin-addr":
			o.AdminAddr = &v.adminAddr
		case "admin-pprof":
			o.AdminPprof = &v.adminPprof
		case "log-level":
			o.LogLevel = &v.logLevel
		case "log-format":
			o.LogFormat = &v.logFormat
		case "shutdown-timeout":
			o.ShutdownTimeout = &v.shutdownTimeout
		}
	})
	return o
}

// LoadConfig resolves dagworkerd's configuration with precedence
// flag > env > file > default, in that order of authority: each later layer
// in this list overrides only the fields it actually mentions.
//
// The config file's own path is resolved with the identical precedence
// (--config flag, then DAGWORKERD_CONFIG) before anything else, because the
// file has to be found before it can contribute a layer.
func LoadConfig(args []string, getenv lookupEnv, output io.Writer) (Config, bool, error) {
	fs, v := newFlagSet("dagworkerd", output)
	if err := fs.Parse(args); err != nil {
		return Config{}, false, err
	}
	if v.version {
		return Config{}, true, nil
	}

	cfg := defaultConfig()

	if err := applyFileLayer(fs, v, getenv, &cfg); err != nil {
		return Config{}, false, err
	}

	envOv, err := loadEnvOverrides(getenv)
	if err != nil {
		return Config{}, false, err
	}
	envOv.apply(&cfg)

	buildFlagOverrides(fs, v).apply(&cfg)

	if err := cfg.validate(); err != nil {
		return Config{}, false, err
	}
	return cfg, false, nil
}

// applyFileLayer resolves the config file's path (flag beats env, exactly
// like every other setting) and, if one was named, applies it as the
// lowest-precedence layer.
func applyFileLayer(fs *flag.FlagSet, v *flagValues, getenv lookupEnv, cfg *Config) error {
	path := v.configFile
	explicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			explicit = true
		}
	})
	if !explicit {
		if envPath, ok := getenv(envConfigFile); ok && envPath != "" {
			path = envPath
		}
	}
	if path == "" {
		return nil
	}
	fileOv, err := loadFileOverrides(path)
	if err != nil {
		return err
	}
	fileOv.apply(cfg)
	return nil
}

var (
	errUnknownStore   = errors.New("dagworkerd: --store must be memory, redis, or postgres")
	errMissingAddr    = errors.New("dagworkerd: --redis-addr is required when --store=redis")
	errMissingDSNFile = errors.New("dagworkerd: --postgres-dsn-file is required when --store=postgres")
	errNoAdapter      = errors.New("dagworkerd: at least one of --grpc-addr or --http-addr must be set")
	errBadLogLevel    = errors.New("dagworkerd: --log-level must be debug, info, warn, or error")
	errBadLogFormat   = errors.New("dagworkerd: --log-format must be json or text")
	errBadShutdown    = errors.New("dagworkerd: --shutdown-timeout must be positive")
)

// validate rejects a configuration that would fail loudly and confusingly
// later — a missing DSN file surfacing as a nil-pointer panic deep in a
// backend constructor, say — in favor of failing here, at startup, with a
// message that names the flag to fix.
func (c Config) validate() error {
	switch c.Store {
	case storeMemory, storeRedis, storePostgres:
	default:
		return errUnknownStore
	}
	if c.Store == storeRedis && c.RedisAddr == "" {
		return errMissingAddr
	}
	if c.Store == storePostgres && c.PostgresDSNFile == "" {
		return errMissingDSNFile
	}
	if c.GRPCAddr == "" && c.HTTPAddr == "" {
		return errNoAdapter
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return errBadLogLevel
	}
	switch c.LogFormat {
	case "json", "text":
	default:
		return errBadLogFormat
	}
	if c.ShutdownTimeout <= 0 {
		return errBadShutdown
	}
	return nil
}
