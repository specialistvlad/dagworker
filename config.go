package dagworker

import (
	"log/slog"
	"time"
)

// Conservative fallbacks for every [ScopeConfig] field. They are deliberately
// unopinionated: the library serves both job-queue-shaped workloads (many
// small, short-lived scopes) and pipeline-shaped ones (a few huge, long-lived
// scopes), so no default privileges either. Where a wrong default would destroy
// data or surprise an operator, the fallback disables the feature instead.
const (
	// DefaultLeaseTimeout is long enough for ordinary work and short enough
	// that a dead worker is noticed promptly.
	DefaultLeaseTimeout = 30 * time.Second

	// DefaultMinLeaseTimeout floors the per-claim override. Below roughly a
	// second, reclaim churn starts to dominate useful work.
	DefaultMinLeaseTimeout = time.Second

	// DefaultMaxLeaseTimeout is a hard ceiling so a misconfigured caller cannot
	// strand a node indefinitely.
	DefaultMaxLeaseTimeout = 24 * time.Hour

	// DefaultMaxAttempts allows one retry for a transient fault and one more
	// for bad luck. It is not a retry loop.
	DefaultMaxAttempts = 3

	// DefaultRetryBaseDelay and DefaultRetryMaxDelay bound the full-jitter
	// backoff.
	DefaultRetryBaseDelay = time.Second
	DefaultRetryMaxDelay  = 5 * time.Minute

	// DefaultPayloadCap sits comfortably below every supported backend's
	// practical value ceiling. Store a reference to a blob rather than the blob.
	DefaultPayloadCap = 256 << 10

	// DefaultMaxBatchSize fits in one PostgreSQL transaction and one bounded
	// Lua script without either becoming a latency outlier.
	DefaultMaxBatchSize = 1000

	// DefaultSweepBatchSize bounds how long a single sweep can stall a
	// single-threaded backend or hold locks in a transactional one.
	DefaultSweepBatchSize = 100

	// DefaultSweepInterval is how often the background sweeper runs when
	// nothing else has triggered an inline reclaim.
	DefaultSweepInterval = 5 * time.Second

	// DefaultSubscriberBuffer is the per-subscription channel depth.
	DefaultSubscriberBuffer = 256
)

// ScopeConfig is the per-scope policy. It is stored in the backend alongside
// the scope's data so that every Manager instance sharing that backend agrees
// on it, rather than each process carrying its own opinion.
//
// Every field's zero value means "use the conservative fallback", except the
// three noted below where zero means "disabled". Disabled is the honest default
// for anything that would otherwise delete a caller's data or silently advance
// past a subscriber.
type ScopeConfig struct {
	// DefaultLeaseTimeout applies when a claim does not specify one.
	DefaultLeaseTimeout time.Duration
	// MinLeaseTimeout and MaxLeaseTimeout clamp every per-claim override.
	MinLeaseTimeout time.Duration
	MaxLeaseTimeout time.Duration

	// MaxAttempts, RetryBaseDelay and RetryMaxDelay are the scope-wide retry
	// defaults; a node's own [RetryPolicy] overrides them field by field.
	MaxAttempts    uint32
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// TerminalRetention is how long a terminal node is kept before garbage
	// collection. Zero means never collect. A library that deletes a caller's
	// data by default is a defect, so this is off unless asked for.
	TerminalRetention time.Duration

	// MaxSubscriberLag caps how far behind a durable subscriber may fall before
	// retention advances past it anyway. Zero means unbounded: never advance
	// past a subscriber silently.
	MaxSubscriberLag time.Duration

	// MaxInFlight caps concurrently claimed nodes in the scope. Zero means
	// unlimited; the host owns its own concurrency.
	MaxInFlight uint32

	// PayloadCap is the largest permitted node payload in bytes. The effective
	// cap is the smallest of this, the Manager's cap, and the backend's own.
	PayloadCap int

	// MaxBatchSize caps the nodes in one [Manager.AddNodes] call.
	MaxBatchSize int

	// SweepBatchSize caps the leases reclaimed in one sweep operation.
	SweepBatchSize int

	// SweepInterval is the background sweeper's period.
	SweepInterval time.Duration

	// PartitionCount is the number of virtual partitions the scope's ready set
	// is split across. One is correct for pull-based competition, which is what
	// this version ships; larger values are reserved for the partitioned
	// distribution strategy and are accepted but not yet acted upon.
	PartitionCount uint32
}

// resolved returns c with every zero-means-fallback field filled in. Fields
// whose zero means "disabled" are left alone.
func (c ScopeConfig) resolved() ScopeConfig {
	out := c
	if out.DefaultLeaseTimeout <= 0 {
		out.DefaultLeaseTimeout = DefaultLeaseTimeout
	}
	if out.MinLeaseTimeout <= 0 {
		out.MinLeaseTimeout = DefaultMinLeaseTimeout
	}
	if out.MaxLeaseTimeout <= 0 {
		out.MaxLeaseTimeout = DefaultMaxLeaseTimeout
	}
	if out.MaxAttempts == 0 {
		out.MaxAttempts = DefaultMaxAttempts
	}
	if out.RetryBaseDelay <= 0 {
		out.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if out.RetryMaxDelay <= 0 {
		out.RetryMaxDelay = DefaultRetryMaxDelay
	}
	if out.PayloadCap <= 0 {
		out.PayloadCap = DefaultPayloadCap
	}
	if out.MaxBatchSize <= 0 {
		out.MaxBatchSize = DefaultMaxBatchSize
	}
	if out.SweepBatchSize <= 0 {
		out.SweepBatchSize = DefaultSweepBatchSize
	}
	if out.SweepInterval <= 0 {
		out.SweepInterval = DefaultSweepInterval
	}
	if out.PartitionCount == 0 {
		out.PartitionCount = 1
	}
	// TerminalRetention, MaxSubscriberLag and MaxInFlight keep their zero:
	// zero means disabled, not "pick something for me".
	return out
}

func (c ScopeConfig) validate() error {
	r := c.resolved()
	if r.MinLeaseTimeout > r.MaxLeaseTimeout {
		return invalidArg("scope config", "min lease timeout %s exceeds max %s",
			r.MinLeaseTimeout, r.MaxLeaseTimeout)
	}
	if r.DefaultLeaseTimeout < r.MinLeaseTimeout || r.DefaultLeaseTimeout > r.MaxLeaseTimeout {
		return invalidArg("scope config", "default lease timeout %s is outside [%s, %s]",
			r.DefaultLeaseTimeout, r.MinLeaseTimeout, r.MaxLeaseTimeout)
	}
	if r.RetryBaseDelay > r.RetryMaxDelay {
		return invalidArg("scope config", "retry base delay %s exceeds max %s",
			r.RetryBaseDelay, r.RetryMaxDelay)
	}
	if c.TerminalRetention < 0 {
		return invalidArg("scope config", "terminal retention must not be negative")
	}
	if c.MaxSubscriberLag < 0 {
		return invalidArg("scope config", "max subscriber lag must not be negative")
	}
	return nil
}

// clampLease brings a requested lease duration inside the scope's bounds.
func (c ScopeConfig) clampLease(d time.Duration) time.Duration {
	r := c.resolved()
	if d <= 0 {
		d = r.DefaultLeaseTimeout
	}
	return min(max(d, r.MinLeaseTimeout), r.MaxLeaseTimeout)
}

// Option configures a [Manager]. The interface is opaque so that options can
// gain behaviour later without the set of legal values becoming part of the
// API's shape.
type Option interface{ apply(*config) }

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

type config struct {
	clock            Clock
	logger           *slog.Logger
	defaults         ScopeConfig
	subscriberBuffer int
	overflow         OverflowPolicy
	sweepDisabled    bool
}

func defaultConfig() config {
	return config{
		clock:            SystemClock{},
		logger:           slog.New(discardHandler{}),
		subscriberBuffer: DefaultSubscriberBuffer,
		overflow:         OverflowDropOldest,
	}
}

func (c config) validate() error {
	if c.clock == nil {
		return invalidArg("clock", "must not be nil")
	}
	if c.logger == nil {
		return invalidArg("logger", "must not be nil")
	}
	if c.subscriberBuffer < 1 {
		return invalidArg("subscriber buffer", "must be at least 1, got %d", c.subscriberBuffer)
	}
	if err := c.overflow.validate(); err != nil {
		return err
	}
	return c.defaults.validate()
}

// WithClock replaces the time source. Tests use it to drive retries, sweeps and
// poll intervals deterministically.
func WithClock(c Clock) Option {
	return optionFunc(func(cfg *config) { cfg.clock = c })
}

// WithLogger sets the structured logger. The default discards everything: a
// library that writes to a host's stderr uninvited is a defect, not a feature.
func WithLogger(l *slog.Logger) Option {
	return optionFunc(func(cfg *config) { cfg.logger = l })
}

// WithScopeDefaults sets the [ScopeConfig] applied to scopes that have no
// stored configuration of their own.
func WithScopeDefaults(sc ScopeConfig) Option {
	return optionFunc(func(cfg *config) { cfg.defaults = sc })
}

// WithSubscriberBuffer sets the default per-subscription channel depth.
func WithSubscriberBuffer(n int) Option {
	return optionFunc(func(cfg *config) { cfg.subscriberBuffer = n })
}

// WithOverflowPolicy sets the default policy for subscribers that fall behind.
func WithOverflowPolicy(p OverflowPolicy) Option {
	return optionFunc(func(cfg *config) { cfg.overflow = p })
}

// WithoutBackgroundSweeper disables the periodic lease reclaimer. Reclaim still
// happens inline on the claim path, so correctness is unaffected; only the
// latency of noticing a dead worker in an otherwise idle scope changes. Tests
// that drive time manually use this to keep their goroutine set quiet.
func WithoutBackgroundSweeper() Option {
	return optionFunc(func(cfg *config) { cfg.sweepDisabled = true })
}
