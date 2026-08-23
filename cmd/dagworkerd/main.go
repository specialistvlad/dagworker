package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

// run is main's entire body, extracted so a test can drive it with synthetic
// args, environment, and output sinks instead of the real process's — the
// same reason [LoadConfig] takes a [lookupEnv] rather than calling
// [os.LookupEnv] directly.
func run(args []string, getenv lookupEnv, stdout, stderr io.Writer) int {
	cfg, versionRequested, err := LoadConfig(args, getenv, stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0 // usage was already written to stderr by the flag package itself
	case err != nil:
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	case versionRequested:
		printVersion(stdout)
		return 0
	}

	logger, err := newLogger(cfg.LogFormat, cfg.LogLevel, stderr)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	logStartupConfig(logger, cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// The instant the shutdown trigger fires, restore default signal
	// disposition — before the bounded shutdown sequence below even starts —
	// so an operator's second Ctrl-C/SIGTERM force-kills the process instead
	// of being silently absorbed by a context that is already cancelled
	// (docs/research/15-daemon-packaging-and-ops.md Part 2 §2.5).
	go func() {
		<-ctx.Done()
		stop()
	}()

	d, err := newDaemon(ctx, cfg, logger)
	if err != nil {
		logger.Error("dagworkerd: startup failed", "error", err)
		return 1
	}
	logger.Info("dagworkerd: listening",
		"grpc_addr", addrString(d.GRPCAddr()),
		"http_addr", addrString(d.HTTPAddr()),
		"admin_addr", addrString(d.AdminAddr()),
	)

	if err := d.Run(ctx); err != nil {
		logger.Error("dagworkerd: exited with error", "error", err)
		return 1
	}
	logger.Info("dagworkerd: shutdown complete")
	return 0
}

// addrString reports addr's string form, or "disabled" for the nil returned
// by [daemon.GRPCAddr]/[daemon.HTTPAddr] when that adapter was never enabled.
func addrString(addr net.Addr) string {
	if addr == nil {
		return "disabled"
	}
	return addr.String()
}
