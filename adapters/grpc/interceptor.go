package grpcadapter

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorUnaryInterceptor is the single place every handler's error passes
// through [mapError], so a handler returns a plain dagworker error and never
// constructs a *status.Status itself — one mapping, applied once, per
// docs/spec/02-adapter-contract.md §3's framing of the table as a contract,
// not a suggestion each handler is free to reinterpret.
//
// It also recovers a panic rather than letting it take down the whole
// server: one handler's bug should cost that RPC an INTERNAL, not every
// other in-flight ClaimNode and Watch on the process.
func errorUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer recoverToError(logger, info.FullMethod, &err)
		resp, err = handler(ctx, req)
		return resp, translateOutgoing(err)
	}
}

// errorStreamInterceptor is errorUnaryInterceptor's counterpart for Watch,
// the one streaming RPC this adapter exposes.
func errorStreamInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer recoverToError(logger, info.FullMethod, &err)
		err = handler(srv, ss)
		return translateOutgoing(err)
	}
}

// translateOutgoing leaves an error that is already a gRPC status alone —
// every handler in this package returns errors already passed through
// mapError or decodeTaskToken's own status.Error, so this is a defensive
// no-op in practice, not a second mapping pass — and maps anything else
// through the table, so a stray plain error can never leak an unstructured
// message with no machine-readable reason attached.
func translateOutgoing(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return mapError(err)
}

func recoverToError(logger *slog.Logger, method string, err *error) {
	if r := recover(); r != nil {
		logger.Error("dagworker/grpc: recovered panic in handler", "method", method, "panic", r)
		*err = status.Error(codes.Internal, "internal error handling "+method)
	}
}
