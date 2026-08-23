package httpadapter

import _ "embed"

// openapiSpec is the hand-written OpenAPI 3.1 document describing this
// server's surface, served at GET /openapi.yaml (routes.go).
//
// It is authored directly rather than derived from the gRPC adapter's
// .proto files: the streaming mismatch (grpc-gateway degrades a server-stream
// RPC to bare NDJSON with no Last-Event-ID/heartbeat/EventSource support) would
// force hand-written code on the events endpoint regardless, and mechanical
// derivation would drag google.golang.org/grpc into this module's dependency
// graph for a consumer who wants only HTTP (docs/research/14 §9.2, ADR-0037).
// Keeping the two surfaces consistent is a documentation/CI discipline, not a
// generator's job.
//
//go:embed openapi.yaml
var openapiSpec []byte
