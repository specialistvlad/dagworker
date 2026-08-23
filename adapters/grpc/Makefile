# adapters/grpc — build and codegen helpers.
#
# `generate` is safe to run with nothing installed but Go: it `go install`s
# the two plugins it needs into $(go env GOPATH)/bin first. buf itself has no
# hard dependency on protoc for this module (the plugins speak buf's plugin
# protocol directly), so the only prerequisite is a `buf` binary on PATH —
# `go install github.com/bufbuild/buf/cmd/buf@latest` if you don't have one.

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: generate lint test tidy

## generate: regenerate gen/dagworker/v1 from proto/. Byte-for-byte
## reproducible from the committed proto/, buf.yaml and buf.gen.yaml — see
## README.md for why the *output* is committed too. protoc-gen-go's output is
## already gofmt/gofumpt-clean on its own; running a formatter over it here
## would only risk a spurious diff (import-grouping heuristics are not
## perfectly idempotent under repeated application), so this step doesn't.
generate:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	buf lint
	buf generate

## lint: buf's own proto lint plus golangci-lint over the Go sources. Uses
## this module's own .golangci.yml (see that file's header for why).
lint:
	buf lint
	golangci-lint run ./...

## test: the full suite this module's tests must pass under, per the
## project's testing ADR: race detector, shuffled order, run twice to shake
## out order-dependent flakes.
test:
	go test -race -shuffle=on -count=2 ./...

## tidy: keep go.mod/go.sum minimal after touching imports.
tidy:
	go mod tidy
