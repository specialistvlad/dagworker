# syntax=docker/dockerfile:1

# Build stage: cross-compiles a fully static binary for TARGETOS/TARGETARCH,
# so a single `docker buildx build --platform linux/amd64,linux/arm64` (or
# goreleaser's buildx backend) produces every architecture from one Dockerfile.
FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Copy the whole repository, not just this module: dagworkerd depends on the
# core module, both adapters, and all three storage backends by local path
# (go.work), so the build needs the full workspace, not a vendored subset.
COPY . .

RUN cd cmd/dagworkerd && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/dagworkerd .

# Runtime stage: distroless/static, not bare scratch — dagworkerd dials
# outbound to Redis/PostgreSQL, which may be behind TLS, and distroless
# ships CA certificates and an /etc/passwd entry for the nonroot user that
# scratch has neither of (docs/research/15-daemon-packaging-and-ops.md
# Part 2 §2.8). CGO_ENABLED=0 above is what makes this legal at all: neither
# image ships a dynamic linker, so a binary built with cgo would not start.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dagworkerd /dagworkerd
USER nonroot:nonroot
ENTRYPOINT ["/dagworkerd"]
