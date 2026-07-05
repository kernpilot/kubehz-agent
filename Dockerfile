# syntax=docker/dockerfile:1

# ── Build stage ──────────────────────────────────────────────────────────────
# Pinned by digest (supply chain): golang:1.26.4-bookworm, verified against
# Docker Hub on 2026-07-05. Update the tag AND the digest together.
FROM golang:1.26.4-bookworm@sha256:b305420a68d0f229d91eb3b3ed9e519fcf2cf5461da4bef997bf927e8c0bfd2b AS build

WORKDIR /src

# Cache modules separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download && go mod verify

COPY . .

# Version is stamped into the binary (agent.version / User-Agent).
ARG VERSION=dev
# buildx populates these; default to the build host otherwise.
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# CGO off + trimpath + stripped => a fully static, reproducible binary that runs
# on a distroless/static base with no libc.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags="-s -w -X github.com/kernpilot/kubehz-agent/internal/buildinfo.Version=${VERSION}" \
      -o /out/kubehz-agent ./cmd/kubehz-agent

# ── Runtime stage ────────────────────────────────────────────────────────────
# distroless static (no shell, no package manager, minimal CVE surface), running
# as the built-in nonroot user. Pinned by digest (gcr.io, verified 2026-07-05).
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

# OCI metadata (spec §1.5: public, auditable image).
LABEL org.opencontainers.image.title="kubehz-agent" \
      org.opencontainers.image.description="Informer-based live-view agent for the kubehz managed tier (outbound-only)." \
      org.opencontainers.image.source="https://github.com/kernpilot/kubehz-agent" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="kernpilot"

COPY --from=build /out/kubehz-agent /usr/local/bin/kubehz-agent

# 65532 = distroless nonroot uid. Redundant with the base's default, but explicit.
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/kubehz-agent"]
