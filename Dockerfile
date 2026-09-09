# syntax=docker/dockerfile:1

# The builder always runs on the native build platform and cross-compiles for
# the target platform. A linux/amd64 + linux/arm64 build therefore costs two
# native `go build` invocations rather than a QEMU-emulated Go toolchain per
# architecture.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /src

# Dependencies first: this layer is reused for as long as go.mod/go.sum are
# unchanged, which is most builds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# VERSION is stamped into main.version with exactly the ldflag the Makefile
# uses, and travels on every batch as Batch.AgentVersion so a backend can
# attribute a schema quirk to a build. CI passes the release tag; the "dev"
# default only applies to a bare `docker build`.
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/agent ./cmd/agent

FROM alpine:3.19

ARG VERSION=dev

LABEL org.opencontainers.image.title="streamsight-agent" \
      org.opencontainers.image.description="Kafka metrics collection agent" \
      org.opencontainers.image.source="https://github.com/streamsight-labs/streamsight-agent" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}"

# ca-certificates is needed for TLS to Kafka and for HTTPS export.
RUN apk --no-cache add ca-certificates \
    && adduser -D -H -u 1000 agent \
    && mkdir -p /var/lib/streamsight \
    && chown 1000:1000 /var/lib/streamsight

COPY --from=builder /out/agent /usr/local/bin/agent

# Every dependency compiled into that binary is BSD-3-Clause, and that
# license's second clause requires a binary redistribution to reproduce the
# copyright notices "in the documentation and/or other materials provided with
# the distribution". A published image is such a redistribution, and it is one
# nobody obtains by way of the repository, so the notices have to be inside it.
# .dockerignore denies everything by default and allows this path back in.
COPY THIRD_PARTY_NOTICES.txt /usr/local/share/streamsight-agent/

# In file export mode the agent writes JSONL to EXPORT_FILE. The image default
# points at a directory owned by uid 1000, because the process is not root and
# the container root filesystem is expected to be read-only: mount a writable
# volume (emptyDir in Kubernetes, a bind mount or named volume in Compose) at
# /var/lib/streamsight. Without a mount, writes land in the container's own
# writable layer and are lost when the container is removed.
ENV EXPORT_FILE=/var/lib/streamsight/metrics.jsonl
WORKDIR /var/lib/streamsight

# Numeric, so a Kubernetes runAsNonRoot check can be satisfied without
# resolving /etc/passwd.
USER 1000:1000

ENTRYPOINT ["agent"]
