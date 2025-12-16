FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION}" -o /agent ./cmd/agent

# Runtime
FROM alpine:3.19

LABEL org.opencontainers.image.source="https://github.com/streamsight/agent"
LABEL org.opencontainers.image.description="Kafka metrics agent"

RUN apk --no-cache add ca-certificates
COPY --from=builder /agent /usr/local/bin/agent

RUN adduser -D -u 1000 agent
USER agent

ENTRYPOINT ["agent"]
