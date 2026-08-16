APP     := kafka-metrics-agent
PKG     := ./cmd/agent
BIN     := bin/$(APP)

# Version is stamped into main.version and travels onto every batch as
# Batch.AgentVersion, so a backend can attribute a schema quirk to a build.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

.PHONY: all build version run run-stdout test test-race vet fmt fmt-check check clean \
        test-local-up test-local-down test-local-logs test-local-restart

all: check build

build:
	go build -ldflags "$(LDFLAGS)" -o $(BIN) $(PKG)

version: build
	@echo "$(VERSION)"

# Local mode: no endpoint, no API key, no network. Batches land in
# ./metrics.jsonl as one JSON object per line.
run: build
	KAFKA_BROKERS=localhost:9092 \
	EXPORT_MODE=file \
	EXPORT_FILE=./metrics.jsonl \
	COLLECTION_INTERVAL=10s \
	LOG_LEVEL=debug \
	$(BIN)

# Same, but piped to stdout for `| jq`.
run-stdout: build
	@KAFKA_BROKERS=localhost:9092 \
	EXPORT_MODE=stdout \
	COLLECTION_INTERVAL=10s \
	$(BIN)

test:
	go test ./...

test-race:
	go test -race -count=2 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# CI gate: fails if anything is unformatted.
fmt-check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt-check vet test

clean:
	rm -rf bin/

# Local test environment with SASL/ACLs
test-local-up:
	cd test && docker compose up -d

test-local-down:
	cd test && docker compose down -v

test-local-logs:
	cd test && docker compose logs -f agent

test-local-restart:
	cd test && docker compose restart agent
