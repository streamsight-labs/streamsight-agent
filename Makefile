APP     := kafka-metrics-agent
PKG     := ./cmd/agent
BIN     := bin/$(APP)

# Version is stamped into main.version and travels onto every batch as
# Batch.AgentVersion, so a backend can attribute a schema quirk to a build.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

# The mock ingest is a test tool. It ships in no release artifact: the
# Dockerfile and release.yml both build ./cmd/agent by explicit path.
MOCK_PKG     := ./cmd/mock-ingest
MOCK_BIN     := bin/mock-ingest
COMPOSE_HTTP := docker compose -f docker-compose.yml -f docker-compose.http.yml

# Not an override of docker-compose.yml, unlike COMPOSE_HTTP: a different broker
# image with a different storage-format path shares nothing with the level-3
# stack. The compose file names its own project so `down -v` cannot cross over.
COMPOSE_K4   := docker compose -f docker-compose.kafka4.yml

# Pinned to match .github/workflows/ci.yml: golangci-lint adds checks in minor
# releases, so a floating local install disagrees with CI at the worst moment.
GOLANGCI_VERSION := v2.12.2

.PHONY: all build version run run-stdout test test-race vet fmt fmt-check lint cover check clean \
        test-local-up test-local-down test-local-logs test-local-restart test-local-urp \
        build-mock run-mock run-http test-http test-http-up test-http-down \
        test-http-logs test-http-restart test-http-verify \
        test-kafka4 test-kafka4-up test-kafka4-down test-kafka4-logs test-kafka4-restart

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

lint:
	@command -v golangci-lint >/dev/null 2>&1 \
		|| { echo "golangci-lint not installed: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)"; exit 1; }
	golangci-lint run ./...

# Coverage as information, never a gate — the same calibration as CI.
cover:
	go test -race -covermode=atomic -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -n 1

check: fmt-check vet test

clean:
	rm -rf bin/ cover.out

# Local test environment with SASL/ACLs
test-local-up:
	cd test && docker compose up -d

# --remove-orphans, not decoration: docker-compose.urp.yml's second broker is
# not in this file's service list, so a plain `down -v` removes the base
# services, leaves that broker running with nothing to talk to, and then fails
# to remove the network with "Resource is still in use" — which the next
# `up` inherits. Measured, not defensive.
test-local-down:
	cd test && docker compose down -v --remove-orphans

test-local-logs:
	cd test && docker compose logs -f agent

test-local-restart:
	cd test && docker compose restart agent

# The only way to make the agent issue ListPartitionReassignments: the phase is
# trigger-driven, and a single broker whose every topic is replication-factor 1
# never gives it a trigger. The script layers test/docker-compose.urp.yml over
# the level-3 stack for a second broker, forces an under-replicated partition,
# and exits non-zero unless the reassignments section comes back ok under
# nothing but the three DESCRIBE grants. Needs jq; `make test-local-down`
# cleans up.
test-local-urp:
	./test/urp-probe.sh

build-mock:
	go build -o $(MOCK_BIN) $(MOCK_PKG)

# Mock ingest on the host: conformance-checks every batch and prints a summary.
run-mock: build-mock
	$(MOCK_BIN) --addr :8088 --api-key local-dev-key

# Agent on the host in http mode, against `make run-mock` and the plain broker
# from ./docker-compose.yml. Mirrors run / run-stdout.
run-http: build
	KAFKA_BROKERS=localhost:9092 \
	EXPORT_MODE=http \
	EXPORT_ENDPOINT=http://localhost:8088/v1/batches \
	API_KEY=local-dev-key \
	COLLECTION_INTERVAL=10s \
	LOG_LEVEL=debug \
	$(BIN)

# One command: SASL/ACL Kafka + agent in http mode + mock ingest, then follow
# the conformance output. The "show me batches flowing" entry point.
test-http: test-http-up
	cd test && $(COMPOSE_HTTP) logs -f mock-ingest

test-http-up:
	cd test && $(COMPOSE_HTTP) up -d --build

test-http-logs:
	cd test && $(COMPOSE_HTTP) logs -f mock-ingest

test-http-down:
	cd test && $(COMPOSE_HTTP) down -v

test-http-restart:
	cd test && $(COMPOSE_HTTP) restart agent

# Gate: non-zero unless the first MOCK_REQUIRE batches all pass conformance.
# docker wait, not --abort-on-container-exit: kafka-init exits 0 on purpose and
# would abort the run before a single batch arrives.
test-http-verify:
	cd test && MOCK_REQUIRE=$${MOCK_REQUIRE:-5} $(COMPOSE_HTTP) up -d --build; \
	rc=$$(docker wait streamsight-mock-ingest); \
	$(COMPOSE_HTTP) logs mock-ingest; \
	$(COMPOSE_HTTP) down -v >/dev/null 2>&1; \
	exit $$rc

# Kafka 4.1: the KIP-848 and KIP-932 paths cp-kafka 7.5.0 cannot answer. One
# command brings up the broker, raises share.version, starts a classic consumer,
# a new-protocol consumer and a share consumer, and follows the agent watching
# all three. docs/TESTING.md records what came back.
test-kafka4: test-kafka4-up
	cd test && $(COMPOSE_K4) logs -f agent

test-kafka4-up:
	cd test && $(COMPOSE_K4) up -d --build

test-kafka4-logs:
	cd test && $(COMPOSE_K4) logs -f agent

test-kafka4-down:
	cd test && $(COMPOSE_K4) down -v

test-kafka4-restart:
	cd test && $(COMPOSE_K4) restart agent
