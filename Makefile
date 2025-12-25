APP := kafka-metrics-agent

build:
	go build -o bin/$(APP) ./cmd/agent

run: build
	KAFKA_BROKERS=localhost:9092 API_KEY=test ./bin/$(APP)

test:
	go test -v ./...

clean:
	rm -rf bin/

# Local test environment with SASL/ACLs
test-local-up:
	cd test && docker-compose up -d

test-local-down:
	cd test && docker-compose down -v

test-local-logs:
	cd test && docker-compose logs -f agent

test-local-restart:
	cd test && docker-compose restart agent

.PHONY: build run test clean test-local-up test-local-down test-local-logs test-local-restart
