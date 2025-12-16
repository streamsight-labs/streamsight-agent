APP := kafka-metrics-agent

build:
	go build -o bin/$(APP) ./cmd/agent

run: build
	KAFKA_BROKERS=localhost:9092 API_KEY=test ./bin/$(APP)

test:
	go test -v ./...

clean:
	rm -rf bin/

.PHONY: build run test clean
