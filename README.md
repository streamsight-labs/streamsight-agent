# Kafka Metrics Agent

Lightweight Go agent that collects Kafka cluster metrics using the Admin API and outputs JSON to stdout.

## Quick Start

```bash
KAFKA_BROKERS=localhost:9092 API_KEY=your-key ./kafka-metrics-agent
```

## Configuration

**Required:**
```bash
KAFKA_BROKERS=broker1:9092,broker2:9092
API_KEY=your-api-key
```

**Optional (Kafka auth):**
```bash
KAFKA_SASL_MECHANISM=SCRAM-SHA-512   # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
KAFKA_SASL_USERNAME=user
KAFKA_SASL_PASSWORD=pass
KAFKA_TLS_ENABLED=true
```

**Optional (tuning):**
```bash
COLLECTION_INTERVAL=30s   # default: 30s
LOG_LEVEL=info            # default: info
```

## Build

```bash
make build
```

## What It Collects

Every 30 seconds:

- **Cluster** - broker list, controller, cluster ID
- **Topics** - partitions, leaders, ISR, start/end offsets
- **Consumer Groups** - state, members, assignments
- **Offsets** - committed offsets per group/topic/partition

## Sample Output

```json
{
  "collected_at": "2024-01-15T10:30:00Z",
  "collection_ms": 127,
  "cluster": {
    "id": "abc123",
    "controller": 1,
    "broker_count": 3,
    "brokers": [
      {"id": 1, "host": "broker1", "port": 9092}
    ]
  },
  "topics": [
    {
      "name": "orders",
      "partition_count": 6,
      "replication_factor": 3,
      "partitions": [
        {"id": 0, "leader": 1, "replicas": [1,2,3], "isr": [1,2,3], "start_offset": 0, "end_offset": 1500000}
      ]
    }
  ],
  "groups": [
    {
      "id": "order-processor",
      "state": "Stable",
      "coordinator": 2,
      "member_count": 3
    }
  ],
  "offsets": [
    {
      "group_id": "order-processor",
      "offsets": [
        {"topic": "orders", "partition": 0, "offset": 1499850}
      ]
    }
  ]
}
```

## Docker

```bash
docker build -t kafka-metrics-agent .
docker run -e KAFKA_BROKERS=broker:9092 -e API_KEY=key kafka-metrics-agent
```
