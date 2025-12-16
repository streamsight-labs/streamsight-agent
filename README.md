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

## Security

### Why SASL + ACLs are required

Without authentication, **any client can do anything** to your Kafka cluster - produce, consume, delete topics, etc. The agent is safe by design (read-only code), but there's no enforcement without Kafka-side security.

**Always enable for production:**
- **SASL** - authenticates clients (who are you?)
- **ACLs** - authorizes operations (what can you do?)

### Quick Setup

The setup script requires Kafka CLI tools. Run it from:

**Option A: On a Kafka broker (tools already installed)**
```bash
./scripts/setup-kafka-user.sh -b localhost:9092 -p your-secret-password
```

**Option B: Using Docker (no local install needed)**
```bash
docker run --rm -v $(pwd)/scripts:/scripts --network host \
  confluentinc/cp-kafka:7.5.0 \
  /scripts/setup-kafka-user.sh -b localhost:9092 -p your-secret-password
```

**Option C: From Kubernetes (if Kafka is in K8s)**
```bash
kubectl exec -it kafka-0 -- /bin/bash
# Then run the commands manually (see Manual Setup below)
```

This creates user `streamsight-agent` with minimal DESCRIBE-only permissions.

**Script options:**
```bash
./scripts/setup-kafka-user.sh \
  -b kafka.example.com:9092 \    # Bootstrap server
  -u streamsight-agent \          # Username (default)
  -p your-password \              # Password (required)
  -m SCRAM-SHA-512 \              # Mechanism (default)
  -c /path/to/admin.properties    # Admin credentials (if needed)
```

### Manual Setup

```bash
# 1. Create SASL user
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config 'SCRAM-SHA-512=[password=your-password]' \
  --entity-type users --entity-name streamsight-agent

# 2. Grant read-only ACLs
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --cluster

kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --topic '*'

kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --group '*'
```

### Required Permissions

| Resource | Operation | Purpose |
|----------|-----------|---------|
| CLUSTER | DESCRIBE | Broker metadata, cluster ID |
| TOPIC | DESCRIBE | Topic/partition info, offsets |
| GROUP | DESCRIBE | Consumer group state, members |

The agent performs **no writes**. It cannot produce, consume, create topics, or modify any Kafka resources.

### Managed Kafka Services

Each provider has its own way to create users and ACLs:

**Confluent Cloud**
```bash
# Create service account
confluent iam service-account create streamsight-agent --description "Metrics agent"

# Create API key
confluent api-key create --service-account sa-xxxxx --resource lkc-xxxxx

# Grant read-only ACLs
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --cluster-scope
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --topic '*'
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --consumer-group '*'
```

**AWS MSK**
```bash
# For SASL/SCRAM: Create secret in AWS Secrets Manager
aws secretsmanager create-secret --name AmazonMSK_streamsight-agent \
  --secret-string '{"username":"streamsight-agent","password":"your-password"}'

# Associate with MSK cluster
aws kafka batch-associate-scram-secret --cluster-arn <arn> \
  --secret-arn-list <secret-arn>

# ACLs via kafka-acls.sh from a client machine with access
```

**Aiven**
```bash
# Via Aiven CLI
aiven service user-create <service> --username streamsight-agent

# Via Console: Service → Users → Add User → Set ACLs to read-only
# Or use Aiven Terraform provider
```

**Azure Event Hubs**
```bash
# Create SAS policy with "Listen" (read) permission only
# Use connection string as SASL/PLAIN password:
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME='$ConnectionString'
KAFKA_SASL_PASSWORD='Endpoint=sb://...;SharedAccessKey=...'
```

**Redpanda Cloud**
```bash
# Via rpk or Console
rpk security user create streamsight-agent --password <pass>
rpk security acl create --allow-principal User:streamsight-agent \
  --operation DESCRIBE --cluster
rpk security acl create --allow-principal User:streamsight-agent \
  --operation DESCRIBE --topic '*'
rpk security acl create --allow-principal User:streamsight-agent \
  --operation DESCRIBE --group '*'
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
