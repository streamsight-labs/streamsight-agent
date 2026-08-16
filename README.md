# streamsight-agent

A read-only Kafka metrics collector. Every cycle it issues a handful of Admin API
requests, turns the answers into one JSON object (a *batch*), and ships that batch
to a local file, an HTTP endpoint, or stdout.

It performs **no writes**: no produce, no consume, no topic or config mutation. The
entire permission requirement is `DESCRIBE` on `CLUSTER`, `TOPIC` and `GROUP`.

## Quick start (no endpoint, no API key)

The default export mode is a local JSONL file. Nothing else is required:

```bash
make build
KAFKA_BROKERS=localhost:9092 ./bin/kafka-metrics-agent
```

That writes one JSON object per line to `./metrics.jsonl`, one line per collection
cycle, and logs to stderr. To watch it:

```bash
tail -f metrics.jsonl | jq '{seq: .batch_seq, topics: (.topics|length), sections: [.sections[]|{name,status}]}'
```

Or pipe batches straight to another process:

```bash
KAFKA_BROKERS=localhost:9092 EXPORT_MODE=stdout ./bin/kafka-metrics-agent | jq .
```

No Kafka to hand? `docker compose up -d` starts a single-node broker plus the agent
in file mode, and publishes the broker on `localhost:9092` so the commands above
work against it.

```bash
docker compose up -d
docker compose exec agent tail -f /var/lib/streamsight/metrics.jsonl
```

Shipping to a collector instead is a two-variable change:

```bash
KAFKA_BROKERS=localhost:9092 \
EXPORT_ENDPOINT=https://ingest.example.com/v1/batches \
API_KEY=... \
./bin/kafka-metrics-agent
```

## Export modes

`EXPORT_MODE` is `file`, `http` or `stdout`. If it is unset, the agent picks `http`
when `EXPORT_ENDPOINT` is set and `file` otherwise — so adding an endpoint is the
only step needed to go from local to remote.

| Mode | Destination | Needs a credential | Delivery |
|------|-------------|--------------------|----------|
| `file` (default) | `EXPORT_FILE`, one JSON object per line | no | synchronous, buffer flushed every batch |
| `stdout` | stdout, same framing | no | synchronous, one `write` per batch |
| `http` | `POST EXPORT_ENDPOINT` | `API_KEY` | bounded queue + single worker, retried |

### File mode

* One batch per line, newline-terminated. `json.Marshal` escapes embedded newlines,
  so a line is always exactly one record — `tail -f | jq -c .` never sees a partial
  object.
* The parent directory is created if missing; the file is opened `O_APPEND` with
  mode `0644` and reopened on restart, so a restart appends rather than truncates.
* The buffer is flushed after every batch. A `SIGKILL` loses at most the batch
  being written; there is no `fsync`, so a power loss can lose more.
* Rotation is by size. At `EXPORT_FILE_MAX_MB` the agent renames
  `metrics.jsonl` → `metrics.jsonl.1`, shifting `.1` → `.2` … and deleting
  `.<EXPORT_FILE_MAX_BACKUPS>`. Rotation happens *before* the write, so a batch is
  never split across two files. A negative `EXPORT_FILE_MAX_MB` disables rotation
  entirely (unbounded growth); `EXPORT_FILE_MAX_BACKUPS=0` keeps no old files.
* Write, flush and rotation failures are returned, logged, counted in
  `agent.batches_dropped` and surfaced in the next batch's `agent.last_export_error`.

**Pointing a log shipper at it.** The file is plain JSONL with no wrapper, so any
shipper that reads lines and parses JSON works unchanged. Rotation is rename-based,
so configure the shipper to follow the inode/path pair and pick up the new file:

```yaml
# Vector
sources:
  streamsight:
    type: file
    include: ["/var/lib/streamsight/metrics.jsonl"]
    read_from: beginning
```

```yaml
# Filebeat
filebeat.inputs:
  - type: filestream
    paths: ["/var/lib/streamsight/metrics.jsonl"]
    parsers:
      - ndjson: {target: ""}
```

Keep the shipper's own retention above `EXPORT_FILE_MAX_MB × (EXPORT_FILE_MAX_BACKUPS + 1)`
or a slow shipper will miss records that rotation has already deleted. In `stdout`
mode the same JSONL arrives on the container's stdout, which is the simpler option
under Docker or Kubernetes since the runtime already collects it — agent logs go to
stderr precisely so stdout stays parseable.

### HTTP mode

* One batch per `POST`, body is the same single JSON object, `Content-Type:
  application/json`.
* Headers: `X-API-Key`, and `Idempotency-Key: <agent_instance_id>-<batch_seq>` so a
  delivered-but-unacknowledged batch can be deduplicated by the receiver.
* `EXPORT_GZIP` (default on) compresses the body and sets `Content-Encoding: gzip`.
* Retries: `408`, `429` and every `5xx` are retried up to `EXPORT_MAX_RETRIES` with
  full-jitter backoff (uniform in `[0, EXPORT_BASE_DELAY << (n-1)]`, capped at 30s).
  A `Retry-After` header overrides that delay, clamped to 5 minutes. Every other
  `4xx` is terminal and counted as `batches_rejected` — a rejected batch is a
  configuration or schema problem, not a transient one.
* The queue is bounded by `EXPORT_QUEUE_SIZE` and the enqueue never blocks:
  collection must not stall behind a slow ingest, so an overflowing queue drops the
  batch and counts it in `agent.batches_dropped`.
* On `SIGTERM` the agent stops accepting batches, drains the queue, and cancels an
  in-flight request after a grace period of `2 × EXPORT_TIMEOUT`. A batch that is
  mid-backoff when shutdown starts is abandoned and counted as dropped.

## Configuration

Everything is environment variables, validated at startup. The agent reports *all*
configuration problems at once and exits non-zero — one restart per fix, not five.

### Required

| Variable | Notes |
|----------|-------|
| `KAFKA_BROKERS` | Comma-separated `host:port`. Blank entries are dropped; a value with no usable entry is an error. |

### Export

| Variable | Default | Required when | Notes |
|----------|---------|---------------|-------|
| `EXPORT_MODE` | `http` if `EXPORT_ENDPOINT` is set, else `file` | — | `file` \| `http` \| `stdout`. Any other value is an error. |
| `EXPORT_ENDPOINT` | — | `EXPORT_MODE=http` | Full URL that batches are POSTed to. |
| `API_KEY` | — | `EXPORT_MODE=http` | Sent as `X-API-Key`. Never required in file/stdout mode. |
| `EXPORT_FILE` | `./metrics.jsonl` (`/var/lib/streamsight/metrics.jsonl` in the image) | file mode | Parent directories are created. |
| `EXPORT_FILE_MAX_MB` | `100` | file mode | `0` uses the 100MB default; negative disables rotation. |
| `EXPORT_FILE_MAX_BACKUPS` | `3` | file mode | `0` keeps none. Negative is an error. |
| `EXPORT_QUEUE_SIZE` | `100` | http mode | Must be > 0. Overflow drops the batch. |
| `EXPORT_MAX_RETRIES` | `3` | http mode | Must be >= 0. |
| `EXPORT_BASE_DELAY` | `1s` | http mode | Must be > 0. Backoff base. |
| `EXPORT_TIMEOUT` | `10s` | http mode | Must be > 0. Per-request deadline. |
| `EXPORT_GZIP` | `true` | http mode | |

### Collection

| Variable | Default | Notes |
|----------|---------|-------|
| `COLLECTION_INTERVAL` | `30s` | Must parse and be > 0. The first cycle runs immediately, not after one interval. |
| `COLLECTION_TIMEOUT` | 80% of `COLLECTION_INTERVAL` (`24s`) | Per-cycle deadline. Exceeding the interval is a startup warning, not an error. |
| `INCLUDE_INTERNAL_TOPICS` | `false` | Includes topics the broker flags internal (`__consumer_offsets`, `__transaction_state`). |
| `TOPIC_INCLUDE_REGEX` | `""` (all) | Compiled at startup; a bad pattern fails the process. |
| `TOPIC_EXCLUDE_REGEX` | `""` | Exclude wins over include. |
| `GROUP_INCLUDE_REGEX` | `""` | |
| `GROUP_EXCLUDE_REGEX` | `""` | |

Regexes are **unanchored**: `orders` also matches `prod.orders`. Write `^orders$` for
an exact match. Group IDs beginning with `__` are always skipped.

### Kafka authentication

| Variable | Default | Notes |
|----------|---------|-------|
| `KAFKA_SASL_MECHANISM` | `""` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Anything else is an error. |
| `KAFKA_SASL_USERNAME` | — | Required iff a mechanism is set. |
| `KAFKA_SASL_PASSWORD` | — | Required iff a mechanism is set. Redacted from all logs. |
| `KAFKA_TLS_ENABLED` | `false` | System root CAs, verified certificates, TLS 1.2 floor. There is no "skip verification" knob. |

### Agent

| Variable | Default | Notes |
|----------|---------|-------|
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error`. Logs go to **stderr**. |
| `AGENT_INSTANCE_ID` | `<hostname>-<8 hex>` | Travels on every batch. Set it explicitly to something stable (e.g. the pod name) if the backend keys on it; the default's random suffix changes on every restart. |

## What it collects

Per cycle, from the Admin API only:

* **Cluster** — cluster ID, controller, broker list (id, host, port, rack).
* **Topics** — partition count, replication factor, and per partition: leader, leader
  epoch, replicas, ISR, offline replicas, log start offset, high watermark.
* **Groups** — state, coordinator, protocol, members (client ID, host, static instance
  ID, subscribed topics, assignment, owned partitions).
* **Offsets** — committed offset per group / topic / partition.
* **Agent** — its own uptime, batch counters, queue depth, last export error.
* **Sections** — per-phase status, sample timestamp and duration.

Requests per cycle: one `Metadata`, one `ListGroups` (broadcast to every broker), one
`DescribeGroups` and one `OffsetFetch` covering all groups — both sharded across the
group coordinators by the client — and `ListOffsets` twice, for start and end offsets,
each preceded by a metadata request the Kafka client library issues internally.

Not obtainable this way, and therefore absent: request latency, bytes in/out, broker
CPU and disk. Those need JMX. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
for what is planned next and what it would cost.

## Output

One batch per cycle. Real output from the local test environment, abridged (long
partition and member lists cut):

```json
{
  "schema_version": 1,
  "agent_version": "v0.2.0",
  "agent_instance_id": "agent-7c9f4d2b",
  "batch_seq": 7,
  "collected_at": "2026-08-16T12:22:40.090740553Z",
  "collection_ms": 12,
  "cluster": {
    "id": "MkU3OEVBNTcwNTJENDM2Qg",
    "controller": 1,
    "broker_count": 1,
    "brokers": [{"id": 1, "host": "kafka", "port": 9092}]
  },
  "topics": [
    {
      "name": "orders",
      "id": "furP32/eSvyEmz61ycxI+A==",
      "internal": false,
      "partition_count": 3,
      "replication_factor": 1,
      "partitions": [
        {"id": 0, "leader": 1, "leader_epoch": 0, "replicas": [1], "isr": [1],
         "start_offset": 0, "end_offset": 98},
        {"id": 1, "leader": 1, "leader_epoch": 0, "replicas": [1], "isr": [1],
         "start_offset": 0, "end_offset": 596}
      ]
    }
  ],
  "groups": [
    {
      "id": "order-processor",
      "state": "Stable",
      "coordinator": 1,
      "generation": -1,
      "protocol": "range",
      "protocol_type": "consumer",
      "member_count": 1,
      "members": [
        {
          "member_id": "console-consumer-9443e6f3-0ced-443c-9ccc-460c3a9e5fb3",
          "client_id": "console-consumer",
          "host": "/172.20.0.2",
          "subscribed_topics": ["orders"],
          "assignment": [
            {"topic": "orders", "partition": 0},
            {"topic": "orders", "partition": 1}
          ]
        }
      ]
    }
  ],
  "offsets": [
    {
      "group_id": "order-processor",
      "offsets": [
        {"topic": "orders", "partition": 0, "offset": 98},
        {"topic": "orders", "partition": 1, "offset": 596}
      ]
    }
  ],
  "agent": {
    "uptime_sec": 60,
    "batches_collected": 7,
    "batches_exported": 6,
    "batches_dropped": 0,
    "batches_rejected": 0,
    "export_retries": 0,
    "queue_depth": 0
  },
  "sections": [
    {"name": "cluster", "status": "ok", "sampled_at": "2026-08-16T12:22:40.090245298Z", "duration_ms": 4},
    {"name": "topics",  "status": "ok", "sampled_at": "2026-08-16T12:22:40.095159048Z", "duration_ms": 7},
    {"name": "groups",  "status": "ok", "sampled_at": "2026-08-16T12:22:40.090288923Z", "duration_ms": 6},
    {"name": "offsets", "status": "ok", "sampled_at": "2026-08-16T12:22:40.096296465Z", "duration_ms": 2}
  ]
}
```

`schema_version` is bumped on any backwards-incompatible change to this shape.

### Reading the data correctly

Four things change how a consumer of this JSON must be written. Ignoring them
produces confidently wrong dashboards.

**1. Offsets are nullable, and `null` never means zero.**
`start_offset`, `end_offset` and a group's `offset` are `*int64`. `null` means *the
agent did not learn this number in this cycle* — an unreachable leader, a partition
missing from the response, or (for a commit) a group that has never committed to that
partition. An empty partition serializes as `0`; an unknown one serializes as `null`.
Never coerce `null` to `0`: doing so reports the entire retained backlog as lag for
every newly created or reset consumer group, and reports an offline partition as
empty. Any arithmetic on a `null` must produce "unknown", not a number.

**2. `end_offset` is the high watermark, not the last stable offset.**
On a topic written by transactional producers it counts aborted records and commit
markers. A `read_committed` consumer therefore shows a small permanent residual lag
against it by construction. That is correct arithmetic on the wrong number, not a
stuck consumer; alert thresholds on transactional topics need slack.

**3. Consumer lag is `end_offset - offset`, computed by the consumer of this data.**
The agent ships raw offsets and does not compute lag, because lag is only meaningful
if you know both halves were sampled and when. Within a batch, committed offsets are
sampled **before** end offsets on purpose: the skew between them then makes lag err
slightly high rather than going negative, so a caught-up consumer never reports `-3`.
Use `sections[].sampled_at` for the phase, not `collected_at`, when differencing
offsets across batches to get a rate.

**4. `sections` distinguishes "nothing there" from "we could not look".**
A section is a collection phase: `cluster`, `topics`, `groups`, `offsets`. Status is
one of:

| Status | Meaning |
|--------|---------|
| `ok` | The phase completed. An empty `topics`/`groups` list really means none exist. |
| `partial` | Some entities failed. The list is truthful but incomplete; see `errors`. |
| `failed` | The phase produced nothing usable. Absence carries no information. |
| `unauthorized` | Same as failed, with an actionable cause: a missing ACL. |
| `skipped` | The phase never ran (topics is skipped when cluster metadata failed). |

`topics`, `groups` and `offsets` are `null` rather than `[]` when a phase produced
nothing. **Absence of an entity is only meaningful when its section is `ok`.** Treat
anything else as "no data" and hold the previous value rather than drawing a drop to
zero. In particular, a group whose whole `OffsetFetch` failed is *omitted* from
`offsets` and recorded as an error, so a missing group under a non-`ok` offsets
section must not be read as "committed nothing".

Every failure is attributed in `errors[]`, which is omitted when empty. Deleting the
group ACL from the local test cluster produces exactly this:

```json
"sections": [
  {"name": "groups",  "status": "unauthorized", "duration_ms": 10, "error_count": 1},
  {"name": "offsets", "status": "partial",      "duration_ms": 4,  "error_count": 1}
],
"errors": [
  {"section": "groups",  "api": "DescribeGroups", "kafka_error_code": 30,
   "kind": "authorization", "message": "GROUP_AUTHORIZATION_FAILED: ..."},
  {"section": "offsets", "api": "OffsetFetch", "group": "order-processor",
   "kafka_error_code": 30, "kind": "authorization", "message": "GROUP_AUTHORIZATION_FAILED: ..."}
]
```

`kind` is a coarse routing class — `authorization` (a human must fix an ACL),
`coordinator` (usually self-healing), `transport`, `unsupported`, `other` — so alerts
can be routed without parsing `message`. Errors also carry `broker_id`, `topic`,
`partition` and `group` where the failure was attributable to one.

### Agent self-telemetry

`agent` exists so that "the cluster is quiet", "the agent is wedged" and "the ingest
is rejecting us" are distinguishable at the backend. `batches_dropped` counts queue
overflow, write failures and exhausted retries; `batches_rejected` counts terminal
`4xx`. `last_export_error` is sticky — it is never cleared by a later success, so
compare counters, not its presence. The counters are read just before the batch is
handed to the exporter, so `batches_exported` is always at least one behind
`batches_collected`.

### Known gaps in the current schema

Verified against the code and a live broker; listed so a consumer is not surprised:

* With `INCLUDE_INTERNAL_TOPICS=true`, internal topics arrive with
  `start_offset`/`end_offset` of `null` and the `topics` section reports `partial`.
  The offset listing path filters internal topics out beneath the agent.
* `groups[].generation` is `-1` for consumers whose join metadata does not carry a
  generation, which includes the standard Java consumer with both the `range` and
  `cooperative-sticky` assignors. Do not read `-1` as a rebalance count.
* `offsets[].leader_epoch` is omitted from the JSON when it is `0`, because the field
  is `omitempty`. Absent and zero are indistinguishable there.

## Security

### Why SASL and ACLs

Without authentication any client can do anything to the cluster. The agent is
read-only in code, but only the broker can enforce that. Enable SASL (who are you)
and ACLs (what may you do), then grant the agent the three DESCRIBE permissions
below and nothing else.

### Required permissions

| Resource | Operation | Used by |
|----------|-----------|---------|
| `CLUSTER` | `DESCRIBE` | `Metadata` (brokers, controller, cluster ID) and `ListGroups` |
| `TOPIC` | `DESCRIBE` | Topic and partition metadata, `ListOffsets` (start and end), and the topics inside `OffsetFetch` |
| `GROUP` | `DESCRIBE` | `DescribeGroups` and `OffsetFetch` |

That is the complete set. **This release adds no new ACL requirement** — the reworked
collector issues the same five APIs as before, just in a different order and once
instead of twice. The set was verified end to end against a broker with
`allow.everyone.if.no.acl.found=false` and exactly these three grants (see
[Local testing](#local-testing)); removing any one of them degrades the corresponding
section to `unauthorized` rather than silently emptying it.

### Setup script

```bash
# On a broker, or anywhere with the Kafka CLI tools on PATH
./scripts/setup-kafka-user.sh -b localhost:9092 -p <agent-password>

# Or through a Kafka image, no local install
docker run --rm -v "$PWD/scripts:/scripts:ro" --network host \
  confluentinc/cp-kafka:7.5.0 \
  /scripts/setup-kafka-user.sh -b localhost:9092 -p <agent-password>
```

The script creates the SCRAM credential and the three ACLs. It handles both CLI
naming conventions (`kafka-acls.sh` in the Apache tarball, `kafka-acls` in the
Confluent and Bitnami images). Options:

```
-b  bootstrap server (default localhost:9092)
-u  username (default streamsight-agent)
-p  password (required)
-m  SCRAM-SHA-512 (default) or SCRAM-SHA-256
-c  properties file with the ADMIN credentials the CLI itself uses
-n  dry run: print the commands instead of running them
```

On a secured cluster `-c` is not optional — without admin credentials the CLI cannot
authenticate to create anything.

### Manual setup

```bash
# 1. SASL credential
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config 'SCRAM-SHA-512=[password=<agent-password>]' \
  --entity-type users --entity-name streamsight-agent

# 2. The three read-only ACLs
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --cluster

kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --topic '*' --resource-pattern-type literal

kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:streamsight-agent \
  --operation DESCRIBE --group '*' --resource-pattern-type literal
```

`--resource-pattern-type literal` with the name `*` is Kafka's wildcard and is the
default pattern type. Do not use `prefixed` here: `--topic '*' --resource-pattern-type
prefixed` is a prefix match on the asterisk *character*, which matches only topics
whose names begin with `*` — i.e. nothing, and the agent will report `unauthorized`
sections while `kafka-acls --list` appears to show a grant.

Verify:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --list --principal User:streamsight-agent
```

### Managed Kafka services

**Confluent Cloud**

```bash
confluent iam service-account create streamsight-agent --description "Metrics agent"
confluent api-key create --service-account sa-xxxxx --resource lkc-xxxxx

confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --cluster-scope
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --topic '*'
confluent kafka acl create --allow --service-account sa-xxxxx \
  --operations DESCRIBE --consumer-group '*'
```

Use the API key as `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` with
`KAFKA_SASL_MECHANISM=PLAIN` and `KAFKA_TLS_ENABLED=true`.

**AWS MSK** (SASL/SCRAM)

```bash
aws secretsmanager create-secret --name AmazonMSK_streamsight-agent \
  --secret-string '{"username":"streamsight-agent","password":"<agent-password>"}'

aws kafka batch-associate-scram-secret --cluster-arn <arn> --secret-arn-list <secret-arn>
# then apply the three ACLs above with kafka-acls.sh from a client with access
```

MSK IAM auth is a different mechanism and is not supported by the agent; use
SASL/SCRAM.

**Aiven**

```bash
aiven service user-create <service> --username streamsight-agent
# Console: Service -> Users -> Add User, then Service -> ACLs for the DESCRIBE grants
```

**Azure Event Hubs**

```bash
# Create a SAS policy with Listen only, and use the connection string as the password
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME='$ConnectionString'
KAFKA_SASL_PASSWORD='Endpoint=sb://...;SharedAccessKey=...'
KAFKA_TLS_ENABLED=true
```

**Redpanda**

```bash
rpk security user create streamsight-agent -p <agent-password>
rpk security acl create --allow-principal User:streamsight-agent --operation describe --cluster
rpk security acl create --allow-principal User:streamsight-agent --operation describe --topic '*'
rpk security acl create --allow-principal User:streamsight-agent --operation describe --group '*'
```

## Deployment

**Docker**

```bash
docker run --rm \
  -e KAFKA_BROKERS=broker:9092 \
  -v streamsight-data:/var/lib/streamsight \
  ghcr.io/streamsight-labs/streamsight-agent:latest
```

The image runs as uid 1000 with a read-only root filesystem in mind, and defaults
`EXPORT_FILE` to `/var/lib/streamsight/metrics.jsonl`. Mount something writable
there, or use `-e EXPORT_MODE=stdout` and let the container runtime collect the
batches.

**Kubernetes** — Helm chart in `charts/streamsight-agent`, plain manifests in
`deploy/k8s`. Run one replica: two agents collect and ship every batch twice. Shard
by topic/group filters across separate releases if one agent is not enough.

## Local testing

`docker compose up -d` at the repo root is the plain path: one broker, no auth, agent
in file mode.

To exercise the real security posture — SASL/SCRAM plus ACLs, with
`allow.everyone.if.no.acl.found=false` — use the environment under `test/`:

```bash
make test-local-up      # broker, SCRAM credential, the three ACLs, seed data, agent
make test-local-logs    # agent stderr plus one JSON batch per line (stdout mode)
make test-local-down    # stop and delete volumes
```

It provisions the agent's credential and ACLs exactly as `scripts/setup-kafka-user.sh`
would, seeds a topic and a lagging consumer group, and runs the agent with nothing
but those three DESCRIBE grants. Removing one of them from the running broker is the
quickest way to see how a degraded section is reported:

```bash
docker compose -f test/docker-compose.yml exec kafka \
  kafka-acls --bootstrap-server localhost:9092 --command-config /tmp/admin.properties \
  --remove --force --allow-principal User:streamsight-agent \
  --operation DESCRIBE --group '*' --resource-pattern-type literal
```

Because SCRAM credentials in KRaft live in the metadata log, the bootstrap admin
credential is written by `kafka-storage format --add-scram` before the broker starts;
`test/docker-compose.yml` overrides the image entrypoint to do that.

## Build and develop

```bash
make build        # bin/kafka-metrics-agent, version stamped from git describe
make run          # build and run in local file mode against localhost:9092
make run-stdout   # same, batches on stdout for | jq
make test         # go test ./...
make test-race    # go test -race -count=2 ./...
make check        # gofmt check, go vet, go test
```

## Roadmap

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the next collection
targets, what each costs in requests and permissions, and what is not reachable
through the Admin API at all.
