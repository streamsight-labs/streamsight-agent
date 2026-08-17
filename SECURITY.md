# Security Policy

## Reporting a vulnerability

Report privately, not in a public issue.

Use [GitHub private vulnerability reporting][gh-pvr] — the *Report a
vulnerability* button under this repository's **Security** tab. It is the only
private channel; there is deliberately no advertised email address rather than
one nobody reads.

If that button is unavailable to you, open a public issue whose entire body is
a request for a private channel — no reproduction, no affected version, no
detail of any kind — and it will be answered with one.

Please include the agent version (`agent -version`, or `agent_version` from any
exported batch), how the agent is deployed (Helm chart, plain manifests,
container, bare binary), and the smallest reproduction you have.

What to expect:

| | |
|---|---|
| Acknowledgement | within 72 hours |
| Assessment and severity | within 7 days |
| Fix or documented mitigation | target 90 days from acknowledgement |
| Public disclosure | coordinated, after a fix is available, with credit unless you'd rather not have it |

[gh-pvr]: https://docs.github.com/code-security/security-advisories/guidance-on-reporting-and-writing/privately-reporting-a-security-vulnerability

## Supported versions

There is no tagged release yet. Until `v0.1.0`, security fixes land on `main`
only. After it, the latest minor is supported.

## What this agent can and cannot do

This is the part a security team evaluating whether to run the binary against a
production Kafka cluster usually wants first. These are design invariants, not
current behaviour that may drift — a change that breaks one of them is a
product decision, not an implementation detail.

**Kafka permissions.** The agent requires **exactly** three grants:

| Resource | Operation | Used for |
|---|---|---|
| `CLUSTER` | `DESCRIBE` | `Metadata` (brokers, controller, cluster ID), `ListGroups`, `ApiVersions`, and `DescribeLogDirs` when `COLLECT_LOG_DIRS` is enabled |
| `TOPIC` | `DESCRIBE` | Topic and partition metadata, `ListOffsets` (log start, high watermark, and the `read_committed` listing that yields the last stable offset), and the topics inside `OffsetFetch` |
| `GROUP` | `DESCRIBE` | `DescribeGroups` and `OffsetFetch` |

Every call the agent makes falls inside those three grants. `ListOffsets` carries an
isolation level that the broker applies only *after* authorization, so reading the last
stable offset needs no grant beyond the one already required for the high watermark.
`DescribeLogDirs` returns directory paths and per-replica byte sizes — never record
content — and the broker gates it on `DESCRIBE` on `CLUSTER`.

Two APIs were evaluated and their authorization measured against a live broker with a
non-permissive authorizer. `OffsetForLeaderEpoch` is satisfied by the existing grants.
`DescribeProducers` requires `READ` on `TOPIC` and is therefore **excluded from this
agent by design**, because `READ` on a topic would permit consuming its records.

Nothing else. In particular:

- **It never reads record data.** No `READ` on any topic, no consumer, no
  `Fetch` request is ever issued. It cannot see the contents of your messages,
  because the credentials it runs with are not permitted to.
- **It never writes to the cluster.** No `WRITE`, `CREATE`, `DELETE`, `ALTER`,
  or `ALTER_CONFIGS` on any resource. It cannot create, modify, or delete a
  topic, a group, a config, or an ACL. It does not commit offsets and does not
  join a consumer group.
- **It is not an admin tool.** No `CLUSTER_ACTION`, no `IDEMPOTENT_WRITE`, no
  broker or controller mutation of any kind.

You can verify this rather than take our word for it: grant the three ACLs
above and nothing else, run the agent, and confirm every section reports `ok`.
`README.md` has the `kafka-acls`, MSK, Confluent Cloud, and Redpanda forms of
those grants, and `test/` brings up a local KRaft cluster with SASL/SCRAM and
`StandardAuthorizer` that enforces exactly this set.

**Data leaving your network.** In `file` and `stdout` export modes the agent
opens no outbound connection except to your brokers. In `http` mode it POSTs
one JSON batch per collection cycle to the single endpoint you configure in
`EXPORT_ENDPOINT`, and nowhere else. There is no telemetry, no update check, no
crash reporter, and no third-party SDK in the binary. The whole module graph is
`franz-go` and `franz-go/pkg/kadm` plus what they pull in — `kmsg`,
`klauspost/compress`, `pierrec/lz4`, and `golang.org/x/crypto` — and everything
else is the Go standard library. `go.mod` is the full list, and it is short on
purpose.

**What is in a batch.** Cluster, broker, topic, partition, consumer group, and
member *metadata*, plus committed offsets and log-end offsets — that is,
numbers and names. Names are not necessarily non-sensitive: topic names, group
names, client IDs, and consumer group `metadata` blobs are included verbatim,
and organisations do sometimes put customer identifiers in a topic name.
`TOPIC_EXCLUDE_REGEX` and `GROUP_EXCLUDE_REGEX` (compiled at startup, applied
in the agent before anything is exported) are the supported way to keep those
out of a batch. Record keys, record values, and headers are never collected in
any mode.

**Credentials.** `KAFKA_SASL_PASSWORD` and `API_KEY` are read from the
environment, are held only in memory, and are redacted from every log line and
from the startup config dump (`config.Redacted()`). They are never written to
the export file and never appear in a batch. On Kubernetes, supply them from a
`Secret` — the chart does this by default.

**Runtime posture.** The agent exposes **no listening socket** — no HTTP
server, no metrics endpoint, no pprof, no health probe. There is nothing to
reach it on. The published image runs as numeric UID `1000`, and the chart sets
`readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, and
`capabilities: drop: [ALL]`.

## Scope

In scope: anything that lets the agent read, write, or damage cluster state
beyond the three DESCRIBE grants; credential leakage through logs, exported
batches, or the container image; remote code execution or memory corruption
reachable from broker responses or ingest responses; a crafted ingest response
that causes the agent to misbehave against your brokers.

Out of scope: vulnerabilities in Kafka itself, in your broker configuration, or
in the network path; findings that require an attacker who already has the
agent's credentials or a shell in its container; missing hardening that has no
demonstrated impact; and DoS achieved by pointing the agent at a cluster with a
pathological entity count (a known and documented limitation — see the
cardinality notes in `docs/ARCHITECTURE.md`).
