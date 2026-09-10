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

These are design invariants, not current behaviour that may drift — a change
that breaks one of them is a product decision, not an implementation detail.

**Kafka permissions.** The agent requires **exactly** five read-only grants:

| Resource | Operation | Used for |
|---|---|---|
| `CLUSTER` | `DESCRIBE` | `Metadata` (brokers, controller, cluster ID), `ListGroups`, `DescribeLogDirs` (`COLLECT_LOG_DIRS`, default on), and `ListPartitionReassignments` (issued only on an observed under-replicated partition). `ApiVersions` — the startup probe — is answered before authentication completes and carries no ACL at all |
| `TOPIC` | `DESCRIBE` | Topic and partition metadata; every `ListOffsets` flavour — log start, high watermark, the `read_committed` listing that yields the last stable offset, the by-timestamp throughput window, max timestamp, and the tiered-storage sentinels, all one API key the broker authorizes before reading the isolation level or the timestamp; `OffsetForLeaderEpoch`; and the topics inside `OffsetFetch` |
| `GROUP` | `DESCRIBE` | `DescribeGroups`, `ConsumerGroupDescribe` (KIP-848 groups), `OffsetFetch`, and — when `COLLECT_SHARE_GROUPS` is enabled — `ShareGroupDescribe` and `DescribeShareGroupOffsets` |
| `TOPIC` | `DESCRIBE_CONFIGS` | `DescribeConfigs` for topic configuration (`COLLECT_CONFIGS`, default on) |
| `CLUSTER` | `DESCRIBE_CONFIGS` | `DescribeConfigs` for broker configuration |

Every call the agent makes falls inside those five grants. `ListOffsets` carries an
isolation level that the broker applies only *after* authorization, so reading the last
stable offset needs no grant beyond the one already required for the high watermark.
`DescribeLogDirs` returns directory paths and per-replica byte sizes — never record
content — and the broker gates it on `DESCRIBE` on `CLUSTER`, which is measured rather than
assumed: see below.

Three APIs have had their authorization measured against a live broker with a
non-permissive authorizer. `OffsetForLeaderEpoch` is satisfied by the existing grants.
`DescribeLogDirs` and `ListPartitionReassignments` are too, measured in both directions: as a
principal holding nothing but the three `DESCRIBE` grants the agent's `log_dirs` and
`reassignments` sections both report `ok` — the second on a deliberately under-replicated
partition, because that request fires on no other cycle — and revoking `DESCRIBE` on
`CLUSTER` turns both `unauthorized` with `CLUSTER_AUTHORIZATION_FAILED`. There is no sixth
grant behind either of them. A fourth API, `DescribeProducers`, requires `READ` on `TOPIC`
and is therefore **excluded from this agent by design**, because `READ` on a topic would
permit consuming its records. That requirement is read off Kafka's authorization rules and
is the one claim here that nothing measured: the agent never issues the call, so no broker
has ever been asked to refuse it.

`DESCRIBE_CONFIGS` is the one grant outside the original three DESCRIBE grants. It is
read-only and it is **not** `ALTER_CONFIGS`: it permits reading configuration, never
changing it. It is required rather than optional because what it reads is a correction, not
a feature — see below. A cluster that refuses it can run the agent with
`COLLECT_CONFIGS=false`; the two config sections then report `unauthorized` and no other
section is affected. `DescribeConfigs` names no keys on the wire, so the broker returns every configuration
entry it holds and the agent projects that response onto a fixed 21-key allowlist before
anything is emitted — the filtering is client-side, and nothing outside those 21 keys ever
reaches a batch. It never ships a value the broker marks `SENSITIVE`: Kafka strips those
server-side, and the agent strips them again on the way out rather than trusting that it
always will. Every listener, SASL, SSL and path-shaped
key is excluded from the allowlist, because the agent has no consumer for any of them.

The reason it is required: without `cleanup.policy` nothing can tell that a topic is
compacted, and on a compacted topic the offset range is not a record count — compaction
removes records and leaves the offsets consumed. Consumer lag, the headline number this
agent produces, is then overstated by an unknowable amount and cannot even be flagged as
unreliable. Without `min.insync.replicas` an under-replicated partition cannot be
distinguished from one where every `acks=all` produce is currently failing: same wire data,
opposite severity.

Nothing else. In particular:

- **It never reads record data.** No `READ` on any topic, no consumer, no
  `Fetch` request is ever issued. It cannot see the contents of your messages,
  because the credentials it runs with are not permitted to.
- **It never writes to the cluster.** No `WRITE`, `CREATE`, `DELETE`, `ALTER`,
  or `ALTER_CONFIGS` on any resource. `DESCRIBE_CONFIGS` reads configuration;
  `ALTER_CONFIGS` would change it, and is never requested. It cannot create, modify, or delete a
  topic, a group, a config, or an ACL. It does not commit offsets and does not
  join a consumer group.
- **It is not an admin tool.** No `CLUSTER_ACTION`, no `IDEMPOTENT_WRITE`, no
  broker or controller mutation of any kind.

You can verify this rather than take our word for it: grant the five ACLs
above and nothing else, run the agent, and confirm every section reports `ok`
or `skipped`. `README.md` has the `kafka-acls`, MSK, Confluent Cloud, and
Redpanda forms of those grants, and `test/` brings up a local KRaft cluster
with SASL/SCRAM and `StandardAuthorizer` enforcing them. `make test-local-urp`
adds the one condition that cluster cannot produce on its own — an
under-replicated partition — and fails unless `reassignments` answers under
those grants.

One deliberate difference in that environment: `test/setup-acls.sh` grants only
the three `DESCRIBE` operations, so `topic_configs` and `broker_configs` report
`unauthorized` there. That is the standing test that the two config sections
degrade cleanly and that nothing else degrades with them — not a defect, and
not the configuration a deployment should run.

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
`TOPIC_EXCLUDE` and `GROUP_EXCLUDE` are the supported way to keep those out of
a batch: they are applied in the agent before anything is exported, and an entry
is an exact name unless wrapped in slashes to make it a regex, so a name
containing a dot excludes that topic and no other (README,
[Configuration](README.md#configuration)). Record keys, record values, and
headers are never collected in any mode.

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
beyond the five read-only grants; credential leakage through logs, exported
batches, or the container image; remote code execution or memory corruption
reachable from broker responses or ingest responses; a crafted ingest response
that causes the agent to misbehave against your brokers.

Out of scope: vulnerabilities in Kafka itself, in your broker configuration, or
in the network path; findings that require an attacker who already has the
agent's credentials or a shell in its container; missing hardening that has no
demonstrated impact; and DoS achieved by pointing the agent at a cluster with a
pathological entity count — a known and documented limitation: nothing truncates
the inventory by design, so the only bound on batch size is the selection
surface, which is an operator decision the payload echoes back in `selection`
(README, [Cardinality caps](README.md#cardinality-caps)).
