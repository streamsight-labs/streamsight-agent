# Testing

Six levels, each proving something the one below it cannot. Run them in order the first
time; after that, run the level that matches what you changed.

| Level | Needs | Proves |
|---|---|---|
| [1. Unit tests](#1-unit-tests) | Go toolchain | Logic, the wire contract, the schema shape |
| [2. One broker](#2-one-broker-no-auth) | Docker | It talks to a real Kafka and produces batches |
| [3. The permission model](#3-the-permission-model-sasl--acls) | Docker | It works inside the ACLs the product promises |
| [4. HTTP export](#4-http-export-against-the-conformance-mock) | Docker | The export path, and ~80 conformance checks per batch |
| [5. Multi-broker under load](#5-multi-broker-under-load-and-failure-khaos) | Docker + [Khaos](https://github.com/aleksandarskrbic/khaos) | Payload size at real cardinality, and the trigger-driven phases |
| [6. Kafka 4.x](#6-kafka-4x-the-two-group-protocols-no-older-broker-can-serve) | Docker | The KIP-848 and KIP-932 paths every other level gates off |

Levels 1–4 and 6 need nothing that isn't in this repo. Level 5 is where the interesting failures
are: a healthy single-broker cluster never fires half the collectors.

---

## 1. Unit tests

```bash
make test         # go test ./...
make test-race    # go test -race -count=2 ./...
make vet          # go vet ./...
make lint         # golangci-lint, pinned to the CI version in the Makefile
make check        # gofmt check + vet + test — the pre-push gate
make cover        # coverage as information, never a gate
```

CI's `go` job is a superset of `make check`: the same gofmt, vet and test steps plus
`go build ./...`, `-race` with coverage, and govulncheck. **Lint is deliberately not in
`check`** — it is a separate blocking CI job, so a lint bump can't silently start failing your
local test loop.

Two tests are worth knowing about by name.

**`TestSchemaV1Frozen`** (`internal/mockingest`) compares a reflected field-path listing of
`metrics.Batch` against the golden `internal/mockingest/testdata/schema_v1.txt`. It is the **only** thing that actually
enforces schema v1: the mock ingest's `DisallowUnknownFields` is inert while the agent and
the mock compile from the same tree, so without the golden a field could be added on both
sides at once and nothing would notice. If you change the wire shape deliberately:

```bash
go test ./internal/mockingest -run TestSchemaV1Frozen -update
```

Read the diff before committing it. A regenerated golden with an unexplained change is a
schema break shipped by accident.

**`TestDefaultSectionsMatchesTheCollector`** binds `mockingest.DefaultSections` to
`collector.SectionNames()`. Those two lists drifted once — three sections were added to the
collector and the mock's required-section contract kept the old list — and nothing caught it,
because the mock's own fixtures are built from its list, so the two agreed circularly.

---

## 2. One broker, no auth

The plain path. One KRaft broker, no SASL, agent in file mode:

```bash
docker compose up -d                 # Kafka + agent
docker compose logs -f agent
docker compose exec agent tail -f /var/lib/streamsight/metrics.jsonl
docker compose down -v
```

Kafka is published on `localhost:9092`, so the host-side targets talk to the same broker:

```bash
make run          # → ./metrics.jsonl, one JSON batch per line
make run-stdout   # → stdout, for | jq
```

What to look at in a batch:

```bash
# every section, its status and how long it took
jq -r '.sections[] | "\(.name)\t\(.status)\t\(.duration_ms)ms"' metrics.jsonl | tail -17

# anything that went wrong, most frequent first
jq -c 'select(.errors) | .errors[]' metrics.jsonl | sort | uniq -c | sort -rn

# errors[] is the only thing that can be short; absent means nothing was dropped
jq -c 'select(.truncation)' metrics.jsonl
```

All seventeen sections appear in **every** batch, in a fixed order, with `status: "skipped"`
for a phase that did not run. An absent section is a defect, not a configuration.

`truncation` is about `errors[]` and nothing else, which is why the recipe above is a
one-liner rather than a per-entity audit: no cap shortens the inventory, and the entity
counts always equal the length of the list beside them — the level-4 mock fails a mismatch
as a sender bug. See README, [Cardinality caps](../README.md#cardinality-caps) and
[Reading the data correctly](../README.md#reading-the-data-correctly), for the design and
for what `selection` says about coverage.

---

## 3. The permission model (SASL + ACLs)

`test/` brings up SASL/SCRAM with `StandardAuthorizer` and
`allow.everyone.if.no.acl.found=false`, grants the agent's principal exactly three ACLs, and
runs the agent under nothing else:

```bash
make test-local-up      # broker, SCRAM credential, three ACLs, seed data, agent
make test-local-logs    # agent stderr + one JSON batch per line
make test-local-down    # stop and delete volumes
```

The grants are `DESCRIBE` on `CLUSTER`, on `TOPIC '*'`, and on `GROUP '*'` — see
`test/setup-acls.sh`. This is the environment that keeps `SECURITY.md` honest: if a new
collector needs a fourth grant, this is where it fails.

**`topic_configs` and `broker_configs` report `unauthorized` here, and that is correct.**
`COLLECT_CONFIGS` defaults on and needs `DESCRIBE_CONFIGS` on `TOPIC` and `CLUSTER`, which
`setup-acls.sh` deliberately does not grant — so this environment doubles as the standing
test that the two config sections degrade cleanly and nothing else degrades with them.

You will not see it in the first batch. The configs phase samples on the cycles where `(n + 5) % CONFIGS_EVERY == 0`, so at the shipped `360` the
first one is cycle 355 — an hour in at this file's 10s interval, and every batch before it
reads `skipped`. Add `CONFIGS_EVERY=1` to the agent's environment to see the refusal
immediately: `topic_configs` then reports `TOPIC_AUTHORIZATION_FAILED` (error code 29) and
`broker_configs` reports `CLUSTER_AUTHORIZATION_FAILED` (31), both on batch 1. Set
`COLLECT_CONFIGS=false` to stop asking, or add the two grants to exercise the success path.

### What this rig has settled about the CLUSTER grant

Two of the five grants in `SECURITY.md` cover APIs the broker gates on `DESCRIBE` of
`CLUSTER`, and both are measured here rather than read off Kafka's authorization rules.

`DescribeLogDirs` needs nothing arranged. The log-dirs phase carries cadence offset 0, so it
samples on cycle 0 and **the first batch of an unmodified `make test-local-up` answers the
question**. As `User:streamsight-agent` holding nothing but the three grants it reports
`{"name":"log_dirs","status":"ok"}` with per-replica sizes and the KIP-827
`total_bytes`/`usable_bytes` — cp-kafka 7.5.0 advertises `DescribeLogDirs` v4, so that is the
raw sharded path and not the kadm fallback. Take the grant away and restart the agent and the
section turns `unauthorized` with `CLUSTER_AUTHORIZATION_FAILED` (error code 31) while every
other section stays `ok` and the `cluster` block comes back byte-identical. That last part is
what makes the control readable: cluster metadata does not need that grant on this broker, so
removing it isolates one API instead of blanking the batch.

Restart with `make test-local-restart`, never `up -d`. `up` re-runs `kafka-init`, which
recreates the very grant you just removed and hands you a clean batch that looks like a
refutation. The restart is also what puts the log-dirs phase back on cycle 0, so you read the
answer on the next batch instead of waiting out `LOG_DIRS_EVERY`.

`ListPartitionReassignments` needs an under-replicated partition, which a single broker
holding only replication-factor-1 topics cannot produce. `make test-local-urp` builds the
missing condition: `test/docker-compose.urp.yml` layers a second SASL broker onto this same
stack — no new ACL, the three grants are still all there is — and `test/urp-probe.sh` creates
a replicated topic, stops that broker, waits out `replica.lag.time.max.ms` for the ISR to
shrink, then reads the next batch.

```bash
make test-local-urp     # non-zero unless reassignments comes back ok
make test-local-down    # tears the second broker down too
```

Under those three grants and three observed URPs the section reports
`{"name":"reassignments","status":"ok"}` with no rows — which is right, because nothing is
actually moving and the controller answers only for live reassignments. Revoke `DESCRIBE` on
`CLUSTER` with the URPs still standing and it reports `unauthorized` with error code 31,
alongside `log_dirs`. What that settles is the authorization and the request, not the row
shaping — see *Still unverified*.

Expect collateral for the length of a URP run: `log_dirs` goes `partial` with an
`unknown broker` error for the shard that cannot answer, and the `topics*` sections go
`partial` with `LEADER_NOT_AVAILABLE` while the election runs. A baseline with a standing
exception in it is not a baseline, which is why the second broker is an overlay and not part
of `make test-local-up`.

### Testing a degraded permission

Removing a grant from the running broker is the fastest way to see how a section reports a
loss of access:

```bash
docker compose -f test/docker-compose.yml exec kafka \
  kafka-acls --bootstrap-server localhost:9092 --command-config /tmp/admin.properties \
  --remove --force --allow-principal User:streamsight-agent \
  --operation DESCRIBE --group '*' --resource-pattern-type literal
```

The `groups` and `offsets` sections should go `unauthorized` while everything else stays
`ok`. What you are checking is that **a failed section means "no data", never "no entities"** —
a backend must not read an empty `groups[]` as "the consumer groups are gone".

SCRAM credentials in KRaft live in the metadata log, so the bootstrap admin credential is
written by `kafka-storage format --add-scram` before the broker starts;
`test/docker-compose.yml` overrides the image entrypoint to do it.

---

## 4. HTTP export against the conformance mock

For local work the repo ships its own receiver rather than pointing `EXPORT_ENDPOINT` at
anything real. `cmd/mock-ingest` is a **conformance checker, not an ingest**: it stores
nothing and runs roughly eighty invariants over every batch, each with a stable, greppable
dotted code; one whose section the batch does not carry reports nothing rather than failing.

```bash
make test-http          # SASL/ACL Kafka + agent in http mode + mock, following the output
make test-http-logs     # follow it again later
make test-http-down     # stop and delete volumes
make test-http-verify   # the same stack as a pass/fail gate (MOCK_REQUIRE batches, default 5)
```

It layers `test/docker-compose.http.yml` over the level-3 environment, so the export path is
exercised under exactly the read-only permission model above. The mock never speaks to Kafka
and adds no ACL of any kind.

Without Docker, run the two halves against the level-2 broker in separate shells:

```bash
make run-mock    # mock ingest on :8088
make run-http    # agent in http mode against it
```

### Faults are injected by default

`test/docker-compose.http.yml` sets `MOCK_FAIL_FIRST=2` and `MOCK_EVERY_NTH=5` (→ 429). The
agent **will** log `export failed` warnings and the mock prints a matching `[fault]` line for
each. **That is the harness working.** For a clean run:

```bash
MOCK_FAIL_FIRST=0 MOCK_EVERY_NTH=0 make test-http
```

Or drive the faults live while it runs:

```bash
curl -XPOST localhost:8088/control -d '{"every_nth":3,"every_nth_status":503}'
curl localhost:8088/stats
curl 'localhost:8088/batches?n=1' | jq
```

`--fail-first`, `--every-nth`, `--delay-ms`, `--drop-every` and `--terminal-once` cover the
retry, backoff, timeout, connection-drop and terminal-rejection paths respectively; `--help`
lists them all.

### What it checks

84 codes across eleven prefixes: `data.*` (24), `sections.*` (13, the seventeen-name list and
its order, per-section `error_count`, phase-order assertions), `transport.*` (12, gzip and
`Content-Encoding` agreement, body size, compression ratio), `envelope.*` and `agent.*` (7 each,
including `envelope.idempotency_key_format` and clock skew), `errors.*` (6), `idempotency.*`
(5 — replay, body mismatch, and `batch_seq` regression and gap detection), `truncation.*` and
`body.*` (3 each), `selection.*` and `schema.*` (2 each, the latter being the strict decode with
`DisallowUnknownFields`).

The one that matters most is **`data.negative_lag`** — a committed offset above the high
watermark for the same partition. It is the runtime detector for the collector's phase-order
constraint, which is a correctness property, not an optimisation: offsets are sampled
`start ≤ committed ≤ LSO ≤ high watermark` so the unavoidable skew between calls makes every
derived difference err high instead of going negative. Reorder the phases and this check is
what tells you. `internal/mockingest/check.go` is the full list.

`--warn-only` never rejects a batch and is right for exploring. `--strict` promotes every
warning to a rejection and is **CI only**: a rejection is terminal, so the exporter discards
that batch permanently.

---

## 5. Multi-broker, under load and failure (Khaos)

Everything above runs against one healthy broker with a handful of partitions. That is not
enough to test this agent, for two reasons:

1. **Payload size is O(partitions) and O(replicas), not O(brokers).** Sections that are
   invisible on a toy cluster dominate at real cardinality.
2. **Several phases only fire on a trigger.** `reassignments` needs an under-replicated
   partition. `epoch_probes` needs a leader-epoch divergence. Neither ever happens on a
   healthy single broker, so the code ships unexercised until something breaks the cluster on
   purpose.

[Khaos](https://github.com/aleksandarskrbic/khaos) is a Kafka load-testing and chaos CLI that
does both. It manages its own local cluster, so there is nothing to configure:

```bash
go install github.com/aleksandarskrbic/khaos/cmd/khaos@latest
khaos list                            # bundled scenarios
khaos run traffic/high-throughput     # auto-starts a 3-broker KRaft cluster
```

The cluster is published on `localhost:9092`, `:9093` and `:9094`. Point the agent at all
three and let it run alongside the scenario:

```bash
make build
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094 \
EXPORT_MODE=file EXPORT_FILE=./khaos-run.jsonl \
COLLECTION_INTERVAL=5s LOG_LEVEL=info \
  ./bin/kafka-metrics-agent
```

To drive an existing cluster instead — including a managed one over SASL/SSL — use
`khaos simulate` rather than `khaos run`.

### Which scenario exercises what

| Scenario | What it puts the agent through |
|---|---|
| `traffic/high-throughput` | Steady-state payload at real partition counts. The measurement that matters: batch bytes, raw and gzipped. |
| `traffic/hot-partition` | Skewed key distribution — per-partition throughput that a topic-level aggregate hides. |
| `traffic/consumer-lag` | Producers outpacing consumers: the committed-vs-high-watermark path, and the offset chain that must never go negative. |
| `chaos/broker-chaos` | Brokers stopping and restarting under traffic. URPs, offline replicas, leader movement, and the error attribution and dedup paths. |
| `chaos/rebalance-storm` | A group rebalancing repeatedly. Member-set churn in `groups[].members[].member_id` is the intended rebalance signal — see the caveat below. |
| `chaos/leadership-churn` | Leader elections, which is the readiest way to get `epoch_probes` to fire. |

### What to measure

The agent's headline cost figure comes from a run like this, and it is worth re-deriving
after any change to a collector:

```bash
# raw bytes per batch, largest last
awk '{print length($0)}' khaos-run.jsonl | sort -n | tail -5

# what actually crosses the network (the exporter gzips)
tail -1 khaos-run.jsonl | gzip -9 | wc -c

# which top-level key is eating the batch
tail -1 khaos-run.jsonl | jq -r 'to_entries[] | "\(.value|tostring|length)\t\(.key)"' | sort -rn

# did any section fail, or lose errors[] entries, at any point in the run?
jq -r '.sections[] | select(.status != "ok" and .status != "skipped") | .name + " " + .status' \
  khaos-run.jsonl | sort | uniq -c
jq -c 'select(.truncation)' khaos-run.jsonl | head
```

For reference, the last full measurement — 390 partitions, 24 groups, 3 brokers, shipped
defaults — was **174 KB raw / 16.9 KB gzipped** steady, ≈8.8 GB/month per cluster.
Partitions and committed-offset rows are the axes; broker count is not.

### Three things a chaos run has already taught, so you don't re-learn them

- **Do not try to detect rebalances by sampling group state.** Measured against
  `chaos/rebalance-storm`: over 33 batches there were 31 real member-set changes, of which
  sampling group state caught 2 — every other sample read `Stable`. A rebalance completes
  well inside a 5s poll, so the sampler lands on either side of it. That is a sampling failure, not a tuning one, and halving the interval
  does not fix it. Detect rebalances from member-ID churn in `groups[].members[]`: it is in
  every batch at zero request cost, and it caught all 31.
- **`generation` is `-1` in practice**, so a detector keyed on generation deltas reads a
  storming cluster as perfectly stable. Use member-ID churn, or `group_epoch` on a KIP-848
  cluster; README's
  [Known gaps](../README.md#known-gaps-in-the-current-schema) has the mechanism.
- **`chaos/broker-chaos` does reach the `reassignments` phase, and only the phase.** Three
  stop/start windows over a 41-batch run at a 5s interval, 6 under-replicated partitions in
  each, and the section flipped `skipped` → `ok` for the length of every window with a real
  request behind it (0–87 ms) and no rows, since a stopped broker is a URP that nothing is
  reassigning. One case is not that clean: an agent started while the *controller* is the
  broker that is down reported `failed` for its first three batches, with a `transport` error
  of `context deadline exceeded` against the section's own budget and no Kafka error code —
  the phase refusing to call an unreachable controller a clean "nothing is moving", which is
  the behaviour you want and is easy to misread as a permission problem. What this cluster
  cannot settle is the ACL: Khaos runs no authorizer, so it proves the trigger and the
  request, never the grant. `make test-local-urp` is where the grant is proved.

---

## 6. Kafka 4.x: the two group protocols no older broker can serve

Levels 2–4 all run `confluentinc/cp-kafka:7.5.0`, which is Kafka 3.5. It advertises neither
`ConsumerGroupDescribe` (KIP-848, API key 69) nor `ShareGroupDescribe` (KIP-932, key 77), so
the capability probe disables both phases at startup — correctly, and every time. That makes
those levels a test of the *degradation* path only. This one runs the other path:

```bash
make test-kafka4        # broker, share.version=1, traffic, three consumers, agent
make test-kafka4-logs   # follow the agent again later
make test-kafka4-down   # stop and delete the volumes
```

`test/docker-compose.kafka4.yml` is Apache Kafka 4.1.0, one KRaft node, published on
`localhost:39092` so it can run beside every other environment here. Against one topic
(`orders`, 6 partitions) under a continuous producer it runs three consumers at once — a
classic one, one with `group.protocol=consumer`, and a `kafka-console-share-consumer` — so a
single batch carries all three group kinds. The agent runs with `COLLECT_SHARE_GROUPS=true`
in stdout mode.

### What 4.1 needs before it will serve them

Read off `kafka-features describe` on a freshly formatted 4.1 cluster, not off the release notes:

- **KIP-848 needs nothing.** `group.version` is already `FinalizedVersionLevel: 1` on a fresh
  cluster. A client that asks for `group.protocol=consumer` gets a new-protocol group with no
  broker configuration at all.
- **KIP-932 is off by default.** `share.version` is `FinalizedVersionLevel: 0` on a fresh
  cluster — while the same broker answers `ShareGroupDescribe(77): 1 [usable: 1]` in
  `ApiVersions`. So the capability probe reports the API supported on a stock 4.1 broker where
  no share group can exist, and the section reports `skipped` for a reason nothing on the wire
  distinguishes from "no share groups have been created". `setup-kafka4.sh` raises the feature
  with `kafka-features upgrade`, because the `apache/kafka` image formats its own storage and
  gives no way to pass `kafka-storage format --feature`.
- **`share.coordinator.state.topic.replication.factor` defaults to 3, its `min.isr` to 2**,
  which one broker cannot satisfy, and the failure is silent. Measured on a probe cluster with
  the defaults left alone: the share group reaches `Stable` with its one member, that member is
  assigned **0 partitions**, `--describe` returns an empty start-offset table, the
  `__share_group_state` topic is never created, and the consumer logs nothing but the generic
  KIP-932 preview warning. No error is raised anywhere. The compose file sets both to 1. This
  is the one that costs an afternoon.
- **`group.coordinator.rebalance.protocols` is not involved**, and neither is
  `unstable.api.versions.enable`. Share groups were verified working on 4.1.0 with the stock
  `classic,consumer,streams` list and no unstable-API flag.

### What came back — apache/kafka 4.1.0, one broker

The probe **enables** both phases: the agent logs no `disabling` line at all, and on the wire
`software_versions: ["v4.1"]`, `features.consumer_group_describe: true`, with
`api_max_versions` reporting `consumer_group_describe: 1`, `list_groups: 5`,
`list_offsets: 10`, `metadata: 13`, `describe_log_dirs: 4`, `offset_for_leader_epoch: 4`,
`list_partition_reassignments: 0`. Section statuses in a steady-state batch: **`share_groups`
`ok`**, **`groups` `partial`**, `cluster` / `topics` / `topics_lso` / `topics_end` / `offsets`
/ `broker_rpc` `ok`, and the interval- and trigger-gated phases `skipped` as anywhere else
(`log_dirs` answers `ok` on batch 1, on its cadence offset, then goes `skipped`).

**Share groups work end to end.** `ShareGroupDescribe` and `DescribeShareGroupOffsets` both
answered, and the section is fully populated — including the `member_epoch` that the KIP-848
path below never gets:

```json
{"id":"share-workers","state":"Stable","coordinator":1,"group_epoch":2,"assignment_epoch":2,
 "assignor":"simple","member_count":1,
 "members":[{"member_id":"w4Iru_WeQS6EHp0ujsIeYg","client_id":"console-share-consumer",
             "member_epoch":2,"subscribed_topics":["orders"],"assignment":[…6 partitions…]}],
 "start_offsets":[{"topic":"orders","partition":0,"start_offset":228,"leader_epoch":0}, …]}
```

**KIP-848 populates the group epochs and nothing else.** `group_epoch`, `assignment_epoch` and
`assignor` (`"uniform"`) arrive; `member_epoch` does not, and never can as the overlay is
written. On 4.x the classic `DescribeGroups` answers `GROUP_ID_NOT_FOUND` for a new-protocol
group, so the group ships with no members for `enrichConsumerGroups` to overlay onto:

```json
{"id":"classic-consumers","state":"Stable","group_type":"classic","member_count":1}
{"id":"modern-consumers","state":"Dead","group_type":"consumer","member_count":0,"members":[],
 "error_code":69,"group_epoch":1,"assignment_epoch":1,"assignor":"uniform"}
{"id":"share-workers","state":"Dead","group_type":"share","member_count":0,"error_code":69}
```

The `modern-consumers` group was `Stable` with one live member holding all six partitions
throughout, confirmed against `kafka-consumer-groups.sh --describe --members`. `Dead` is the
broker's own word — Kafka returns it alongside `GROUP_ID_NOT_FOUND` — and the `offsets` section
is unaffected: `OffsetFetch` returns all six committed offsets for that same group in the same
batch, so one batch says both "this group has no members" and "here is what its members
committed".

**That leaves `groups` `partial` on every cycle, and it is a defect in the collector, not the
rig.** Both the new-protocol group and the share group are refused, and error dedup collapses
them — the key is `(api, kind, kafka_error_code, broker_id)`, so the two arrive as one exemplar
with `count: 2` and whichever group was seen first in the `group` field:

```json
{"section":"groups","api":"DescribeGroups","group":"modern-consumers","kafka_error_code":69,
 "kind":"other","message":"GROUP_ID_NOT_FOUND: The group id does not exist.","count":2}
```

`internal/collector/groups.go` treats the classic describe as authoritative and the KIP-848
call as an overlay ("the classic describe is the one that decides"). On a 4.x cluster that is
backwards for exactly the groups the overlay exists to describe, and share groups should not be
in `DescribeGroups`' argument list at all — `collectShareGroups` already filters them out of its
own phase by `GroupType` for precisely this reason.

---

## Still unverified

Honest gaps, so nobody assumes a green test run covers them:

- **The `DESCRIBE_CONFIGS` success path.** `test/setup-acls.sh` withholds both config grants
  on purpose, so what the level-3 rig measures is the refusal — `topic_configs` with
  `TOPIC_AUTHORIZATION_FAILED`, `broker_configs` with `CLUSTER_AUTHORIZATION_FAILED`. No run
  has granted the two and watched the sections come back `ok`.
- **Reassignment rows.** `reassignments` has been made to answer under the real ACLs, on a
  forced under-replicated partition, and it answered `ok` with an empty list — correct,
  because a stopped broker is a URP that nothing is moving. So `buildReassignments` has still
  never had `adding_replicas`/`removing_replicas` to shape. The authorization is settled; the
  payload is not.
- **KIP-848 epochs under churn.** `group_epoch` and `assignment_epoch` are populated on a 4.1
  cluster (level 6), but only ever observed at 1 on a single-member, single-broker group.
  Nothing has watched them advance through a rebalance, which is the whole reason they are
  collected — that needs Khaos' `chaos/rebalance-storm` pointed at a 4.x cluster. `member_epoch`
  is a separate matter and is not a gap: it is never populated today, for the reason level 6
  records.
- **Share groups beyond one member, and under ACLs.** The section returns `ok` with epochs,
  assignment and start offsets on Kafka 4.1 (level 6), but with a single share consumer, one
  topic and no acknowledgement backlog; nothing has exercised a multi-member share group or a
  member losing its assignment. `ShareGroupDescribe` and `DescribeShareGroupOffsets` have also
  never run under a DESCRIBE-only principal — the level-3 SASL environment is cp-kafka 7.5.0
  and cannot serve them at all — so `SECURITY.md`'s grant list is unverified for this section.
- **Kafka 4.0.x, and any multi-broker 4.x cluster.** Everything level 6 records was measured on
  a single-node 4.1.0. 4.0 shipped share groups as early access, where `unstable.api.versions.enable`
  and an explicit `share` in `group.coordinator.rebalance.protocols` may still be required; none
  of that was tested, so do not read the level-6 notes as holding for 4.0.
