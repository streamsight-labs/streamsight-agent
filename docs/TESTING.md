# Testing

Five levels, each proving something the one below it cannot. Run them in order the first
time; after that, run the level that matches what you changed.

| Level | Needs | Proves |
|---|---|---|
| [1. Unit tests](#1-unit-tests) | Go toolchain | Logic, the wire contract, the schema shape |
| [2. One broker](#2-one-broker-no-auth) | Docker | It talks to a real Kafka and produces batches |
| [3. The permission model](#3-the-permission-model-sasl--acls) | Docker | It works inside the ACLs the product promises |
| [4. HTTP export](#4-http-export-against-the-conformance-mock) | Docker | The export path, and ~100 conformance checks per batch |
| [5. Multi-broker under load](#5-multi-broker-under-load-and-failure-khaos) | Docker + [Khaos](https://github.com/aleksandarskrbic/khaos) | Payload size at real cardinality, and the trigger-driven phases |

Levels 1–4 need nothing that isn't in this repo. Level 5 is where the interesting failures
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

`make check` is what CI's `go` job runs. **Lint is deliberately not in `check`** — it is a
separate blocking CI job, so a lint bump can't silently start failing your local test loop.

Two tests are worth knowing about by name.

**`TestSchemaV1Frozen`** (`internal/mockingest`) compares a generated batch against the
golden `internal/mockingest/testdata/schema_v1.txt`. It is the **only** thing that actually
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

# was anything dropped by a cap?
jq -c 'select(.truncation)' metrics.jsonl
```

All seventeen sections appear in **every** batch, in a fixed order, with `status: "skipped"`
for a phase that did not run. An absent section is a defect, not a configuration.

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

**Expect `topic_configs` and `broker_configs` to report `unauthorized` here.** That is
correct, not a bug. `COLLECT_CONFIGS` defaults on and needs `DESCRIBE_CONFIGS` on `TOPIC` and
`CLUSTER`, which `setup-acls.sh` deliberately does not grant — so this environment doubles as
the standing test that the two config sections degrade cleanly and nothing else degrades with
them. Set `COLLECT_CONFIGS=false` to stop asking, or add the grants if you want to exercise
the success path.

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

`EXPORT_ENDPOINT` has nowhere real to point yet, so the repo ships its own receiver.
`cmd/mock-ingest` is a **conformance checker, not an ingest**: it stores nothing and
validates roughly a hundred invariants on every batch, each with a stable, greppable dotted
code.

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

The codes group by prefix: `transport.*` (gzip and `Content-Encoding` agreement,
`Idempotency-Key` format and uniqueness, body size, compression ratio), `schema.*` and
`envelope.*` (strict decode with `DisallowUnknownFields`, `batch_seq` monotonicity and gap
detection, clock skew), `sections.*` (the seventeen-name list, its order, per-section
`error_count`, phase-order assertions), `errors.*`, and `data.*`.

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

# did any section fail, or get truncated, at any point in the run?
jq -r '.sections[] | select(.status != "ok" and .status != "skipped") | .name + " " + .status' \
  khaos-run.jsonl | sort | uniq -c
jq -c 'select(.truncation)' khaos-run.jsonl | head
```

For reference, the last full measurement — 390 partitions, 24 groups, 3 brokers, shipped
defaults — was **174 KB raw / 16.9 KB gzipped** steady, ≈8.8 GB/month per cluster.
Partitions and committed-offset rows are the axes; broker count is not.

### Two things a chaos run has already taught, so you don't re-learn them

- **A rebalance completes well inside a 5s poll.** A dedicated `PreparingRebalance` dwell
  watcher on a fast ticker was built, measured against `chaos/rebalance-storm`, and found
  **2 transitions out of 31 real member-set changes** — reading `Stable` on every sample,
  because the poll landed on either side of each rebalance. It was deleted. This is a
  sampling failure, not a tuning one: halving the interval does not fix it. Rebalance
  detection is member-ID churn in `groups[].members[]`, which is in every batch at zero
  request cost and caught all 31.
- **`generation` is `-1` in practice.** It comes from the sticky-assignor hint, not the
  group's authoritative generation, and the standard Java consumer leaves it at `-1` with
  both `range` and `cooperative-sticky`. A detector keyed on generation deltas reads a
  storming cluster as perfectly stable. Use member-ID churn, or `group_epoch` on a
  KIP-848 cluster.

---

## Still unverified

Honest gaps, so nobody assumes a green test run covers them:

- **`DescribeLogDirs` under `DESCRIBE CLUSTER`** is shipping code and now defaults on, but
  the end-to-end ACL run has never had `COLLECT_LOG_DIRS` enabled. Flipping it against the
  level-3 environment settles the claim in minutes, and it is the cheapest open item here.
- **`ListPartitionReassignments` under `DESCRIBE CLUSTER`** fires only on an observed
  under-replicated partition. `chaos/broker-chaos` is the way to produce one.
- **The KIP-848 success path.** `ConsumerGroupDescribe` runs as an overlay on the classic
  describe and only its *degradation* path is verified (cp-kafka 7.5.0 disables the phase at
  startup, correctly). No Kafka 4.x cluster has answered it.
- **Share groups** (`COLLECT_SHARE_GROUPS`, KIP-932) have never run against a broker that can
  answer. Kafka 4.0+.
- **Every entity cap defaults to unlimited.** Truncation is reported at batch, section and
  entity level when a cap is set, but no default cap has been chosen, so nothing bounds the
  batch on a pathological cluster.
- **No unit test** covers the SIGTERM/`batch_seq` path, the file exporter's `needsReopen`
  rotation retry, or `EXPORT_FILE_FSYNC`. The first is covered at runtime only, by the mock's
  `idempotency.seq_gap` check.
