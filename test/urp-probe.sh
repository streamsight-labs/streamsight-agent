#!/bin/bash
#
# Forces an under-replicated partition on the level-3 SASL/ACL cluster and
# reports what the agent's reassignments section did about it.
#
# WHY THIS EXISTS. ListPartitionReassignments is the only request the agent
# issues that no healthy cluster ever provokes: the phase fires on a partition
# it has already observed under-replicated, and the level-3 broker is a single
# node whose every topic is replication-factor 1, where a URP is arithmetically
# impossible. So the claim SECURITY.md makes about that API — that DESCRIBE on
# CLUSTER is the whole of its authorization, and that no sixth grant hides
# behind it — had no way to be exercised by any run of the standing rig. This
# script builds the missing condition: a second broker, a replicated topic, and
# then the second broker taken away.
#
# It is deliberately NOT part of `make test-local-up`. The second broker
# doubles the standing test's footprint to prove something that only matters
# when it is being questioned, and stopping a broker makes log_dirs report
# `partial` — the shard for the dead broker cannot answer — which would turn
# the level-3 run's clean all-ok baseline into one with a standing exception in
# it.
#
#   make test-local-urp     # this script, start to finish
#   make test-local-down    # removes the second broker too
#
set -euo pipefail

cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.urp.yml)

TOPIC="${URP_TOPIC:-replicated}"
ADMIN_CONFIG=/tmp/admin.properties

command -v jq > /dev/null 2>&1 || { echo "urp-probe.sh needs jq on PATH"; exit 1; }

# One helper for every admin CLI call: the broker answers nothing without
# credentials, and the bootstrap admin's properties file is written into the
# kafka container by its entrypoint (see docker-compose.yml).
kadmin() {
    "${COMPOSE[@]}" exec -T kafka "$@" --bootstrap-server localhost:9092 \
        --command-config "$ADMIN_CONFIG"
}

# Counts the JSON batches the agent has printed. --no-log-prefix drops the
# "agent-1  | " compose adds, so a batch line is a bare JSON object and the
# count is exact. The log is cumulative across restarts, which is why every
# wait below is expressed as a delta rather than a --tail.
batches() {
    "${COMPOSE[@]}" logs --no-log-prefix agent 2>/dev/null | grep -c '^{' || true
}

echo "==> bringing up the level-3 stack plus the second broker"
"${COMPOSE[@]}" up -d --build

echo "==> waiting for both brokers to register"
until [ "$(kadmin kafka-broker-api-versions 2>/dev/null | grep -c 'id:')" -ge 2 ]; do
    sleep 3
done

echo "==> creating $TOPIC with replication-factor 2"
kadmin kafka-topics --create --topic "$TOPIC" --partitions 3 \
    --replication-factor 2 --if-not-exists

echo "==> stopping kafka2"
"${COMPOSE[@]}" stop kafka2

# The ISR does not shrink the moment a broker dies: the leader waits
# replica.lag.time.max.ms (30s by default) before evicting the follower, and
# until it does the partition is not under-replicated and the trigger the agent
# reads is not yet true.
echo "==> waiting for the ISR to shrink (up to replica.lag.time.max.ms)"
until [ "$(kadmin kafka-topics --describe --topic "$TOPIC" \
    --under-replicated-partitions 2>/dev/null | grep -c 'Isr:')" -ge 1 ]; do
    sleep 5
done
kadmin kafka-topics --describe --topic "$TOPIC" --under-replicated-partitions

# The phase is trigger-driven rather than cadenced, so the first batch
# collected after the URP is visible is the one that asks. Two batches are
# awaited, not one: a cycle already in flight when the ISR shrank read the old
# metadata and would answer for a cluster that was still healthy.
echo "==> waiting for two more batches"
before="$(batches)"
until [ "$(batches)" -gt "$((before + 1))" ]; do
    sleep 2
done

batch="$("${COMPOSE[@]}" logs --no-log-prefix agent 2>/dev/null | grep '^{' | tail -1)"
status="$(printf '%s' "$batch" | jq -r '.sections[] | select(.name=="reassignments") | .status')"
principal="$(printf '%s' "$batch" | jq -r '.principal')"
urps="$(printf '%s' "$batch" | jq '[.topics[].partitions[] | select((.isr|length) < (.replicas|length))] | length')"

echo
echo "principal:                   $principal"
echo "under-replicated partitions: $urps"
echo "reassignments section:       $status"
printf '%s' "$batch" | jq -c '.errors // [] | map(select(.section == "reassignments"))'

if [ "$status" != "ok" ]; then
    echo
    echo "FAIL: reassignments reported '$status' under the three DESCRIBE grants."
    echo "An 'unauthorized' here means ListPartitionReassignments needs a grant"
    echo "the agent does not have, and SECURITY.md's five-grant promise is wrong."
    exit 1
fi

echo
echo "PASS: ListPartitionReassignments answered under DESCRIBE on CLUSTER alone."
