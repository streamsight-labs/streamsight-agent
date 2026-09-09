#!/bin/bash
#
# Turns a freshly formatted Kafka 4.1 cluster into one that can serve both group
# protocols, then seeds the topic the three consumers subscribe to.
#
# Runs inside apache/kafka, where the CLI tools DO carry the .sh suffix and live
# under /opt/kafka/bin -- the opposite of confluentinc/cp-kafka, which is what
# setup-acls.sh next door has to match.
#
# The only thing that has to happen here is share.version. KIP-848 needs nothing:
# group.version is already finalized at 1 on a fresh 4.1 cluster. KIP-932 is
# finalized at 0, and the image formats its own storage inside KafkaDockerWrapper
# with no hook for `kafka-storage format --feature`, so the level is raised over
# the admin API after the broker is up instead. It lands in the metadata log, so
# it survives a restart of the broker container.
#
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
TEST_TOPIC="${TEST_TOPIC:-orders}"
PARTITIONS="${PARTITIONS:-6}"

BIN=/opt/kafka/bin

echo "Waiting for $BOOTSTRAP_SERVER to answer..."
for _ in $(seq 1 60); do
    if "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVER" > /dev/null 2>&1; then
        break
    fi
    sleep 2
done
"$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP_SERVER" > /dev/null

echo "Enabling share groups (KIP-932)"
"$BIN/kafka-features.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    upgrade --feature share.version=1

# The finalized levels end up in `docker compose logs kafka-init`, which is the
# evidence for what the cluster was actually asked to serve on this run.
"$BIN/kafka-features.sh" --bootstrap-server "$BOOTSTRAP_SERVER" describe

echo "Creating $TEST_TOPIC with $PARTITIONS partitions"
"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists --topic "$TEST_TOPIC" \
    --partitions "$PARTITIONS" --replication-factor 1

echo "Kafka 4.x environment ready"
