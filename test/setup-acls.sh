#!/bin/bash
#
# Provisions the agent's SASL credential and its read-only ACLs on the local
# test broker, then seeds a topic and a consumer group so the first collected
# batch is not empty.
#
# Runs inside confluentinc/cp-kafka, where the CLI tools have no .sh suffix.
# The bootstrap admin SCRAM credential is written at storage-format time by
# docker-compose.yml; everything below authenticates as that admin.
#
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin-secret}"
AGENT_USER="${AGENT_USER:-streamsight-agent}"
AGENT_PASSWORD="${AGENT_PASSWORD:-agent-secret}"
TEST_TOPIC="${TEST_TOPIC:-orders}"
TEST_GROUP="${TEST_GROUP:-order-processor}"

ADMIN_CONFIG=/tmp/admin.properties
cat > "$ADMIN_CONFIG" << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${ADMIN_USER}" password="${ADMIN_PASSWORD}";
EOF

echo "Waiting for $BOOTSTRAP_SERVER to accept authenticated requests..."
for _ in $(seq 1 60); do
    if kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP_SERVER" \
        --command-config "$ADMIN_CONFIG" > /dev/null 2>&1; then
        break
    fi
    sleep 2
done
kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" > /dev/null

echo "Creating SCRAM credential for $AGENT_USER"
kafka-configs --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --alter \
    --add-config "SCRAM-SHA-512=[password=${AGENT_PASSWORD}]" \
    --entity-type users \
    --entity-name "$AGENT_USER"

# The three grants the agent needs, and nothing else.
#
# --resource-pattern-type literal with the name '*' is Kafka's wildcard: it
# matches every topic/group. It is NOT the same as `prefixed` with '*', which
# matches only names that literally begin with an asterisk.
echo "Granting DESCRIBE on CLUSTER"
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal "User:${AGENT_USER}" \
    --operation DESCRIBE \
    --cluster

echo "Granting DESCRIBE on all TOPICs"
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal "User:${AGENT_USER}" \
    --operation DESCRIBE \
    --topic '*' \
    --resource-pattern-type literal

echo "Granting DESCRIBE on all GROUPs"
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal "User:${AGENT_USER}" \
    --operation DESCRIBE \
    --group '*' \
    --resource-pattern-type literal

echo "Seeding topic $TEST_TOPIC and consumer group $TEST_GROUP"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --create \
    --topic "$TEST_TOPIC" \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

seq 1 500 | kafka-console-producer --bootstrap-server "$BOOTSTRAP_SERVER" \
    --producer.config "$ADMIN_CONFIG" \
    --topic "$TEST_TOPIC"

# Consumes less than was produced, so the group has committed offsets and a
# real, non-zero lag against the high watermark.
timeout 30 kafka-console-consumer --bootstrap-server "$BOOTSTRAP_SERVER" \
    --consumer.config "$ADMIN_CONFIG" \
    --topic "$TEST_TOPIC" \
    --group "$TEST_GROUP" \
    --from-beginning \
    --max-messages 400 > /dev/null || true

echo
echo "Setup complete. Agent credentials: ${AGENT_USER} / ${AGENT_PASSWORD}"
echo "ACLs for User:${AGENT_USER}:"
kafka-acls --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --list \
    --principal "User:${AGENT_USER}"
