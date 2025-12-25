#!/bin/bash
#
# Sets up the streamsight-agent user with read-only ACLs
# Run this after Kafka is ready
#

set -e

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
ADMIN_CONFIG="/tmp/admin.properties"

# Create admin properties file
cat > "$ADMIN_CONFIG" << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOF

echo "Waiting for Kafka to be ready..."
until kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP_SERVER" --command-config "$ADMIN_CONFIG" > /dev/null 2>&1; do
    echo "  Kafka not ready, retrying in 2s..."
    sleep 2
done
echo "Kafka is ready!"

echo ""
echo "Creating user: streamsight-agent"
kafka-configs.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --alter \
    --add-config 'SCRAM-SHA-512=[password=agent-secret]' \
    --entity-type users \
    --entity-name streamsight-agent

echo ""
echo "Granting DESCRIBE on CLUSTER..."
kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:streamsight-agent \
    --operation DESCRIBE \
    --cluster

echo ""
echo "Granting DESCRIBE on all TOPICs..."
kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:streamsight-agent \
    --operation DESCRIBE \
    --topic '*' \
    --resource-pattern-type prefixed

echo ""
echo "Granting DESCRIBE on all GROUPs..."
kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --add \
    --allow-principal User:streamsight-agent \
    --operation DESCRIBE \
    --group '*' \
    --resource-pattern-type prefixed

echo ""
echo "Creating test topic: test-topic"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --create \
    --topic test-topic \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "Agent credentials:"
echo "  Username: streamsight-agent"
echo "  Password: agent-secret"
echo ""
echo "Listing ACLs:"
kafka-acls.sh --bootstrap-server "$BOOTSTRAP_SERVER" \
    --command-config "$ADMIN_CONFIG" \
    --list \
    --principal User:streamsight-agent
