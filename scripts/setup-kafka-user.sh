#!/bin/bash
#
# Creates a read-only Kafka user for Streamsight Agent
# Usage: ./setup-kafka-user.sh [options]
#

set -e

# Defaults
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
USERNAME="${KAFKA_USERNAME:-streamsight-agent}"
PASSWORD="${KAFKA_PASSWORD:-}"
MECHANISM="${KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"
COMMAND_CONFIG="${KAFKA_COMMAND_CONFIG:-}"

print_usage() {
    cat << EOF
Usage: $0 [options]

Creates a read-only Kafka user for Streamsight Agent with minimal ACLs.

Options:
    -b, --bootstrap-server  Kafka bootstrap server (default: localhost:9092)
    -u, --username          Username to create (default: streamsight-agent)
    -p, --password          Password for the user (required)
    -m, --mechanism         SASL mechanism: SCRAM-SHA-256 or SCRAM-SHA-512 (default: SCRAM-SHA-512)
    -c, --command-config    Path to config file for kafka CLI auth (optional)
    -h, --help              Show this help message

Environment variables:
    KAFKA_BOOTSTRAP_SERVER  Alternative to --bootstrap-server
    KAFKA_USERNAME          Alternative to --username
    KAFKA_PASSWORD          Alternative to --password
    KAFKA_SASL_MECHANISM    Alternative to --mechanism
    KAFKA_COMMAND_CONFIG    Alternative to --command-config

Examples:
    # Basic usage
    $0 -p my-secret-password

    # Custom bootstrap server
    $0 -b kafka.example.com:9092 -p my-secret-password

    # With existing admin credentials
    $0 -b kafka:9092 -p agent-password -c /path/to/admin.properties

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--bootstrap-server)
            BOOTSTRAP_SERVER="$2"
            shift 2
            ;;
        -u|--username)
            USERNAME="$2"
            shift 2
            ;;
        -p|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -m|--mechanism)
            MECHANISM="$2"
            shift 2
            ;;
        -c|--command-config)
            COMMAND_CONFIG="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate
if [[ -z "$PASSWORD" ]]; then
    echo "Error: Password is required. Use -p or set KAFKA_PASSWORD."
    echo ""
    print_usage
    exit 1
fi

if [[ "$MECHANISM" != "SCRAM-SHA-256" && "$MECHANISM" != "SCRAM-SHA-512" ]]; then
    echo "Error: Mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512"
    exit 1
fi

# Build common args
COMMON_ARGS="--bootstrap-server $BOOTSTRAP_SERVER"
if [[ -n "$COMMAND_CONFIG" ]]; then
    COMMON_ARGS="$COMMON_ARGS --command-config $COMMAND_CONFIG"
fi

echo "============================================"
echo "Streamsight Agent - Kafka User Setup"
echo "============================================"
echo ""
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Username:         $USERNAME"
echo "Mechanism:        $MECHANISM"
echo ""

# Step 1: Create SASL user
echo "[1/4] Creating SASL user..."
kafka-configs.sh $COMMON_ARGS \
    --alter \
    --add-config "$MECHANISM=[password=$PASSWORD]" \
    --entity-type users \
    --entity-name "$USERNAME"
echo "      User '$USERNAME' created."

# Step 2: Grant DESCRIBE on CLUSTER
echo "[2/4] Granting DESCRIBE on CLUSTER..."
kafka-acls.sh $COMMON_ARGS \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --cluster
echo "      CLUSTER DESCRIBE granted."

# Step 3: Grant DESCRIBE on all TOPICs
echo "[3/4] Granting DESCRIBE on all TOPICs..."
kafka-acls.sh $COMMON_ARGS \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --topic '*' \
    --resource-pattern-type prefixed
echo "      TOPIC DESCRIBE granted."

# Step 4: Grant DESCRIBE on all GROUPs
echo "[4/4] Granting DESCRIBE on all GROUPs..."
kafka-acls.sh $COMMON_ARGS \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --group '*' \
    --resource-pattern-type prefixed
echo "      GROUP DESCRIBE granted."

echo ""
echo "============================================"
echo "Setup complete!"
echo "============================================"
echo ""
echo "Configure the agent with:"
echo ""
echo "  KAFKA_BROKERS=$BOOTSTRAP_SERVER"
echo "  KAFKA_SASL_MECHANISM=$MECHANISM"
echo "  KAFKA_SASL_USERNAME=$USERNAME"
echo "  KAFKA_SASL_PASSWORD=<your-password>"
echo "  KAFKA_TLS_ENABLED=true"
echo ""
echo "To verify ACLs:"
echo "  kafka-acls.sh $COMMON_ARGS --list --principal User:$USERNAME"
echo ""
