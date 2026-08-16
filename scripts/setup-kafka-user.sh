#!/bin/bash
#
# Creates a read-only Kafka user for the Streamsight agent: one SCRAM
# credential and the three DESCRIBE ACLs the agent needs. Nothing else.
#
# Usage: ./setup-kafka-user.sh -p <password> [options]
#
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
USERNAME="${KAFKA_USERNAME:-streamsight-agent}"
PASSWORD="${KAFKA_PASSWORD:-}"
MECHANISM="${KAFKA_SASL_MECHANISM:-SCRAM-SHA-512}"
COMMAND_CONFIG="${KAFKA_COMMAND_CONFIG:-}"
DRY_RUN=false

print_usage() {
    cat << EOF
Usage: $0 [options]

Creates a read-only Kafka user for the Streamsight agent with minimal ACLs:
DESCRIBE on CLUSTER, DESCRIBE on all TOPICs, DESCRIBE on all GROUPs.

Options:
    -b, --bootstrap-server  Kafka bootstrap server (default: localhost:9092)
    -u, --username          Username to create (default: streamsight-agent)
    -p, --password          Password for the user (required)
    -m, --mechanism         SCRAM-SHA-256 or SCRAM-SHA-512 (default: SCRAM-SHA-512)
    -c, --command-config    Properties file with the ADMIN credentials the CLI
                            uses to connect. Required on any secured cluster.
    -n, --dry-run           Print the commands instead of running them
    -h, --help              Show this help

Environment variables:
    KAFKA_BOOTSTRAP_SERVER, KAFKA_USERNAME, KAFKA_PASSWORD,
    KAFKA_SASL_MECHANISM, KAFKA_COMMAND_CONFIG

Examples:
    $0 -p my-secret-password
    $0 -b kafka.example.com:9093 -p my-secret-password -c /path/to/admin.properties
    $0 -p x -n                      # show exactly what would be granted

Requires the Kafka CLI tools on PATH. Both naming conventions are supported:
kafka-acls.sh (Apache tarball) and kafka-acls (Confluent/Bitnami images).
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--bootstrap-server) BOOTSTRAP_SERVER="$2"; shift 2 ;;
        -u|--username)         USERNAME="$2"; shift 2 ;;
        -p|--password)         PASSWORD="$2"; shift 2 ;;
        -m|--mechanism)        MECHANISM="$2"; shift 2 ;;
        -c|--command-config)   COMMAND_CONFIG="$2"; shift 2 ;;
        -n|--dry-run)          DRY_RUN=true; shift ;;
        -h|--help)             print_usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; print_usage; exit 1 ;;
    esac
done

if [[ -z "$PASSWORD" ]]; then
    echo "Error: password is required. Use -p or set KAFKA_PASSWORD." >&2
    exit 1
fi

if [[ "$MECHANISM" != "SCRAM-SHA-256" && "$MECHANISM" != "SCRAM-SHA-512" ]]; then
    echo "Error: mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512" >&2
    exit 1
fi

# The Apache tarball ships kafka-acls.sh; the Confluent and Bitnami images ship
# kafka-acls. Resolve whichever exists rather than assuming.
resolve_tool() {
    local base="$1"
    if command -v "${base}.sh" > /dev/null 2>&1; then
        echo "${base}.sh"
    elif command -v "$base" > /dev/null 2>&1; then
        echo "$base"
    else
        echo "Error: neither ${base}.sh nor ${base} is on PATH." >&2
        echo "Run this on a broker, or inside a Kafka image (see README)." >&2
        exit 1
    fi
}

KAFKA_CONFIGS="$(resolve_tool kafka-configs)"
KAFKA_ACLS="$(resolve_tool kafka-acls)"

COMMON_ARGS=(--bootstrap-server "$BOOTSTRAP_SERVER")
if [[ -n "$COMMAND_CONFIG" ]]; then
    COMMON_ARGS+=(--command-config "$COMMAND_CONFIG")
fi

run() {
    if [[ "$DRY_RUN" == true ]]; then
        printf '  %q' "$@"; printf '\n'
    else
        "$@"
    fi
}

echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Username:         $USERNAME"
echo "Mechanism:        $MECHANISM"
echo

echo "[1/4] Creating SASL user..."
run "$KAFKA_CONFIGS" "${COMMON_ARGS[@]}" \
    --alter \
    --add-config "$MECHANISM=[password=$PASSWORD]" \
    --entity-type users \
    --entity-name "$USERNAME"

echo "[2/4] Granting DESCRIBE on CLUSTER..."
run "$KAFKA_ACLS" "${COMMON_ARGS[@]}" \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --cluster

# --resource-pattern-type literal with the name '*' is Kafka's wildcard and
# matches every topic. `prefixed` with '*' would be a prefix match on the
# asterisk character itself, i.e. it would match nothing on a normal cluster.
echo "[3/4] Granting DESCRIBE on all TOPICs..."
run "$KAFKA_ACLS" "${COMMON_ARGS[@]}" \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --topic '*' \
    --resource-pattern-type literal

echo "[4/4] Granting DESCRIBE on all GROUPs..."
run "$KAFKA_ACLS" "${COMMON_ARGS[@]}" \
    --add \
    --allow-principal "User:$USERNAME" \
    --operation DESCRIBE \
    --group '*' \
    --resource-pattern-type literal

echo
echo "Done. Configure the agent with:"
echo
echo "  KAFKA_BROKERS=$BOOTSTRAP_SERVER"
echo "  KAFKA_SASL_MECHANISM=$MECHANISM"
echo "  KAFKA_SASL_USERNAME=$USERNAME"
echo "  KAFKA_SASL_PASSWORD=<the password you passed>"
echo "  KAFKA_TLS_ENABLED=true    # if the listener is SASL_SSL"
echo
echo "Verify with:"
echo "  $KAFKA_ACLS ${COMMON_ARGS[*]} --list --principal User:$USERNAME"
