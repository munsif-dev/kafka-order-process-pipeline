#!/usr/bin/env bash
set -e

# Local Kafka (KRaft mode) Runner for environments where Docker Desktop is not active
KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_DIR_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TARBALL="${KAFKA_DIR_NAME}.tgz"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TARBALL}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_ROOT="$(dirname "$SCRIPT_DIR")"
KAFKA_HOME="${PIPELINE_ROOT}/.kafka/${KAFKA_DIR_NAME}"

echo "======================================================================"
echo " Starting Apache Kafka (KRaft Mode - Native Local Execution) "
echo "======================================================================"

# Check Java
if ! command -v java >/dev/null 2>&1; then
    echo "ERROR: Java 17+ or 21+ is required but not found in PATH."
    exit 1
fi
echo "Using Java: $(java -version 2>&1 | head -n 1)"

mkdir -p "${PIPELINE_ROOT}/.kafka"

if [ ! -d "$KAFKA_HOME" ]; then
    echo "Kafka binary not found. Downloading Apache Kafka ${KAFKA_VERSION}..."
    cd "${PIPELINE_ROOT}/.kafka"
    if [ ! -f "$KAFKA_TARBALL" ]; then
        curl -SL -o "$KAFKA_TARBALL" "$KAFKA_URL"
    fi
    echo "Extracting ${KAFKA_TARBALL}..."
    tar -xzf "$KAFKA_TARBALL"
    rm -f "$KAFKA_TARBALL"
fi

cd "$KAFKA_HOME"

KRAFT_CONFIG="config/kraft/server.properties"
DATA_DIR="/tmp/kraft-combined-logs"

# If data directory doesn't exist or is empty, format KRaft storage
if [ ! -d "$DATA_DIR" ] || [ -z "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    echo "Formatting KRaft metadata storage in ${DATA_DIR}..."
    CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$KRAFT_CONFIG"
fi

echo "Starting Kafka broker in KRaft mode on localhost:9092..."
echo "Press Ctrl+C to stop."
exec bin/kafka-server-start.sh "$KRAFT_CONFIG"

