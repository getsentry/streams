#!/bin/sh
set -euo pipefail

HERE="$( cd "$( dirname "$0" )" && pwd )"

# URL of the file to download
KAFKA_CONNECTOR_FILE="flink-connector-kafka-3.4.0-1.20.jar"
KAFKA_CONNECTOR="https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.4.0-1.20/${KAFKA_CONNECTOR_FILE}"

KAFKA_CLIENTS_FILE="kafka-clients-3.4.0.jar"
KAFKA_CLIENTS="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/${KAFKA_CLIENTS_FILE}"

# Directory where the file will be saved
DEST_DIR="${HERE}/../flink_libs"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Download the file using curl

download() (
    if [ -f "$1" ]; then
        echo "File already exists: $1"
    else
        echo "Downloading $2"
        curl -o "$1" "$2"
        echo "Download completed: $1"
    fi
)

download "$DEST_DIR/$KAFKA_CONNECTOR_FILE" "$KAFKA_CONNECTOR"
download "$DEST_DIR/$KAFKA_CLIENTS_FILE" "$KAFKA_CLIENTS"
