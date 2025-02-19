#!/usr/bin/env bash
set -euo pipefail

HERE="$( cd "$( dirname "$0" )" && pwd )"

# URL of the file to download
KAFKA_CONNECTOR_FILE="flink-sql-connector-kafka-3.4.0-1.20.jar"
KAFKA_CONNECTOR="https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/${KAFKA_CONNECTOR_FILE}"

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
