#!/bin/bash

# Flink Bridge gRPC CLI Runner
# This script compiles and runs the Java CLI application

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Flink Bridge gRPC CLI Runner ===${NC}"

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}Error: Maven is not installed or not in PATH${NC}"
    echo "Please install Maven and try again"
    exit 1
fi

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo -e "${RED}Error: Java is not installed or not in PATH${NC}"
    echo "Please install Java and try again"
    exit 1
fi

echo -e "${YELLOW}Compiling the project...${NC}"
mvn clean compile

echo -e "${YELLOW}Building the CLI application...${NC}"
mvn package

# Check if the CLI JAR was created
CLI_JAR="target/flink-bridge-cli.jar"
if [ ! -f "$CLI_JAR" ]; then
    echo -e "${RED}Error: CLI JAR file not found at $CLI_JAR${NC}"
    exit 1
fi

echo -e "${GREEN}CLI application built successfully!${NC}"
echo -e "${YELLOW}Usage:${NC}"
echo "  $0 [host] [port]"
echo "  Default: localhost:50053"
echo ""
echo -e "${YELLOW}Make sure the Python gRPC server is running:${NC}"
echo "  cd ../flink_worker && source .venv/bin/activate && python -m flink_worker.server --port 50053"
echo ""

# Parse command line arguments
HOST=${1:-localhost}
PORT=${2:-50053}

echo -e "${GREEN}Starting CLI application connecting to $HOST:$PORT...${NC}"
echo ""

# Run the CLI application
java -jar "$CLI_JAR" "$HOST" "$PORT"
