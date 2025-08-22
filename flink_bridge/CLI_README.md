# Flink Bridge gRPC CLI Client

This is a simple Java CLI application that demonstrates the gRPC client functionality by communicating with the Python FlinkWorker gRPC service.

## Features

- Interactive command-line interface
- Send custom messages to the gRPC service
- Built-in test message functionality
- Configurable host and port
- Real-time message processing and response display

## Prerequisites

- Java 24 or later
- Maven 3.6 or later
- Python gRPC server running (see setup below)

## Quick Start

### 1. Start the Python gRPC Server

First, start the Python gRPC server in a separate terminal:

```bash
cd flink_worker
source .venv/bin/activate
python -m flink_worker.server --port 50053
```

### 2. Run the CLI Application

#### Option A: Using the provided script (recommended)

```bash
cd flink_bridge
./run-cli.sh
```

This script will:
- Compile the project
- Build the CLI JAR
- Run the application connecting to localhost:50053

#### Option B: Manual compilation and execution

```bash
cd flink_bridge

# Compile and build
mvn clean package

# Run the CLI application
java -jar target/flink-bridge-cli.jar [host] [port]
```

### 3. Use the CLI

Once running, you'll see an interactive prompt:

```
=== Flink Worker gRPC CLI Client ===
Commands:
  <message>  - Send a message to the gRPC service
  help       - Show this help message
  quit/exit  - Exit the application
  test       - Send a test message

grpc>
```

## CLI Commands

- **Any text message**: Sends the message to the gRPC service
- **`test`**: Sends a predefined test message
- **`help`**: Shows available commands
- **`quit` or `exit`**: Exits the application

## Example Usage

```
grpc> Hello, this is a test message
Response received:
  - Payload: Hello, this is a test message
    Headers: {source=cli, timestamp=1703123456789, message_id=0}
    Timestamp: 1703123456789
  - Payload: Hello, this is a test message
    Headers: {source=cli, timestamp=1703123456789, message_id=0, processed=true, segment_id=0}
    Timestamp: 1703123456789

grpc> test
Sending test message: This is a test message from the CLI client
Response received:
  - Payload: This is a test message from the CLI client
    Headers: {source=cli, timestamp=1703123456790, message_id=1}
    Timestamp: 1703123456790
  - Payload: This is a test message from the CLI client
    Headers: {source=cli, timestamp=1703123456790, message_id=1, processed=true, segment_id=1}
    Timestamp: 1703123456790

grpc> quit
Goodbye!
```

## Configuration

### Command Line Arguments

- **Host**: First argument (default: `localhost`)
- **Port**: Second argument (default: `50053`)

Examples:
```bash
# Connect to localhost:50053 (default)
java -jar target/flink-bridge-cli.jar

# Connect to specific host and port
java -jar target/flink-bridge-cli.jar 192.168.1.100 50051

# Connect to specific host with default port
java -jar target/flink-bridge-cli.jar my-server.com
```

## Architecture

The CLI application uses the same `GrpcClient` class that the Flink application uses, demonstrating:

1. **gRPC Communication**: Direct communication with the Python service
2. **Message Processing**: Sending and receiving protobuf messages
3. **Error Handling**: Graceful error handling and user feedback
4. **Resource Management**: Proper cleanup of gRPC connections

## Troubleshooting

### Common Issues

1. **"gRPC service is not available"**
   - Make sure the Python server is running
   - Check the host and port configuration
   - Verify the server is accessible from your network

2. **"Connection refused"**
   - Check if the server is running on the specified port
   - Verify firewall settings
   - Ensure the server is binding to the correct interface

3. **"Invalid port number"**
   - Port must be a valid integer between 1-65535
   - Check the command line arguments

### Debug Mode

To see more detailed logging, you can set the log level:

```bash
export SLF4J_SIMPLE_LOG_LEVEL=DEBUG
java -jar target/flink-bridge-cli.jar
```

## Development

### Building

```bash
mvn clean compile    # Compile only
mvn clean package    # Compile and package
mvn clean install    # Compile, package, and install to local repo
```

### Project Structure

- `GrpcCliApp.java` - Main CLI application
- `GrpcClient.java` - gRPC client abstraction
- `run-cli.sh` - Convenience script for running the CLI
- `pom.xml` - Maven configuration with CLI build profile

### Adding New Features

The CLI application is designed to be easily extensible. You can:

1. Add new commands in the `runInteractiveCli` method
2. Enhance message processing in the `sendMessage` method
3. Add configuration options for different gRPC service features
4. Implement batch processing or file input capabilities
