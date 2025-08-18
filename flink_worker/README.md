# Flink Worker gRPC Service

A Python gRPC service for processing messages in a Flink environment.

## Features

- **gRPC Service**: Implements the `FlinkWorkerService` with a `ProcessMessage` method
- **Message Processing**: Takes a message with payload, headers, and timestamp, plus a segment_id
- **Extensible**: The Message class can be subclassed for custom functionality
- **CLI Client**: Includes a test client for easy testing

## Installation

1. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -e .
```

## Usage

### Starting the Server

```bash
# Start the server on default port 50051
python -m flink_worker.server

# Start on a custom port
python -m flink_worker.server --port 50052

# Start on a specific host
python -m flink_worker.server --host 0.0.0.0 --port 50051
```

### Using the CLI Client

```bash
# Basic usage with defaults
python -m flink_worker.client

# Custom message
python -m flink_worker.client --payload "Custom message" --segment-id 42

# Custom headers
python -m flink_worker.client --headers '{"source": "test", "priority": "high"}'

# Connect to remote server
python -m flink_worker.client --host 192.168.1.100 --port 50051
```

### Programmatic Usage

```python
import grpc
from flink_worker_pb2 import Message, ProcessMessageRequest
from flink_worker_pb2_grpc import FlinkWorkerServiceStub

# Create a channel
channel = grpc.insecure_channel('localhost:50051')
stub = FlinkWorkerServiceStub(channel)

# Create a message
message = Message(
    payload=b"Hello, World!",
    headers={"source": "python", "type": "test"},
    timestamp=1234567890
)

# Create request
request = ProcessMessageRequest(
    message=message,
    segment_id=1
)

# Process the message
response = stub.ProcessMessage(request)

# Handle response
for msg in response.messages:
    print(f"Processed message: {msg.payload}")
```

## API Reference

### Message

- `payload`: bytes - The message content
- `headers`: map<string, string> - Key-value metadata
- `timestamp`: int64 - Unix timestamp

### ProcessMessageRequest

- `message`: Message - The message to process
- `segment_id`: uint32 - Segment identifier

### ProcessMessageResponse

- `messages`: repeated Message - List of processed messages

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black .
isort .
```

### Type Checking

```bash
mypy .
```

## Architecture

The service is designed to be extensible:

1. **Base Message Class**: The protobuf-generated `Message` class can be subclassed
2. **Service Implementation**: `FlinkWorkerService` handles the gRPC calls
3. **Processing Logic**: Currently returns the original message plus a processed copy
4. **Error Handling**: Graceful error handling with proper gRPC status codes

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
