# Flink gRPC Bridge Application

This is an Apache Flink application that demonstrates integration with a gRPC service for message processing. The application reads messages from a text file source and processes them using the `FlinkWorkerService` gRPC service.

## Architecture

The application consists of:
- **FlinkGrpcApp**: Main Flink application that orchestrates the data flow
- **GrpcMessageProcessor**: A `ProcessFunction` that implements the `OneInputStreamProcessFunction` pattern
- **GrpcClient**: Client for communicating with the gRPC service
- **Proto-generated classes**: Auto-generated from `flink_worker.proto`

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- Apache Flink 2.1.0
- The gRPC service must be running (see the `flink_worker` service)

## Building the Application

1. Navigate to the project directory:
   ```bash
   cd streams/flink_bridge
   ```

2. Build the project:
   ```bash
   mvn clean compile
   ```

   This will:
   - Generate Java classes from the protobuf definition
   - Compile the Java source code
   - Create the JAR file

3. Package the application:
   ```bash
   mvn package
   ```

## Running the Application

### Prerequisites

1. **Start the gRPC service** (from the `flink_worker` directory):
   ```bash
   cd streams/flink_worker
   python -m flink_worker.server
   ```

   The service will start on port 50051 by default.

2. **Start Flink cluster** (using the installed Flink 2.1.0):
   ```bash
   cd flink-2.1.0
   ./bin/start-cluster.sh
   ```

### Submit the Job

1. **Submit the Flink job**:
   ```bash
   cd flink-2.1.0
   ./bin/flink run -c com.sentry.flink_bridge.FlinkGrpcApp \
     ../streams/flink_bridge/target/flink-bridge-1.0.0.jar
   ```

2. **Monitor the job**:
   - Open Flink Web UI: http://localhost:8081
   - Check the job status and logs

### Alternative: Run Locally

You can also run the application locally for development/testing:

```bash
cd streams/flink_bridge
mvn exec:java -Dexec.mainClass="com.sentry.flink_bridge.FlinkGrpcApp"
```

## Configuration

### Input File

The application reads from `input.txt` by default. You can modify the file path in `FlinkGrpcApp.java`:

```java
new Path("input.txt") // Change this path as needed
```

### gRPC Service

The gRPC service connection details can be configured in `GrpcClient.java`:

```java
grpcClient = new GrpcClient("localhost", 50051); // Change host/port as needed
```

## Expected Output

When running successfully, you should see:
1. Messages being read from the input file
2. gRPC service calls for each message
3. Processed messages printed to standard output
4. Logs showing the processing flow

## Troubleshooting

### Common Issues

1. **gRPC service not available**:
   - Ensure the Python gRPC service is running
   - Check the port number (default: 50051)
   - Verify network connectivity

2. **Protobuf compilation errors**:
   - Ensure the proto file is accessible
   - Check Maven dependencies
   - Run `mvn clean compile` to regenerate classes

3. **Flink job submission errors**:
   - Verify Flink cluster is running
   - Check the JAR file path
   - Ensure all dependencies are included

### Logs

The application uses SLF4J for logging. Check the Flink task manager logs for detailed information about the processing.

## Development

### Adding New Features

1. **New message types**: Modify the protobuf definition and regenerate classes
2. **Additional processing**: Extend the `GrpcMessageProcessor` class
3. **Error handling**: Implement custom error handling in the process function
4. **Configuration**: Add configuration parameters for flexibility

### Testing

1. **Unit tests**: Add tests for individual components
2. **Integration tests**: Test the full data flow
3. **gRPC service mocking**: Use mock services for testing without external dependencies

## Dependencies

- **Apache Flink 2.1.0**: Core streaming framework
- **gRPC**: For service communication
- **Protobuf**: For message serialization
- **SLF4J**: For logging

## License

This project is part of the Sentry Streams project.
