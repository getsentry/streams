package com.sentry.flink_bridge;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Flink application that reads messages from a text file and processes them
 * using a gRPC service.
 * This demonstrates the integration between Apache Flink and gRPC services.
 */
public class FlinkGrpcApp {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkGrpcApp.class);

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        env.setExecutionMode(RuntimeExecutionMode.STREAMING);

        // Create a data stream from a text file using Flink 2.1.0 API
        NonKeyedPartitionStream<String> textStream = env.fromSource(
                DataStreamV2SourceUtils.fromData(
                        Arrays.asList(new String[] {
                                "Hello World",
                                "This is a test message",
                                "Another message for processing",
                                "Flink gRPC integration test",
                                "Processing stream data with external service"
                        })),
                "in memory list");

        // Apply the gRPC processing function
        NonKeyedPartitionStream<String> processedStream = textStream
                .process(new GrpcMessageProcessor());

        // Print the processed messages to standard output
        processedStream.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");

        // Execute the Flink job
        LOG.info("Starting Flink gRPC application...");
        env.execute("Flink gRPC Message Processing Job");
    }

}
