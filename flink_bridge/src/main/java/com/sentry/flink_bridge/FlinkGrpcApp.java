package com.sentry.flink_bridge;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import flink_worker.FlinkWorker;
import org.apache.flink.datastream.api.common.Collector;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Flink application that reads messages from a text file and processes them using a gRPC service.
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
                        Arrays.asList(new String[]{
                                "Hello World",
                                "This is a test message",
                                "Another message for processing",
                                "Flink gRPC integration test",
                                "Processing stream data with external service"
                        })
                ),
                "in memory list"
        );

        // Apply the gRPC processing function
        NonKeyedPartitionStream<String> processedStream = textStream
            .process(new GrpcMessageProcessor());

        // Print the processed messages to standard output
        processedStream.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");


        // Execute the Flink job
        LOG.info("Starting Flink gRPC application...");
        env.execute("Flink gRPC Message Processing Job");
    }

    /**
     * ProcessFunction that sends messages to the gRPC service for processing.
     * This implements the OneInputStreamProcessFunction pattern as requested.
     */
    public static class GrpcMessageProcessor implements OneInputStreamProcessFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageProcessor.class);
        private GrpcClient grpcClient;

        @Override
        public void open(NonPartitionedContext<String> ctx) throws Exception {
            // Initialize the gRPC client
            grpcClient = new GrpcClient("localhost", 50053);
            LOG.info("gRPC client initialized");
        }

        @Override
        public void processRecord(
                String record,
                Collector<String> out,
                PartitionedContext<String> ctx
        ) throws Exception {
            try {
                LOG.info("Processing message: {}", record);

                // Create a message for the gRPC service
                FlinkWorker.Message message;
                message = FlinkWorker.Message.newBuilder()
                        .setPayload(ByteString.copyFrom(record.getBytes(StandardCharsets.UTF_8)))
                        .putHeaders("source", "flink")
                        .putHeaders("timestamp", String.valueOf(System.currentTimeMillis()))
                        .setTimestamp(System.currentTimeMillis())
                        .build();

                // Create the processing request
                FlinkWorker.ProcessMessageRequest request = FlinkWorker.ProcessMessageRequest.newBuilder()
                        .setMessage(message)
                        .setSegmentId(0) // Simple segment ID for now
                        .build();

                // Send to gRPC service and get response
                FlinkWorker.ProcessMessageResponse response = grpcClient.processMessage(request);

                // Process the response and output processed messages
                for (FlinkWorker.Message processedMsg : response.getMessagesList()) {
                    String processedContent = new String(processedMsg.getPayload().toByteArray(), StandardCharsets.UTF_8);
                    LOG.info("Received processed message: {}", processedContent);
                    out.collect(processedContent);

                }

            } catch (Exception e) {
                LOG.error("Error processing message: {}", record, e);
                // In a production environment, you might want to handle errors differently
                // For now, we'll just log the error and continue
            }
        }

        @Override
        public void close() throws Exception {
            if (grpcClient != null) {
                grpcClient.shutdown();

            }
        }
    }

}
