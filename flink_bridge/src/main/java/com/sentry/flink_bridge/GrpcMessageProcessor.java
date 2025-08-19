package com.sentry.flink_bridge;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.ByteString;
import flink_worker.FlinkWorker;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * ProcessFunction that sends messages to the gRPC service for processing.
 * This implements the OneInputStreamProcessFunction pattern for Flink
 * DataStream API.
 */
public class GrpcMessageProcessor implements OneInputStreamProcessFunction<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageProcessor.class);
    protected GrpcClient grpcClient;

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
            PartitionedContext<String> ctx) throws Exception {
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

            // Send to gRPC service and get response
            List<FlinkWorker.Message> processedMessages = grpcClient.processMessage(message);

            // Process the response and output processed messages
            for (FlinkWorker.Message processedMsg : processedMessages) {
                String processedContent = new String(processedMsg.getPayload().toByteArray(),
                        StandardCharsets.UTF_8);
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
