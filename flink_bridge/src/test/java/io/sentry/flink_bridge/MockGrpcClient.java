package io.sentry.flink_bridge;

import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import flink_worker.FlinkWorker.ProcessWatermarkRequest;
import java.util.List;

/**
 * Mock implementation of GrpcClient for testing purposes.
 * This class can be used across multiple test files to avoid duplication.
 *
 * Supports both direct GrpcClient testing (with ProcessMessageRequest/Response)
 * and indirect testing through GrpcMessageProcessor (with Message objects).
 */
public class MockGrpcClient extends GrpcClient {
    private ProcessMessageResponse nextResponse;
    private Exception nextException;
    private ProcessMessageRequest lastRequest;
    private boolean shutdownCalled = false;

    public MockGrpcClient() {
        super("localhost", 50053);
    }

    /**
     * Set the next response for testing ProcessMessageRequest/Response flow
     */
    public void setNextResponse(ProcessMessageResponse response) {
        this.nextResponse = response;
        this.nextException = null;
    }

    /**
     * Set the next response for testing Message flow (used by GrpcMessageProcessor)
     */
    public void setNextResponse(List<Message> messages) {
        ProcessMessageResponse.Builder builder = ProcessMessageResponse.newBuilder();
        for (Message message : messages) {
            builder.addMessages(message);
        }
        this.nextResponse = builder.build();
        this.nextException = null;
    }

    public void setNextException(Exception exception) {
        this.nextException = exception;
        this.nextResponse = null;
    }

    public ProcessMessageRequest getLastRequest() {
        return lastRequest;
    }

    public boolean isShutdownCalled() {
        return shutdownCalled;
    }

    @Override
    public List<Message> processMessage(Message message, int segment_id) {
        // Simulate the same logic as the real GrpcClient
        try {
            // Construct the request internally (same as real implementation)
            ProcessMessageRequest request = ProcessMessageRequest.newBuilder()
                    .setMessage(message)
                    .build();

            this.lastRequest = request;

            if (nextException != null) {
                throw nextException;
            }

            return nextResponse != null ? nextResponse.getMessagesList() : List.of();
        } catch (Exception e) {
            throw new RuntimeException("Failed to process message via gRPC", e);
        }
    }

    @Override
    public List<Message> processWatermark(long timestamp, java.util.Map<String, String> headers, int segmentId) {
        // Simulate the same logic as the real GrpcClient
        try {
            // Construct the request internally (same as real implementation)
            ProcessWatermarkRequest request = ProcessWatermarkRequest.newBuilder()
                    .setTimestamp(timestamp)
                    .putAllHeaders(headers != null ? headers : new java.util.HashMap<>())
                    .setSegmentId(segmentId)
                    .build();

            // Note: We're reusing the same response mechanism for both message and
            // watermark processing
            // In a more sophisticated mock, you might want separate response handling for
            // watermarks
            if (nextException != null) {
                throw nextException;
            }

            return nextResponse != null ? nextResponse.getMessagesList() : List.of();
        } catch (Exception e) {
            throw new RuntimeException("Failed to process watermark via gRPC", e);
        }
    }

    @Override
    public void shutdown() {
        shutdownCalled = true;
    }
}
