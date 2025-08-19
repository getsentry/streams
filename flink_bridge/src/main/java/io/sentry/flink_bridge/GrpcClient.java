package io.sentry.flink_bridge;

import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import flink_worker.FlinkWorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for communicating with the FlinkWorkerService.
 * This client handles the connection and communication with the gRPC service.
 */
public class GrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcClient.class);

    private final ManagedChannel channel;
    private final FlinkWorkerServiceGrpc.FlinkWorkerServiceBlockingStub blockingStub;

    /**
     * Constructs a gRPC client for the specified host and port.
     *
     * @param host the hostname of the gRPC service
     * @param port the port number of the gRPC service
     */
    public GrpcClient(String host, int port) {
        LOG.info("Creating gRPC client for {}:{}", host, port);
        try {
            // Use DNS resolver with the format dns:///host:port
            String target = "dns:///" + host + ":" + port;
            LOG.info("Using target: {}", target);

            ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                    .usePlaintext()
                    .maxInboundMessageSize(1024 * 1024)
                    .build();

            this.channel = channel;
            this.blockingStub = FlinkWorkerServiceGrpc.newBlockingStub(channel);
            LOG.info("gRPC client created successfully");
        } catch (Exception e) {
            LOG.error("Failed to create gRPC client for {}:{}", host, port, e);
            throw new RuntimeException("Failed to create gRPC client", e);
        }
    }

    /**
     * Sends a message processing request to the gRPC service.
     *
     * @param message the message to process
     * @return a list of processed messages
     * @throws RuntimeException if the gRPC call fails
     */
    public List<Message> processMessage(Message message) {
        try {
            // Construct the request internally
            ProcessMessageRequest request = ProcessMessageRequest.newBuilder()
                    .setMessage(message)
                    .build();

            LOG.debug("Sending request to gRPC service: {}", request);
            ProcessMessageResponse response = blockingStub.processMessage(request);
            LOG.debug("Received response from gRPC service: {} messages",
                    response.getMessagesCount());
            return response.getMessagesList();
        } catch (Exception e) {
            LOG.error("Error calling gRPC service", e);
            throw new RuntimeException("Failed to process message via gRPC", e);
        }
    }

    /**
     * Shuts down the gRPC client and closes the channel.
     * This method should be called when the client is no longer needed.
     */
    public void shutdown() {
        try {
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                LOG.info("gRPC client shutdown completed");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while shutting down gRPC client", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Checks if the gRPC client is available and responsive.
     *
     * @return true if the service is available, false otherwise
     */
    public boolean isAvailable() {
        try {
            // Try to send a simple request to check availability
            // For now, we'll just check if the channel is ready
            return channel != null && !channel.isShutdown() && !channel.isTerminated();
        } catch (Exception e) {
            LOG.debug("Service availability check failed", e);
            return false;
        }
    }
}
