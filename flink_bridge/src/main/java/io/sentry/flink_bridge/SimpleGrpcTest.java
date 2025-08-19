package io.sentry.flink_bridge;

import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import flink_worker.FlinkWorkerServiceGrpc;

import java.nio.charset.StandardCharsets;

/**
 * Simple test program to debug gRPC connection issues.
 */
public class SimpleGrpcTest {

    public static void main(String[] args) {
        System.out.println("Starting simple gRPC test...");

        String host = "127.0.0.1";
        int port = 50053;

        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[1]);
                System.exit(1);
            }
        }

        System.out.println("Connecting to " + host + ":" + port);

        ManagedChannel channel = null;
        try {
            // Try different connection methods
            System.out.println("Method 1: Using forAddress...");
            channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            System.out.println("Channel created successfully!");

            // Test the connection
            FlinkWorkerServiceGrpc.FlinkWorkerServiceBlockingStub stub = FlinkWorkerServiceGrpc
                    .newBlockingStub(channel);

            // Create a simple test message
            Message message = Message.newBuilder()
                    .setPayload(ByteString.copyFrom("Hello from Java!".getBytes(StandardCharsets.UTF_8)))
                    .putHeaders("source", "test")
                    .setTimestamp(System.currentTimeMillis())
                    .build();

            ProcessMessageRequest request = ProcessMessageRequest.newBuilder()
                    .setMessage(message)
                    .setSegmentId(0)
                    .build();

            System.out.println("Sending test message...");
            ProcessMessageResponse response = stub.processMessage(request);

            System.out.println("Success! Received response with " + response.getMessagesCount() + " messages");

            for (Message msg : response.getMessagesList()) {
                String payload = new String(msg.getPayload().toByteArray(), StandardCharsets.UTF_8);
                System.out.println("  - " + payload);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (channel != null) {
                channel.shutdown();
            }
        }
    }
}
