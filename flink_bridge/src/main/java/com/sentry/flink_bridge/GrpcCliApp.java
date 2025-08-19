package com.sentry.flink_bridge;

import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.List;

/**
 * Simple CLI application that demonstrates the gRPC client functionality.
 * This program allows users to send messages to the FlinkWorker gRPC service
 * and see the responses.
 */
public class GrpcCliApp {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcCliApp.class);
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 50053;

    public static void main(String[] args) {
        // Parse command line arguments
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                LOG.error("Invalid port number: {}. Using default port {}", args[1], DEFAULT_PORT);
                port = DEFAULT_PORT;
            }
        }

        LOG.info("Starting gRPC CLI client connecting to {}:{}", host, port);

        GrpcClient client = null;
        try {
            // Create the gRPC client
            client = new GrpcClient(host, port);

            // Check if the service is available
            if (!client.isAvailable()) {
                LOG.error("gRPC service is not available at {}:{}", host, port);
                LOG.error("Make sure the Python gRPC server is running with:");
                LOG.error("cd flink_worker && source .venv/bin/activate && python -m flink_worker.server --port {}",
                        port);
                System.exit(1);
            }

            LOG.info("Successfully connected to gRPC service at {}:{}", host, port);

            // Start interactive CLI
            runInteractiveCli(client);

        } catch (Exception e) {
            LOG.error("Error in gRPC CLI application", e);
            System.exit(1);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Runs the interactive command-line interface.
     */
    private static void runInteractiveCli(GrpcClient client) {
        Scanner scanner = new Scanner(System.in);
        int messageCounter = 0;

        System.out.println("\n=== Flink Worker gRPC CLI Client ===");
        System.out.println("Commands:");
        System.out.println("  <message>  - Send a message to the gRPC service");
        System.out.println("  help       - Show this help message");
        System.out.println("  quit/exit  - Exit the application");
        System.out.println("  test       - Send a test message");
        System.out.println();

        while (true) {
            System.out.print("grpc> ");
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                continue;
            }

            if (input.equalsIgnoreCase("quit") || input.equalsIgnoreCase("exit")) {
                System.out.println("Goodbye!");
                break;
            }

            if (input.equalsIgnoreCase("help")) {
                System.out.println("Commands:");
                System.out.println("  <message>  - Send a message to the gRPC service");
                System.out.println("  help       - Show this help message");
                System.out.println("  quit/exit  - Exit the application");
                System.out.println("  test       - Send a test message");
                continue;
            }

            if (input.equalsIgnoreCase("test")) {
                input = "This is a test message from the CLI client";
                System.out.println("Sending test message: " + input);
            }

            try {
                // Send the message to the gRPC service
                List<Message> processedMessages = sendMessage(client, input, messageCounter++);

                // Display the response
                System.out.println("Response received:");
                for (Message msg : processedMessages) {
                    String payload = new String(msg.getPayload().toByteArray(), StandardCharsets.UTF_8);
                    System.out.println("  - Payload: " + payload);
                    System.out.println("    Headers: " + msg.getHeadersMap());
                    System.out.println("    Timestamp: " + msg.getTimestamp());
                }

            } catch (Exception e) {
                LOG.error("Error processing message: {}", input, e);
                System.out.println("Error: " + e.getMessage());
            }

            System.out.println();
        }

        scanner.close();
    }

    /**
     * Sends a message to the gRPC service.
     */
    private static List<Message> sendMessage(GrpcClient client, String messageText, int segmentId) {
        // Create the message
        Message message = Message.newBuilder()
                .setPayload(ByteString.copyFrom(messageText.getBytes(StandardCharsets.UTF_8)))
                .putHeaders("source", "cli")
                .putHeaders("timestamp", String.valueOf(System.currentTimeMillis()))
                .putHeaders("message_id", String.valueOf(segmentId))
                .setTimestamp(System.currentTimeMillis())
                .build();

        // Send to gRPC service
        return client.processMessage(message);
    }
}
