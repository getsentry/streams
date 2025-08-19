package com.sentry.flink_bridge;

import com.google.protobuf.ByteString;
import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GrpcMessageProcessor class.
 * Tests the message processing functionality with manually mocked dependencies.
 */
class GrpcMessageProcessorTest {

    private TestGrpcMessageProcessor processor;
    private MockGrpcClient mockGrpcClient;
    private MockCollector mockCollector;

    @BeforeEach
    void setUp() {
        processor = new TestGrpcMessageProcessor();
        mockGrpcClient = new MockGrpcClient();
        mockCollector = new MockCollector();
    }

    @Test
    void testProcessRecord_SuccessfulProcessing() throws Exception {
        // Arrange
        String inputMessage = "test message";
        Message grpcMessage = createTestMessage("processed message 1");
        mockGrpcClient.setNextResponse(List.of(grpcMessage));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        assertEquals("processed message 1", mockCollector.getCollectedItems().get(0));
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_MultipleProcessedMessages() throws Exception {
        // Arrange
        String inputMessage = "test message";
        Message grpcMessage1 = createTestMessage("processed message 1");
        Message grpcMessage2 = createTestMessage("processed message 2");
        mockGrpcClient.setNextResponse(List.of(grpcMessage1, grpcMessage2));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(2, mockCollector.getCollectedItems().size());
        assertEquals("processed message 1", mockCollector.getCollectedItems().get(0));
        assertEquals("processed message 2", mockCollector.getCollectedItems().get(1));
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_EmptyResponse() throws Exception {
        // Arrange
        String inputMessage = "test message";
        mockGrpcClient.setNextResponse(List.of());
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(0, mockCollector.getCollectedItems().size());
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_GrpcClientException() throws Exception {
        // Arrange
        String inputMessage = "test message";
        RuntimeException testException = new RuntimeException("gRPC service error");
        mockGrpcClient.setNextException(testException);
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, null);

        // Assert
        // Should not throw exception, just log error and continue
        assertEquals(0, mockCollector.getCollectedItems().size());
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_MessageConstruction() throws Exception {
        // Arrange
        String inputMessage = "test message";
        Message grpcMessage = createTestMessage("response");
        mockGrpcClient.setNextResponse(List.of(grpcMessage));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, null);

        // Assert
        ProcessMessageRequest lastRequest = mockGrpcClient.getLastRequest();
        assertNotNull(lastRequest);

        // Verify the message was constructed correctly
        Message message = lastRequest.getMessage();
        String payload = new String(message.getPayload().toByteArray(), StandardCharsets.UTF_8);
        assertEquals(inputMessage, payload);
        assertTrue(message.getHeadersMap().containsKey("source"));
        assertTrue(message.getHeadersMap().containsKey("timestamp"));
        assertTrue(message.getTimestamp() > 0);
    }

    @Test
    void testClose_ShutsDownGrpcClient() throws Exception {
        // Arrange
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.close();

        // Assert
        assertTrue(mockGrpcClient.isShutdownCalled());
    }

    @Test
    void testClose_GrpcClientIsNull() throws Exception {
        // Act & Assert - should not throw exception
        assertDoesNotThrow(() -> processor.close());
    }

    /**
     * Testable version of GrpcMessageProcessor that allows dependency injection
     * for testing purposes.
     */
    private static class TestGrpcMessageProcessor extends GrpcMessageProcessor {
        private GrpcClient testGrpcClient;

        public void setGrpcClient(GrpcClient client) {
            this.testGrpcClient = client;
            this.grpcClient = client;
        }

        public GrpcClient getGrpcClient() {
            return this.grpcClient;
        }
    }

    /**
     * Mock implementation of Collector for testing.
     */
    private static class MockCollector implements org.apache.flink.datastream.api.common.Collector<String> {
        private final List<String> collectedItems = new java.util.ArrayList<>();

        @Override
        public void collect(String record) {
            collectedItems.add(record);
        }

        @Override
        public void collectAndOverwriteTimestamp(String record, long timestamp) {
            collect(record);
        }

        public List<String> getCollectedItems() {
            return collectedItems;
        }
    }

    // Helper methods for creating test data
    private Message createTestMessage(String payload) {
        return Message.newBuilder()
                .setPayload(ByteString.copyFrom(payload.getBytes(StandardCharsets.UTF_8)))
                .putHeaders("source", "test")
                .putHeaders("timestamp", String.valueOf(System.currentTimeMillis()))
                .setTimestamp(System.currentTimeMillis())
                .build();
    }
}
