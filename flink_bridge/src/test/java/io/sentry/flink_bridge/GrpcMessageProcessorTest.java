package io.sentry.flink_bridge;

import com.google.protobuf.ByteString;
import io.sentry.flink_bridge.Message;
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
    private TestUtils.MockMessageCollector mockCollector;
    private TestUtils.MockPartitionedContext mockContext;

    @BeforeEach
    void setUp() {
        processor = new TestGrpcMessageProcessor();
        mockGrpcClient = new MockGrpcClient();
        mockCollector = new TestUtils.MockMessageCollector();
        mockContext = TestUtils.createMockPartitionedContext();
    }

    @Test
    void testProcessRecord_SuccessfulProcessing() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        flink_worker.FlinkWorker.Message grpcMessage = TestUtils.createTestProtoMessage("processed message 1");
        mockGrpcClient.setNextResponse(List.of(grpcMessage));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, mockContext);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        Message result = mockCollector.getCollectedItems().get(0);
        assertEquals("processed message 1", new String(result.getPayload(), StandardCharsets.UTF_8));
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_MultipleProcessedMessages() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        flink_worker.FlinkWorker.Message grpcMessage1 = TestUtils.createTestProtoMessage("processed message 1");
        flink_worker.FlinkWorker.Message grpcMessage2 = TestUtils.createTestProtoMessage("processed message 2");
        mockGrpcClient.setNextResponse(List.of(grpcMessage1, grpcMessage2));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, mockContext);

        // Assert
        assertEquals(2, mockCollector.getCollectedItems().size());
        Message result1 = mockCollector.getCollectedItems().get(0);
        Message result2 = mockCollector.getCollectedItems().get(1);
        assertEquals("processed message 1", new String(result1.getPayload(), StandardCharsets.UTF_8));
        assertEquals("processed message 2", new String(result2.getPayload(), StandardCharsets.UTF_8));
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_EmptyResponse() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        mockGrpcClient.setNextResponse(List.of());
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, mockContext);

        // Assert
        assertEquals(0, mockCollector.getCollectedItems().size());
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_GrpcClientException() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        RuntimeException testException = new RuntimeException("gRPC service error");
        mockGrpcClient.setNextException(testException);
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, mockContext);

        // Assert
        // Should not throw exception, just log error and continue
        assertEquals(0, mockCollector.getCollectedItems().size());
        assertNotNull(mockGrpcClient.getLastRequest());
    }

    @Test
    void testProcessRecord_MessageConstruction() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        flink_worker.FlinkWorker.Message grpcMessage = TestUtils.createTestProtoMessage("response");
        mockGrpcClient.setNextResponse(List.of(grpcMessage));
        processor.setGrpcClient(mockGrpcClient);

        // Act
        processor.processRecord(inputMessage, mockCollector, mockContext);

        // Assert
        ProcessMessageRequest lastRequest = mockGrpcClient.getLastRequest();
        assertNotNull(lastRequest);

        // Verify the message was constructed correctly
        flink_worker.FlinkWorker.Message message = lastRequest.getMessage();
        String payload = new String(message.getPayload().toByteArray(), StandardCharsets.UTF_8);
        assertEquals("test message", payload);
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
}
