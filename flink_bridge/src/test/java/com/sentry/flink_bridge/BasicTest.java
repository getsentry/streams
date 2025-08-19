package com.sentry.flink_bridge;

import com.google.protobuf.ByteString;
import flink_worker.FlinkWorker.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Basic unit test to verify the testing infrastructure is working.
 * Also demonstrates testing the GrpcClient class.
 */
class BasicTest {

    private MockGrpcClient mockGrpcClient;

    @BeforeEach
    void setUp() {
        mockGrpcClient = new MockGrpcClient();
    }

    @Test
    void testBasicAssertion() {
        // Basic assertion to verify tests are running
        assertEquals(1, 1, "One should equal one");
    }

    @Test
    void testTrueAssertion() {
        // Another simple assertion
        assertTrue(true, "True should be true");
    }

    @Test
    void testStringAssertion() {
        // String assertion
        String expected = "Hello";
        String actual = "Hello";
        assertEquals(expected, actual, "Strings should be equal");
    }

    @Test
    void testGrpcClientProcessMessage_Success() {
        // Arrange
        Message inputMessage = createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse(
                createTestMessage("processed message 1"),
                createTestMessage("processed message 2")));

        // Act
        List<Message> result = mockGrpcClient.processMessage(inputMessage);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("processed message 1", getMessagePayload(result.get(0)));
        assertEquals("processed message 2", getMessagePayload(result.get(1)));

        // Verify the request was processed
        assertNotNull(mockGrpcClient.getLastRequest());
        assertEquals(inputMessage, mockGrpcClient.getLastRequest().getMessage());
    }

    @Test
    void testGrpcClientProcessMessage_EmptyResponse() {
        // Arrange
        Message inputMessage = createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse());

        // Act
        List<Message> result = mockGrpcClient.processMessage(inputMessage);

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGrpcClientProcessMessage_Exception() {
        // Arrange
        Message inputMessage = createTestMessage("test message");
        RuntimeException testException = new RuntimeException("Test error");
        mockGrpcClient.setNextException(testException);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> mockGrpcClient.processMessage(inputMessage));

        assertEquals("Failed to process message via gRPC", exception.getMessage());
        assertEquals(testException, exception.getCause());
    }

    @Test
    void testGrpcClientProcessMessage_RequestConstruction() {
        // Arrange
        Message inputMessage = createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse(createTestMessage("response")));

        // Act
        mockGrpcClient.processMessage(inputMessage);

        // Verify the request was constructed correctly
        ProcessMessageRequest lastRequest = mockGrpcClient.getLastRequest();
        assertNotNull(lastRequest);
        assertEquals(inputMessage, lastRequest.getMessage());
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

    private ProcessMessageResponse createMockResponse(Message... messages) {
        ProcessMessageResponse.Builder builder = ProcessMessageResponse.newBuilder();
        for (Message message : messages) {
            builder.addMessages(message);
        }
        return builder.build();
    }

    private String getMessagePayload(Message message) {
        return new String(message.getPayload().toByteArray(), StandardCharsets.UTF_8);
    }
}
