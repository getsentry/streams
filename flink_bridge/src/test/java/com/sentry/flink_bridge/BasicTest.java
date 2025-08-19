package com.sentry.flink_bridge;

import com.google.protobuf.ByteString;
import com.sentry.flink_bridge.Message;
import flink_worker.FlinkWorker.ProcessMessageRequest;
import flink_worker.FlinkWorker.ProcessMessageResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import flink_worker.FlinkWorker;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
        Message inputMessage = TestUtils.createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse(
                TestUtils.createTestMessage("processed message 1"),
                TestUtils.createTestMessage("processed message 2")));

        // Act
        List<FlinkWorker.Message> result = mockGrpcClient.processMessage(inputMessage.toProto());

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("processed message 1", getMessagePayload(result.get(0)));
        assertEquals("processed message 2", getMessagePayload(result.get(1)));

        // Verify the request was processed
        assertNotNull(mockGrpcClient.getLastRequest());
        assertTrue(Arrays.equals(inputMessage.getPayload(),
                mockGrpcClient.getLastRequest().getMessage().getPayload().toByteArray()));
    }

    @Test
    void testGrpcClientProcessMessage_EmptyResponse() {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse());

        // Act
        List<flink_worker.FlinkWorker.Message> result = mockGrpcClient.processMessage(inputMessage.toProto());

        // Assert
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testGrpcClientProcessMessage_Exception() {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        RuntimeException testException = new RuntimeException("Test error");
        mockGrpcClient.setNextException(testException);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> mockGrpcClient.processMessage(inputMessage.toProto()));

        assertEquals("Failed to process message via gRPC", exception.getMessage());
        assertEquals(testException, exception.getCause());
    }

    @Test
    void testGrpcClientProcessMessage_RequestConstruction() {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");
        mockGrpcClient.setNextResponse(createMockResponse(TestUtils.createTestMessage("response")));

        // Act
        mockGrpcClient.processMessage(inputMessage.toProto());

        // Verify the request was constructed correctly
        ProcessMessageRequest lastRequest = mockGrpcClient.getLastRequest();
        assertNotNull(lastRequest);
        assertTrue(Arrays.equals(inputMessage.getPayload(), lastRequest.getMessage().getPayload().toByteArray()));
    }

    private ProcessMessageResponse createMockResponse(Message... messages) {
        ProcessMessageResponse.Builder builder = ProcessMessageResponse.newBuilder();
        for (Message message : messages) {
            builder.addMessages(message.toProto());
        }
        return builder.build();
    }

    private String getMessagePayload(flink_worker.FlinkWorker.Message message) {
        return new String(message.getPayload().toByteArray(), StandardCharsets.UTF_8);
    }
}
