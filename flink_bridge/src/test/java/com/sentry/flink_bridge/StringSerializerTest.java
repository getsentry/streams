package com.sentry.flink_bridge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StringSerializer class.
 */
class StringSerializerTest {

    private StringSerializer stringSerializer;
    private TestUtils.MockStringCollector mockCollector;

    @BeforeEach
    void setUp() {
        stringSerializer = new StringSerializer();
        mockCollector = new TestUtils.MockStringCollector();
    }

    @Test
    void testProcessRecord_SimpleMessage() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("test message");

        // Act
        stringSerializer.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        assertEquals("test message", mockCollector.getCollectedItems().get(0));
    }

    @Test
    void testProcessRecord_EmptyMessage() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("");

        // Act
        stringSerializer.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        assertEquals("", mockCollector.getCollectedItems().get(0));
    }

    @Test
    void testProcessRecord_UnicodeMessage() throws Exception {
        // Arrange
        Message inputMessage = TestUtils.createTestMessage("Hello 世界 🌍");

        // Act
        stringSerializer.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        assertEquals("Hello 世界 🌍", mockCollector.getCollectedItems().get(0));
    }

    @Test
    void testProcessRecord_MessageWithHeaders() throws Exception {
        // Arrange
        Map<String, String> headers = new HashMap<>();
        headers.put("source", "test");
        headers.put("timestamp", "123456789");

        Message inputMessage = new Message(
                "test with headers".getBytes(StandardCharsets.UTF_8),
                headers,
                123456789L);

        // Act
        stringSerializer.processRecord(inputMessage, mockCollector, null);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());
        assertEquals("test with headers", mockCollector.getCollectedItems().get(0));
    }
}
