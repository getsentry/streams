package com.sentry.flink_bridge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StringDeserializer class.
 */
class StringDeserializerTest {

    private StringDeserializer stringDeserializer;
    private TestUtils.MockMessageCollector mockCollector;
    private TestUtils.MockPartitionedContext mockContext;

    @BeforeEach
    void setUp() {
        stringDeserializer = new StringDeserializer("my_pipeline");
        mockCollector = new TestUtils.MockMessageCollector();
        mockContext = TestUtils.createMockPartitionedContext();
    }

    @Test
    void testProcessRecord_SimpleString() throws Exception {
        // Arrange
        String inputString = "test message";

        // Act
        stringDeserializer.processRecord(inputString, mockCollector, mockContext);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());

        Message result = mockCollector.getCollectedItems().get(0);
        assertNotNull(result);
        assertEquals(inputString, new String(result.getPayload(), StandardCharsets.UTF_8));
        assertEquals("string_deserializer", result.getHeaders().get("source"));
        assertNotNull(result.getHeaders().get("receive_timestamp"));
        assertEquals("test-job", result.getHeaders().get("job_name"));
        assertEquals("test-task", result.getHeaders().get("task_name"));
        assertEquals("my_pipeline", result.getHeaders().get("pipeline_name"));
        assertTrue(result.getTimestamp() > 0);
    }

    @Test
    void testProcessRecord_EmptyString() throws Exception {
        // Arrange
        String inputString = "";

        // Act
        stringDeserializer.processRecord(inputString, mockCollector, mockContext);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());

        Message result = mockCollector.getCollectedItems().get(0);
        assertNotNull(result);
        assertEquals("", new String(result.getPayload(), StandardCharsets.UTF_8));
        assertNotNull(result.getHeaders().get("receive_timestamp"));
        assertEquals("test-job", result.getHeaders().get("job_name"));
        assertEquals("test-task", result.getHeaders().get("task_name"));
        assertEquals("my_pipeline", result.getHeaders().get("pipeline_name"));
    }

    @Test
    void testProcessRecord_UnicodeString() throws Exception {
        // Arrange
        String inputString = "Hello 世界 🌍";

        // Act
        stringDeserializer.processRecord(inputString, mockCollector, mockContext);

        // Assert
        assertEquals(1, mockCollector.getCollectedItems().size());

        Message result = mockCollector.getCollectedItems().get(0);
        assertNotNull(result);
        assertEquals(inputString, new String(result.getPayload(), StandardCharsets.UTF_8));
        assertNotNull(result.getHeaders().get("receive_timestamp"));
        assertEquals("test-job", result.getHeaders().get("job_name"));
        assertEquals("test-task", result.getHeaders().get("task_name"));
        assertEquals("my_pipeline", result.getHeaders().get("pipeline_name"));
    }
}
