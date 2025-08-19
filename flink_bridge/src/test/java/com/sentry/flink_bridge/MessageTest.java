package com.sentry.flink_bridge;

import flink_worker.FlinkWorker;
import org.apache.flink.types.PojoTestUtils;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Message POJO class.
 * Tests that the class follows Flink's POJO serialization rules.
 */
class MessageTest {

    @Test
    void testMessageIsValidPojo() {
        PojoTestUtils.assertSerializedAsPojo(Message.class);
    }

    @Test
    void testDefaultConstructor() {
        Message message = new Message();

        assertNotNull(message);
        assertArrayEquals(new byte[0], message.getPayload());
        assertNotNull(message.getHeaders());
        assertTrue(message.getHeaders().isEmpty());
        assertEquals(0L, message.getTimestamp());
    }

    @Test
    void testConstructorWithAllFields() {
        byte[] payload = "test payload".getBytes();
        Map<String, String> headers = new HashMap<>();
        headers.put("key1", "value1");
        headers.put("key2", "value2");
        long timestamp = 1234567890L;

        Message message = new Message(payload, headers, timestamp);

        assertArrayEquals(payload, message.getPayload());
        assertEquals(headers, message.getHeaders());
        assertEquals(timestamp, message.getTimestamp());
    }

    @Test
    void testConstructorFromProtoMessage() {
        // Create a protobuf message
        FlinkWorker.Message protoMessage = FlinkWorker.Message.newBuilder()
                .setPayload(com.google.protobuf.ByteString.copyFromUtf8("proto payload"))
                .putHeaders("proto_key", "proto_value")
                .setTimestamp(987654321L)
                .build();

        Message message = new Message(protoMessage);

        assertArrayEquals("proto payload".getBytes(), message.getPayload());
        assertEquals(protoMessage.getHeadersMap(), message.getHeaders());
        assertEquals(protoMessage.getTimestamp(), message.getTimestamp());
    }

    @Test
    void testToProto() {
        byte[] payload = "test payload".getBytes();
        Map<String, String> headers = new HashMap<>();
        headers.put("key1", "value1");
        headers.put("key2", "value2");
        long timestamp = 1234567890L;

        Message message = new Message(payload, headers, timestamp);
        FlinkWorker.Message protoMessage = message.toProto();

        assertArrayEquals(payload, protoMessage.getPayload().toByteArray());
        assertEquals(headers, protoMessage.getHeadersMap());
        assertEquals(timestamp, protoMessage.getTimestamp());
    }

    @Test
    void testGettersAndSetters() {
        Message message = new Message();

        byte[] payload = "new payload".getBytes();
        Map<String, String> headers = new HashMap<>();
        headers.put("new_key", "new_value");
        long timestamp = 999999999L;

        message.setPayload(payload);
        message.setHeaders(headers);
        message.setTimestamp(timestamp);

        assertArrayEquals(payload, message.getPayload());
        assertEquals(headers, message.getHeaders());
        assertEquals(timestamp, message.getTimestamp());
    }

    @Test
    void testNullHandling() {
        Message message = new Message(null, null, 0L);

        assertArrayEquals(new byte[0], message.getPayload());
        assertNotNull(message.getHeaders());
        assertTrue(message.getHeaders().isEmpty());
        assertEquals(0L, message.getTimestamp());
    }

    @Test
    void testEqualsAndHashCode() {
        Message message1 = new Message(
                "payload".getBytes(),
                Map.of("key", "value"),
                123L);

        Message message2 = new Message(
                "payload".getBytes(),
                Map.of("key", "value"),
                123L);

        Message message3 = new Message(
                "different".getBytes(),
                Map.of("key", "value"),
                123L);

        assertEquals(message1, message2);
        assertNotEquals(message1, message3);
        assertEquals(message1.hashCode(), message2.hashCode());
        assertNotEquals(message1.hashCode(), message3.hashCode());
    }

    @Test
    void testToString() {
        Message message = new Message(
                "test".getBytes(),
                Map.of("header", "value"),
                456L);

        String str = message.toString();

        assertTrue(str.contains("payload="));
        assertTrue(str.contains("headers="));
        assertTrue(str.contains("timestamp="));
        // Check for byte array representation [116, 101, 115, 116] which represents
        // "test"
        assertTrue(str.contains("116"));
        assertTrue(str.contains("101"));
        assertTrue(str.contains("115"));
        assertTrue(str.contains("116"));
        assertTrue(str.contains("header"));
        assertTrue(str.contains("456"));
    }
}
