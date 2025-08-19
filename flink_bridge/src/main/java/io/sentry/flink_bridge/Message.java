package io.sentry.flink_bridge;

import flink_worker.FlinkWorker;
import java.util.HashMap;
import java.util.Map;

/**
 * POJO Message class that follows Flink's serialization rules.
 * This class can be serialized by Flink's POJO serializer.
 */
public class Message {

    private byte[] payload;
    private Map<String, String> headers;
    private long timestamp;

    /**
     * Default constructor required by Flink's POJO serializer.
     */
    public Message() {
        this.payload = new byte[0];
        this.headers = new HashMap<>();
        this.timestamp = 0L;
    }

    /**
     * Constructor with all fields.
     *
     * @param payload   the message payload
     * @param headers   the message headers
     * @param timestamp the message timestamp
     */
    public Message(byte[] payload, Map<String, String> headers, long timestamp) {
        this.payload = payload != null ? payload : new byte[0];
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
        this.timestamp = timestamp;
    }

    /**
     * Constructor that creates Message from FlinkWorker.Message.
     *
     * @param protoMessage the protobuf message to convert from
     */
    public Message(FlinkWorker.Message protoMessage) {
        this.payload = protoMessage.getPayload().toByteArray();
        this.headers = new HashMap<>(protoMessage.getHeadersMap());
        this.timestamp = protoMessage.getTimestamp();
    }

    /**
     * Serialization method that produces FlinkWorker.Message.
     *
     * @return the protobuf message
     */
    public FlinkWorker.Message toProto() {
        FlinkWorker.Message.Builder builder = FlinkWorker.Message.newBuilder();
        builder.setPayload(com.google.protobuf.ByteString.copyFrom(this.payload));
        builder.putAllHeaders(this.headers);
        builder.setTimestamp(this.timestamp);
        return builder.build();
    }

    // Getters and setters required by Flink's POJO serializer

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload != null ? payload : new byte[0];
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? new HashMap<>(headers) : new HashMap<>();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        Message message = (Message) obj;

        if (timestamp != message.timestamp)
            return false;
        if (!java.util.Arrays.equals(payload, message.payload))
            return false;
        return headers.equals(message.headers);
    }

    @Override
    public int hashCode() {
        int result = java.util.Arrays.hashCode(payload);
        result = 31 * result + headers.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "payload=" + java.util.Arrays.toString(payload) +
                ", headers=" + headers +
                ", timestamp=" + timestamp +
                '}';
    }
}
