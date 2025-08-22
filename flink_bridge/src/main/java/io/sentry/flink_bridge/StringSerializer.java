package io.sentry.flink_bridge;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * ProcessFunction that serializes FlinkWorker.Message input to String output.
 * This implements the OneInputStreamProcessFunction pattern for Flink
 * DataStream API.
 */
public class StringSerializer implements OneInputStreamProcessFunction<Message, String> {

    private static final Logger LOG = LoggerFactory.getLogger(StringSerializer.class);

    @Override
    public void open(NonPartitionedContext<String> ctx) throws Exception {
        LOG.info("StringSerializer opened");
    }

    @Override
    public void processRecord(
            Message record,
            Collector<String> out,
            PartitionedContext<String> ctx) throws Exception {
        try {
            LOG.debug("Serializing message: {}", record);

            // TODO: Implement your serialization logic here
            // For now, converting the message payload to string
            String serializedContent = new String(record.getPayload(), StandardCharsets.UTF_8);

            out.collect(serializedContent);

        } catch (Exception e) {
            LOG.error("Error serializing message: {}", record, e);
            // In a production environment, you might want to handle errors differently
            // For now, we'll just log the error and continue
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("StringSerializer closed");
    }
}
