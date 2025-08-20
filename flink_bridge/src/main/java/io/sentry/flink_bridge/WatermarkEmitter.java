package io.sentry.flink_bridge;

import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkEmitter
        implements OneInputStreamProcessFunction<Message, Message> {

    private static final Logger LOG = LoggerFactory.getLogger(WatermarkEmitter.class);

    @Override
    public void processRecord(Message record, Collector<Message> output, PartitionedContext<Message> ctx)
            throws Exception {
        // Wait 500 milliseconds before adding the message to output
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting before output collection", e);
        }

        output.collect(record);
    }
}
