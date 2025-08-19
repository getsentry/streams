package com.sentry.flink_bridge;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;

import java.nio.charset.StandardCharsets;

/**
 * ProcessFunction that deserializes String input to Message output.
 * This implements the OneInputStreamProcessFunction pattern for Flink
 * DataStream API.
 */
public class StringDeserializer implements OneInputStreamProcessFunction<String, Message> {

    private static final Logger LOG = LoggerFactory.getLogger(StringDeserializer.class);

    private final String pipelineName;

    public StringDeserializer(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    @Override
    public void open(NonPartitionedContext<Message> ctx) throws Exception {
        LOG.info("StringDeserializer opened");
    }

    @Override
    public void processRecord(
            String record,
            Collector<Message> out,
            PartitionedContext<Message> ctx) throws Exception {
        try {
            LOG.info("Deserializing message: {}", record);

            // long time = ctx.getProcessingTimeManager().currentTime();
            long time = System.currentTimeMillis();

            // TODO: Implement your deserialization logic here
            // For now, creating a basic message structure
            Map<String, String> headers = new HashMap<>();
            headers.put("source", "string_deserializer");
            headers.put("receive_timestamp", String.valueOf(time));
            headers.put("job_name", ctx.getJobInfo().getJobName());
            headers.put("task_name", ctx.getTaskInfo().getTaskName());
            headers.put("pipeline_name", this.pipelineName);

            Message message = new Message(
                    record.getBytes(StandardCharsets.UTF_8),
                    headers,
                    System.currentTimeMillis());

            out.collect(message);

        } catch (Exception e) {
            LOG.error("Error deserializing message: {}", record, e);
            // In a production environment, you might want to handle errors differently
            // For now, we'll just log the error and continue
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("StringDeserializer closed");
    }
}
