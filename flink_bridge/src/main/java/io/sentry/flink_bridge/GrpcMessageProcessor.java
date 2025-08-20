package io.sentry.flink_bridge;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.function.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink_worker.FlinkWorker;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ProcessFunction that sends messages to the gRPC service for processing.
 * This implements the OneInputStreamProcessFunction pattern for Flink
 * DataStream API.
 */
public class GrpcMessageProcessor implements OneInputEventTimeStreamProcessFunction<Message, Message> {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcMessageProcessor.class);
    protected GrpcClient grpcClient;

    private EventTimeManager eventTimeManager;

    @Override
    public void open(NonPartitionedContext<Message> ctx) throws Exception {
        // Initialize the gRPC client
        grpcClient = new GrpcClient("localhost", 50051);
        LOG.info("gRPC client initialized");
    }

    @Override
    public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
        // get event time manager instance
        this.eventTimeManager = eventTimeManager;
    }

    @Override
    public void processRecord(
            Message record,
            Collector<Message> out,
            PartitionedContext<Message> ctx) throws Exception {
        try {
            LOG.info("Processing message: {}", record);

            // Send to gRPC service and get response
            List<FlinkWorker.Message> processedMessages = grpcClient.processMessage(record.toProto());

            // Process the response and output processed messages
            for (FlinkWorker.Message processedMsg : processedMessages) {
                // String processedContent = new String(processedMsg.getPayload().toByteArray(),
                // StandardCharsets.UTF_8);
                // LOG.info("Received processed message: {}", processedMsg);
                out.collect(new Message(processedMsg));

            }

        } catch (Exception e) {
            LOG.error("Error processing message: {}", record, e);
            // In a production environment, you might want to handle errors differently
            // For now, we'll just log the error and continue
        }
    }

    @Override
    public void onEventTimer(
            long timestamp,
            Collector<Message> output,
            PartitionedContext<Message> ctx) {
        // write your event timer callback here
        LOG.info("Received trigger for time {}", timestamp);
    }

    @Override
    public void onEventTimeWatermark(
            long watermarkTimestamp,
            Collector<Message> output,
            NonPartitionedContext<Message> ctx)
            throws Exception {
        // sense event time watermark arrival
        Map<String, String> headers = new HashMap<>();
        headers.put("job_name", ctx.getJobInfo().getJobName());
        List<FlinkWorker.Message> processedMessages = grpcClient.processWatermark(watermarkTimestamp, headers, 0);

        for (FlinkWorker.Message processedMsg : processedMessages) {
            output.collect(new Message(processedMsg));
        }

        LOG.info("Received watermark for time {}", watermarkTimestamp);
    }

    @Override
    public void close() throws Exception {
        if (grpcClient != null) {
            grpcClient.shutdown();

        }
    }
}
