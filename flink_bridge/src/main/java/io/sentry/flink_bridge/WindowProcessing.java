package io.sentry.flink_bridge;

import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.common.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink_worker.FlinkWorker;

import java.util.List;

/**
 * ProcessFunction that implements window processing by delegating to the gRPC
 * service.
 * This class handles adding messages to windows and triggering windows to
 * retrieve
 * accumulated messages.
 */
public class WindowProcessing implements OneInputWindowStreamProcessFunction<Message, Message> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowProcessing.class);
    private GrpcClient grpcClient;

    @Override
    public void onRecord(
            Message record,
            Collector<Message> output,
            PartitionedContext<Message> ctx,
            OneInputWindowContext<Message> windowContext) throws Exception {

        if (grpcClient == null) {
            grpcClient = new GrpcClient("localhost", 50051);
            LOG.info("WindowProcessing gRPC client initialized");
        }

        try {
            LOG.info("Processing message for window: {}", record);

            String partitionKey = ctx.getStateManager().getCurrentKey().toString();
            long windowStartTime = windowContext.getStartTime();
            int segmentId = 0;

            // Add the message to the window via gRPC service
            grpcClient.addToWindow(record.toProto(), segmentId, partitionKey, windowStartTime);
            LOG.debug("Added message to window: partition={}, startTime={}, segmentId={}",
                    partitionKey, windowStartTime, segmentId);

        } catch (Exception e) {
            LOG.error("Error processing message for window: {}", record, e);
            // In a production environment, you might want to handle errors differently
            // For now, we'll just log the error and continue
        }
    }

    @Override
    public void onTrigger(
            Collector<Message> output,
            PartitionedContext<Message> ctx,
            OneInputWindowContext<Message> windowContext)
            throws Exception {

        String partitionKey = ctx.getStateManager().getCurrentKey().toString();
        long windowStartTime = windowContext.getStartTime();
        int segmentId = 0;
        try {

            LOG.info("Triggering window: partition={}, startTime={}, segmentId={}",
                    partitionKey, windowStartTime, segmentId);

            // Trigger the window via gRPC service
            List<FlinkWorker.Message> windowMessages = grpcClient.triggerWindow(segmentId, partitionKey,
                    windowStartTime);

            // Output all retrieved messages
            for (FlinkWorker.Message windowMsg : windowMessages) {
                Message outputMsg = new Message(windowMsg);
                output.collect(outputMsg);
                LOG.info("Output window message: {}", outputMsg);
            }

            LOG.info("Window triggered successfully, retrieved {} messages", windowMessages.size());

        } catch (Exception e) {
            LOG.error("Error triggering window: partition={}, startTime={}, segmentId={}",
                    partitionKey, windowStartTime, segmentId, e);
        }
    }

    @Override
    public void onClear(
            Collector<Message> output,
            PartitionedContext<Message> ctx,
            OneInputWindowContext<Message> windowContext) {
        LOG.info("Clearing window: partition={}, startTime={}, segmentId={}",
                ctx.getStateManager().getCurrentKey().toString(), windowContext.getStartTime(), 0);
    }

    @Override
    public void onLateRecord(Message record, Collector<Message> output, PartitionedContext<Message> ctx) {
        LOG.info("Late record: partition={}, segmentId={}",
                ctx.getStateManager().getCurrentKey().toString(), 0);
    }

    @Override
    public void close() throws Exception {
        if (grpcClient != null) {
            grpcClient.shutdown();
            LOG.info("WindowProcessing gRPC client shutdown");
        }
    }
}
