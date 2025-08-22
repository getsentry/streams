package io.sentry.flink_bridge;

import com.google.protobuf.ByteString;
import flink_worker.FlinkWorker;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.TaskInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared test utilities and mock classes for Flink Bridge tests.
 */
public class TestUtils {

    /**
     * Mock implementation of Collector for testing String outputs.
     */
    public static class MockStringCollector implements Collector<String> {
        private final List<String> collectedItems = new java.util.ArrayList<>();

        @Override
        public void collect(String record) {
            collectedItems.add(record);
        }

        @Override
        public void collectAndOverwriteTimestamp(String record, long timestamp) {
            collect(record);
        }

        public List<String> getCollectedItems() {
            return collectedItems;
        }
    }

    /**
     * Mock implementation of Collector for testing Message outputs.
     */
    public static class MockMessageCollector implements Collector<Message> {
        private final List<Message> collectedItems = new java.util.ArrayList<>();

        @Override
        public void collect(Message record) {
            collectedItems.add(record);
        }

        @Override
        public void collectAndOverwriteTimestamp(Message record, long timestamp) {
            collect(record);
        }

        public List<Message> getCollectedItems() {
            return collectedItems;
        }
    }

    /**
     * Mock implementation of ProcessingTimeManager for testing.
     */
    public static class MockProcessingTimeManager implements ProcessingTimeManager {
        private final long mockTime;

        public MockProcessingTimeManager(long mockTime) {
            this.mockTime = mockTime;
        }

        @Override
        public long currentTime() {
            return mockTime;
        }

        @Override
        public void registerTimer(long time) {
            // Mock implementation
        }

        @Override
        public void deleteTimer(long time) {
            // Mock implementation
        }
    }

    /**
     * Mock implementation of JobInfo for testing.
     */
    public static class MockJobInfo implements JobInfo {
        private final String jobName;

        public MockJobInfo(String jobName) {
            this.jobName = jobName;
        }

        @Override
        public String getJobName() {
            return jobName;
        }

        @Override
        public org.apache.flink.datastream.api.context.JobInfo.ExecutionMode getExecutionMode() {
            return org.apache.flink.datastream.api.context.JobInfo.ExecutionMode.STREAMING;
        }
    }

    /**
     * Mock implementation of TaskInfo for testing.
     */
    public static class MockTaskInfo implements TaskInfo {
        private final String taskName;

        public MockTaskInfo(String taskName) {
            this.taskName = taskName;
        }

        @Override
        public String getTaskName() {
            return taskName;
        }

        @Override
        public int getIndexOfThisSubtask() {
            return 0;
        }

        @Override
        public int getMaxParallelism() {
            return 1;
        }

        @Override
        public int getAttemptNumber() {
            return 0;
        }

        @Override
        public int getParallelism() {
            return 1;
        }
    }

    /**
     * Mock implementation of PartitionedContext for testing.
     */
    public static class MockPartitionedContext implements PartitionedContext<Message> {
        private final ProcessingTimeManager processingTimeManager;
        private final JobInfo jobInfo;
        private final TaskInfo taskInfo;

        public MockPartitionedContext(ProcessingTimeManager processingTimeManager, JobInfo jobInfo, TaskInfo taskInfo) {
            this.processingTimeManager = processingTimeManager;
            this.jobInfo = jobInfo;
            this.taskInfo = taskInfo;
        }

        @Override
        public ProcessingTimeManager getProcessingTimeManager() {
            return processingTimeManager;
        }

        @Override
        public JobInfo getJobInfo() {
            return jobInfo;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        @Override
        public org.apache.flink.datastream.api.context.NonPartitionedContext<Message> getNonPartitionedContext() {
            return null; // Mock implementation
        }

        @Override
        public org.apache.flink.datastream.api.context.StateManager getStateManager() {
            return null; // Mock implementation
        }

        @Override
        public org.apache.flink.metrics.MetricGroup getMetricGroup() {
            return null; // Mock implementation
        }
    }

    /**
     * Helper method for creating test Message objects.
     */
    public static Message createTestMessage(String payload) {
        Map<String, String> headers = new HashMap<>();
        headers.put("source", "test");
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));

        return new Message(
                payload.getBytes(StandardCharsets.UTF_8),
                headers,
                System.currentTimeMillis());
    }

    /**
     * Helper method for creating test Message objects with custom
     * headers.
     */
    public static Message createTestMessage(String payload, String source, String timestamp) {
        Map<String, String> headers = new HashMap<>();
        headers.put("source", source);
        headers.put("timestamp", timestamp);

        return new Message(
                payload.getBytes(StandardCharsets.UTF_8),
                headers,
                Long.parseLong(timestamp));
    }

    /**
     * Helper method for creating test FlinkWorker.Message objects.
     */
    public static FlinkWorker.Message createTestProtoMessage(String payload) {
        return FlinkWorker.Message.newBuilder()
                .setPayload(ByteString.copyFrom(payload.getBytes(StandardCharsets.UTF_8)))
                .putHeaders("source", "test")
                .putHeaders("timestamp", String.valueOf(System.currentTimeMillis()))
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    /**
     * Helper method for creating test FlinkWorker.Message objects with custom
     * headers.
     */
    public static FlinkWorker.Message createTestProtoMessage(String payload, String source, String timestamp) {
        return FlinkWorker.Message.newBuilder()
                .setPayload(ByteString.copyFrom(payload.getBytes(StandardCharsets.UTF_8)))
                .putHeaders("source", source)
                .putHeaders("timestamp", timestamp)
                .setTimestamp(Long.parseLong(timestamp))
                .build();
    }

    /**
     * Helper method for creating a mock PartitionedContext with default test
     * values.
     */
    public static MockPartitionedContext createMockPartitionedContext() {
        return new MockPartitionedContext(
                new MockProcessingTimeManager(123456789L),
                new MockJobInfo("test-job"),
                new MockTaskInfo("test-task"));
    }
}
