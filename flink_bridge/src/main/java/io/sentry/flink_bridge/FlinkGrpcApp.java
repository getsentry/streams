package io.sentry.flink_bridge;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkGeneratorBuilder;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.sentry.flink_bridge.Message;
import io.sentry.flink_bridge.StringDeserializer;
import io.sentry.flink_bridge.GrpcMessageProcessor;
import io.sentry.flink_bridge.CustomPostProcessor;
import io.sentry.flink_bridge.StringSerializer;

import java.time.Duration;
import java.util.Arrays;

/**
 * Flink application that reads messages from a text file and processes them
 * using a gRPC service.
 * This demonstrates the integration between Apache Flink and gRPC services.
 */
class KeyGenerator implements KeySelector<Message, String> {
        @Override
        public String getKey(Message message) {
                return String.valueOf(message.getPayload().length);
        }
}

public class FlinkGrpcApp {

        private static final Logger LOG = LoggerFactory.getLogger(FlinkGrpcApp.class);

        public static void main(String[] args) throws Exception {
                // Set up the streaming execution environment
                ExecutionEnvironment env = ExecutionEnvironment.getInstance();
                env.setExecutionMode(RuntimeExecutionMode.STREAMING);

                // Create a data stream from a text file using Flink 2.1.0 API
                NonKeyedPartitionStream<String> textStream = env.fromSource(
                                DataStreamV2SourceUtils.fromData(
                                                Arrays.asList(new String[] {
                                                                "Hello World",
                                                                "This is a test message",
                                                                "Another message for processing",
                                                                "Flink gRPC integration test",
                                                                "Processing stream data with external service",
                                                                "Hello World",
                                                                "This is a test message",
                                                                "Another message for processing",
                                                                "Flink gRPC integration test",
                                                                "Processing stream data with external service",
                                                                "Hello World",
                                                                "This is a test message",
                                                                "Another message for processing",
                                                                "Flink gRPC integration test",
                                                                "Processing stream data with external service",
                                                })),
                                "in memory list");

                KeyedPartitionStream<String, Message> messageStream = textStream
                                .process(new StringDeserializer("my_pipeline"))
                                .keyBy(new KeyGenerator());

                EventTimeWatermarkGeneratorBuilder<Message> watermarkBuilder = EventTimeExtension
                                .newWatermarkGeneratorBuilder(Message::getTimestamp)
                                .withIdleness(Duration.ofSeconds(10))
                                .withMaxOutOfOrderTime(Duration.ofSeconds(30)) // set max out-of-order time
                                .periodicWatermark(Duration.ofMillis(250));

                KeyedPartitionStream<String, Message> delayedStream = messageStream
                                .process(new WatermarkEmitter())
                                .keyBy(new KeyGenerator());

                // Apply the gRPC processing function
                KeyedPartitionStream<String, Message> watermarkedStream = delayedStream
                                .process(watermarkBuilder.buildAsProcessFunction())
                                .keyBy(new KeyGenerator());

                KeyedPartitionStream<String, Message> processedStream = watermarkedStream
                                .process(EventTimeExtension.wrapProcessFunction(new GrpcMessageProcessor()))
                                .keyBy(new KeyGenerator());

                // Add custom post-processing function after gRPC processing
                NonKeyedPartitionStream<Message> postProcessedStream = processedStream
                                .process(BuiltinFuncs.window(
                                                WindowStrategy.tumbling(Duration.ofSeconds(2),
                                                                WindowStrategy.EVENT_TIME),
                                                new WindowProcessing()));

                // NonKeyedPartitionStream<String> serializedStream =
                // postProcessedStream.process(new StringSerializer());

                // Print the processed messages to standard output
                postProcessedStream.toSink(new WrappedSink<>(new PrintSink<>())).withName("print-sink");

                // Execute the Flink job
                LOG.info("Starting Flink gRPC application...");
                env.execute("Flink gRPC Message Processing Job");
        }

}
