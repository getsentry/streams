package io.sentry.flink_bridge;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkGeneratorBuilder;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Flink application that reads messages from a text file and processes them
 * using a gRPC service.
 * This demonstrates the integration between Apache Flink and gRPC services.
 */
class KeyGenerator implements KeySelector<Message, String> {
        @Override
        public String getKey(Message message) {
                return String.valueOf(message.getPayload().length % 4);
        }
}

class MessageSerializer implements SerializationSchema<Message> {
        @Override
        public byte[] serialize(Message message) {
                return message.getPayload();
        }
}

public class FlinkGrpcApp {

        private static final Logger LOG = LoggerFactory.getLogger(FlinkGrpcApp.class);

        public static void main(String[] args) throws Exception {
                ParameterTool parameters = ParameterTool.fromArgs(args);
                String pipelineConfigFile = parameters.getRequired("pipeline-name");
                PipelineParser parser = new PipelineParser();
                Pipeline pipeline = parser.parseFile(pipelineConfigFile);
                List<PipelineStep> steps = pipeline.getSteps();

                List<String> data = TestData.getMetrics();

                // Set up the streaming execution environment
                ExecutionEnvironment env = ExecutionEnvironment.getInstance();
                env.setExecutionMode(RuntimeExecutionMode.STREAMING);

                Source source = pipeline.getSource();

                String bootstrapServers = String.join(",", (List<String>) source.getConfig().get("bootstrap_servers"));
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setTopics((String) source.getConfig().get("stream_name"))
                                .setGroupId("flink-grpc-group")
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .build();

                LOG.info("Kafka source: {} conencts to {}", kafkaSource, bootstrapServers);
                // Create a data stream from a text file using Flink 2.1.0 API
                // NonKeyedPartitionStream<String> textStream = env.fromSource(
                // DataStreamV2SourceUtils.fromData(data), "in memory list");

                NonKeyedPartitionStream<String> textStream = env
                                .fromSource(
                                                DataStreamV2SourceUtils.wrapSource(kafkaSource),
                                                "kafka-source");

                KeyedPartitionStream<String, Message> messageStream = textStream
                                .process(new StringDeserializer("my_pipeline"))
                                .keyBy(new KeyGenerator());

                EventTimeWatermarkGeneratorBuilder<Message> watermarkBuilder = EventTimeExtension
                                .newWatermarkGeneratorBuilder(Message::getTimestamp)
                                .withIdleness(Duration.ofSeconds(10))
                                .withMaxOutOfOrderTime(Duration.ofSeconds(4)) // set max out-of-order time
                                .periodicWatermark(Duration.ofMillis(2000));

                KeyedPartitionStream<String, Message> delayedStream = messageStream
                                .process(new WatermarkEmitter())
                                .withName("message-delay-generator")
                                .keyBy(new KeyGenerator());

                // Apply the gRPC processing function
                KeyedPartitionStream<String, Message> watermarkedStream = delayedStream
                                .process(watermarkBuilder.buildAsProcessFunction())
                                .withName("watermark-generator")
                                .keyBy(new KeyGenerator());

                //////////////// APPLICATION LOGIC ////////////////

                KeyedPartitionStream<String, Message> processedStream = watermarkedStream;
                for (int i = 0; i < steps.size(); i++) {
                        LOG.info("Applying step {} of {}", steps.get(i).getStepName(), i);
                        processedStream = processedStream
                                        .process(EventTimeExtension.wrapProcessFunction(new GrpcMessageProcessor(i)))
                                        .withName(String.format("processor-%s-%d", steps.get(i).getStepName(), i))
                                        .keyBy(new KeyGenerator());
                }

                for (Sink sink : pipeline.getSinks().values()) {
                        String sinkBootstrapServers = String.join(",",
                                        (List<String>) sink.getConfig().get("bootstrap_servers"));
                        KafkaSink<Message> kafkaSink = KafkaSink.<Message>builder()
                                        .setBootstrapServers(sinkBootstrapServers)
                                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                                        .setTopic((String) sink.getConfig().get("stream_name"))
                                                        .setValueSerializationSchema(new MessageSerializer())
                                                        .build())
                                        .build();

                        processedStream.toSink(DataStreamV2SinkUtils.wrapSink(kafkaSink)).withName(sink.getName());
                }

                // Add custom post-processing function after gRPC processing
                // NonKeyedPartitionStream<Message> postProcessedStream = processedStream
                // .process(BuiltinFuncs.window(
                // WindowStrategy.tumbling(Duration.ofSeconds(2),
                // WindowStrategy.EVENT_TIME),
                // new WindowProcessing()));

                // NonKeyedPartitionStream<String> serializedStream =
                // postProcessedStream.process(new StringSerializer());

                // Print the processed messages to standard output
                // processedStream.toSink(new WrappedSink<>(new
                // PrintSink<>())).withName("print-sink");

                // Execute the Flink job
                LOG.info("Starting Flink gRPC application...");
                env.execute("Flink gRPC Message Processing Job");
        }

}
