import logging
import os
from typing import (
    Any,
    Mapping,
    MutableMapping,
    Self,
    Sequence,
    Union,
    cast,
    get_type_hints,
)

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.data_stream import (
    AllWindowedStream,
    DataStream,
    DataStreamSink,
    WindowedStream,
)
from sentry_streams.adapters.stream_adapter import (
    PipelineConfig,
    StreamAdapter,
)
from sentry_streams.config_types import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
    SegmentConfig,
    StepConfig,
)
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    FlatMap,
    Map,
    Reduce,
    Router,
    Sink,
    Source,
    Step,
)
from sentry_streams.pipeline.window import MeasurementUnit

from sentry_flink.flink.flink_translator import (
    FlinkAggregate,
    FlinkGroupBy,
    FlinkRoutingFunction,
    RoutingFuncReturnType,
    build_flink_window,
    get_router_message_type,
    is_standard_type,
    translate_custom_type,
    translate_to_flink_type,
)

logger = logging.getLogger(__name__)


class FlinkAdapter(StreamAdapter[DataStream, DataStreamSink]):
    # TODO: make the (de)serialization schema configurable
    # TODO: infer the output type from steps which
    # perform transformations / maps.

    # NOTE: Output type must be specified for steps
    # that send data to a next step that
    # performs serialization (e.g. Map --> Sink)

    def __init__(self, config: PipelineConfig, env: StreamExecutionEnvironment) -> None:
        self.environment_config = config["env"]
        self.pipeline_config = config["pipeline"]
        self.env = env

        self.env.set_parallelism(self.environment_config.get("parallelism", 1))

        self.cur_segment = 0
        # A list of segment config objects
        self.segment_config: Sequence[SegmentConfig] = self.pipeline_config["segments"]
        # Maps the step to the list index corresponding to the segment config
        self.steps_to_segments: MutableMapping[str, int] = {}
        self.steps_config: MutableMapping[str, StepConfig] = {}

        for ind in range(len(self.segment_config)):
            segment = self.segment_config[ind]
            self.steps_config.update(segment["steps_config"])
            for step in segment["steps_config"]:
                self.steps_to_segments[step] = ind

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        env = StreamExecutionEnvironment.get_execution_environment()

        libs_path = config.get("kafka_connect_lib_path")
        if libs_path is None:
            libs_path = os.environ.get("FLINK_LIBS")

        if libs_path:
            jar_file = os.path.join(
                os.path.abspath(libs_path), "flink-sql-connector-kafka-4.0.0-2.0.jar"
            )
            kafka_jar_file = os.path.join(
                os.path.abspath(libs_path), "flink-sql-connector-kafka-4.0.0-2.0.jar"
            )
            env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

        return cls(config, env)

    def resolve_incoming_chain(
        self, outgoing_step: Step, incoming_stream: DataStream
    ) -> DataStream:
        step_name = outgoing_step.name

        config = self.steps_config.get(step_name)
        if config and "starts_segment" in config:
            return incoming_stream.rebalance()

        return incoming_stream

    def resolve_outoing_chain(self, incoming_step: Step, outgoing_stream: DataStream) -> DataStream:
        step_name = incoming_step.name

        config = self.steps_config.get(step_name)
        if config and "starts_segment" in config:
            assert isinstance(outgoing_stream, DataStream)
            segment_id = self.steps_to_segments[step_name]
            parallelism = self.segment_config[segment_id].get("parallelism", 1)

            self.cur_segment = segment_id
            return outgoing_stream.start_new_chain().set_parallelism(parallelism)

        # Since Flink must set parallelism on a per-step basis (not per-chain)
        cur_parallelism = self.segment_config[self.cur_segment].get("parallelism", 1)
        return outgoing_stream.set_parallelism(cur_parallelism)

    def resolve_sink_chain(self, step: Sink, outgoing_stream: DataStreamSink) -> DataStreamSink:
        sink_name = step.name
        segment_id = self.steps_to_segments[sink_name]
        parallelism = self.segment_config[segment_id].get("parallelism", 1)

        return outgoing_stream.set_parallelism(parallelism)

    def resolve_segment_config(self, step: Step) -> SegmentConfig:
        segment_ind = self.steps_to_segments[step.name]
        config: SegmentConfig = self.segment_config[segment_ind]

        return config

    def source(self, step: Source) -> DataStream:
        config = self.resolve_segment_config(step)
        consumer_config = cast(KafkaConsumerConfig, config["steps_config"][step.name])

        assert hasattr(step, "stream_name")
        topic = step.stream_name

        deserialization_schema = SimpleStringSchema()

        # TODO: Look into using KafkaSource instead
        kafka_consumer = (
            KafkaSource.builder()
            .set_bootstrap_servers(consumer_config.get("bootstrap_servers", "kafka:9093"))
            .set_group_id(f"pipeline-{step.name}")
            .set_topics(topic)
            .set_value_only_deserializer(deserialization_schema)
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .build()
        )

        source_stream = self.env.from_source(
            kafka_consumer, WatermarkStrategy.for_monotonous_timestamps(), step.name
        )
        return self.resolve_outoing_chain(step, source_stream)

    def sink(self, step: Sink, stream: DataStream) -> DataStreamSink:
        config = self.resolve_segment_config(step)
        producer_config = cast(KafkaProducerConfig, config["steps_config"][step.name])

        assert hasattr(step, "stream_name")
        topic = step.stream_name

        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(producer_config.get("bootstrap_servers", "kafka:9093"))
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(
                    topic,
                )
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        sink_stream = self.resolve_incoming_chain(step, stream).sink_to(sink)
        return self.resolve_sink_chain(step, sink_stream)

    def filter(self, step: Filter, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        filter_stream = self.resolve_incoming_chain(step, stream).filter(
            func=lambda msg: imported_fn(msg)
        )

        return self.resolve_outoing_chain(step, filter_stream)

    def map(self, step: Map, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        return_type = imported_fn.__annotations__["return"]

        # TODO: Ensure output type is configurable like the schema above
        map_stream = self.resolve_incoming_chain(step, stream).map(
            func=lambda msg: imported_fn(msg),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

        return self.resolve_outoing_chain(step, map_stream)

    def flat_map(self, step: FlatMap, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        return_type = get_type_hints(imported_fn)["return"]

        # TODO: Ensure output type is configurable like the schema above
        flat_map_stream = self.resolve_incoming_chain(step, stream).flat_map(
            func=lambda msg: imported_fn(msg),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

        return self.resolve_outoing_chain(step, flat_map_stream)

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: DataStream,
    ) -> DataStream:
        agg = step.aggregate_fn
        windowing = step.windowing

        flink_window = build_flink_window(windowing)

        # Optional parameters
        group_by = step.group_by
        agg_backend = step.aggregate_backend if hasattr(step, "aggregate_backend") else None

        # TODO: Configure WatermarkStrategy as part of KafkaSource
        # Injecting strategy within a step like here produces
        # a new, watermarked stream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
        time_stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

        # If we have a groupby key, Flink should take care of repartitioning as needed
        if group_by:
            keyed_stream = time_stream.key_by(FlinkGroupBy(group_by))

            windowed_stream: Union[WindowedStream, AllWindowedStream] = keyed_stream.window(
                flink_window
            )

        else:
            windowed_stream = self.resolve_incoming_chain(step, time_stream).window_all(
                flink_window
            )

        return_type = agg().get_value.__annotations__["return"]

        # TODO: Figure out a systematic way to convert types
        aggregate_stream = windowed_stream.aggregate(
            FlinkAggregate(agg, agg_backend),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

        return self.resolve_outoing_chain(step, aggregate_stream)

    def broadcast(self, step: Broadcast, stream: DataStream) -> Mapping[str, DataStream]:

        broadcast_stream = self.resolve_incoming_chain(step, stream)
        # Broadcast in flink is implicit, so no processing needs to happen here
        return {
            branch.name: self.resolve_outoing_chain(step, broadcast_stream)
            for branch in step.routes
        }

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> Mapping[str, Any]:
        routing_table = step.routing_table
        routing_func = step.routing_function

        # routing functions should only have a single parameter since we're using
        # Flink's ProcessFunction which only takes a single value as input
        message_type = get_router_message_type(routing_func)

        output_tags = {
            key: OutputTag(
                tag_id=route.name,
                type_info=(
                    translate_to_flink_type(message_type)
                    if is_standard_type(message_type)
                    else translate_custom_type(message_type)
                ),
            )
            for key, route in routing_table.items()
        }
        routing_process_func = FlinkRoutingFunction(routing_func, output_tags)

        routed_stream = self.resolve_outoing_chain(step, stream.process(routing_process_func))

        return {
            route.tag_id: self.resolve_outoing_chain(step, routed_stream.get_side_output(route))
            for route in output_tags.values()
        }

    def shutdown(self) -> None:
        raise NotImplementedError

    def run(self) -> None:
        self.env.execute()
