import os
from typing import (
    Any,
    MutableMapping,
    Self,
    Union,
    get_type_hints,
)

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors import (  # type: ignore[attr-defined]
    FlinkKafkaConsumer,
)
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)
from pyflink.datastream.data_stream import (
    AllWindowedStream,
    DataStream,
    DataStreamSink,
    WindowedStream,
)
from sentry_streams.adapters.arroyo.adapter import (
    KafkaConsumerConfig,
    KafkaProducerConfig,
    SegmentConfig,
    StepConfig,
)
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
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

        self.segment_config = {}
        self.steps_to_segments = {}
        for segment in self.pipeline_config["segments"]:
            all_segment_config = self.pipeline_config["segments"][segment]
            self.segment_config[segment] = all_segment_config

            for step in all_segment_config["steps_config"]:
                self.steps_to_segments[step] = segment

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        env = StreamExecutionEnvironment.get_execution_environment()

        libs_path = config.get("kafka_connect_lib_path")
        if libs_path is None:
            libs_path = os.environ.get("FLINK_LIBS")

        if libs_path:
            jar_file = os.path.join(
                os.path.abspath(libs_path), "flink-sql-connector-kafka-3.4.0-1.20.jar"
            )
            kafka_jar_file = os.path.join(
                os.path.abspath(libs_path), "flink-sql-connector-kafka-3.4.0-1.20.jar"
            )
            env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

        return cls(config, env)

    def resolve_incoming_chain(
        self, outgoing_step: Step, incoming_stream: DataStream, config: SegmentConfig
    ) -> DataStream:
        step_config = config["steps_config"][outgoing_step.name]

        if "starts_segment" in step_config:
            return incoming_stream.rebalance()

        return incoming_stream

    def resolve_outoing_chain(
        self, incoming_step: Step, outgoing_stream: DataStream, config: SegmentConfig
    ) -> DataStream:
        step_config = config["steps_config"][incoming_step.name]

        if "starts_segment" in step_config:
            assert isinstance(outgoing_stream, DataStream)
            return outgoing_stream.start_new_chain().set_parallelism(config["parallelism"])

        return outgoing_stream.set_parallelism(config["parallelism"])

    def resolve_sink_chain(
        self, outgoing_stream: DataStreamSink, config: SegmentConfig
    ) -> DataStreamSink:

        return outgoing_stream.set_parallelism(config["parallelism"])

    def resolve_segment_config(self, step: Step) -> SegmentConfig:
        segment = self.steps_to_segments[step.name]
        config: SegmentConfig = self.segment_config[segment]

        return config

    def source(self, step: Source) -> DataStream:
        config: SegmentConfig = self.resolve_segment_config(step)
        step_config: StepConfig = config["steps_config"][step.name]
        consumer_config: KafkaConsumerConfig = step_config["common"]

        assert hasattr(step, "stream_name")
        topic = step.stream_name

        deserialization_schema = SimpleStringSchema()

        # TODO: Look into using KafkaSource instead
        kafka_consumer = FlinkKafkaConsumer(
            topics=self.environment_config["topics"][topic],
            deserialization_schema=deserialization_schema,
            properties={
                "bootstrap.servers": consumer_config["bootstrap_servers"],
                "group.id": consumer_config["consumer_group"],
            },
        )

        source_stream = self.env.add_source(kafka_consumer)
        return self.resolve_outoing_chain(step, source_stream, config)

    def sink(self, step: Sink, stream: DataStream) -> DataStreamSink:
        config: SegmentConfig = self.resolve_segment_config(step)
        step_config: StepConfig = config["steps_config"][step.name]
        producer_config: KafkaProducerConfig = step_config["common"]

        assert hasattr(step, "stream_name")
        topic = step.stream_name

        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(producer_config["bootstrap_servers"])
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(
                    self.environment_config["topics"][topic],
                )
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

        sink_stream = self.resolve_incoming_chain(step, stream, config).sink_to(sink)
        return self.resolve_sink_chain(sink_stream, config)

    def filter(self, step: Filter, stream: DataStream) -> DataStream:
        config: SegmentConfig = self.resolve_segment_config(step)

        imported_fn = step.resolved_function

        filter_stream = self.resolve_incoming_chain(step, stream, config).filter(
            func=lambda msg: imported_fn(msg)
        )

        return self.resolve_outoing_chain(step, filter_stream, config)

    def map(self, step: Map, stream: DataStream) -> DataStream:
        config: SegmentConfig = self.resolve_segment_config(step)
        imported_fn = step.resolved_function

        return_type = imported_fn.__annotations__["return"]

        # TODO: Ensure output type is configurable like the schema above
        map_stream = self.resolve_incoming_chain(step, stream, config).map(
            func=lambda msg: imported_fn(msg),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

        return self.resolve_outoing_chain(step, map_stream, config)

    def flat_map(self, step: FlatMap, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        return_type = get_type_hints(imported_fn)["return"]

        # TODO: Ensure output type is configurable like the schema above
        return stream.flat_map(
            func=lambda msg: imported_fn(msg),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: DataStream,
    ) -> DataStream:
        config: SegmentConfig = self.resolve_segment_config(step)

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
            windowed_stream = self.resolve_incoming_chain(step, time_stream, config).window_all(
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

        return self.resolve_outoing_chain(step, aggregate_stream, config)

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> MutableMapping[str, Any]:
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
        routed_stream = stream.process(routing_process_func)

        return {
            route.tag_id: routed_stream.get_side_output(route) for route in output_tags.values()
        }

    def shutdown(self) -> None:
        raise NotImplementedError

    def run(self) -> None:
        self.env.execute()
