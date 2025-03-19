import os
from typing import (
    Any,
    Callable,
    MutableMapping,
    Self,
    Union,
    cast,
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
)
from sentry_streams.pipeline.window import MeasurementUnit

from sentry_flink.flink.flink_translator import (
    FlinkAggregate,
    FlinkGroupBy,
    FlinkRoutingFunction,
    RoutingFuncReturnType,
    build_flink_window,
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
        self.environment_config = config
        self.env = env

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        env = StreamExecutionEnvironment.get_execution_environment()

        flink_config = config.get("flink", {})

        libs_path = flink_config.get("kafka_connect_lib_path")
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

        env.set_parallelism(flink_config.get("parallelism", 1))

        return cls(config, env)

    def source(self, step: Source) -> DataStream:
        assert hasattr(step, "logical_topic")
        topic = step.logical_topic

        deserialization_schema = SimpleStringSchema()

        # TODO: Look into using KafkaSource instead
        kafka_consumer = FlinkKafkaConsumer(
            topics=self.environment_config["topics"][topic],
            deserialization_schema=deserialization_schema,
            properties={
                "bootstrap.servers": self.environment_config["broker"],
                "group.id": "python-flink-consumer",
            },
        )

        return self.env.add_source(kafka_consumer)

    def sink(self, step: Sink, stream: DataStream) -> DataStreamSink:
        assert hasattr(step, "logical_topic")
        topic = step.logical_topic

        sink = (
            KafkaSink.builder()
            .set_bootstrap_servers(self.environment_config["broker"])
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

        return stream.sink_to(sink)

    def filter(self, step: Filter, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        return stream.filter(func=lambda msg: imported_fn(msg))

    def map(self, step: Map, stream: DataStream) -> DataStream:
        imported_fn = step.resolved_function

        return_type = imported_fn.__annotations__["return"]

        # TODO: Ensure output type is configurable like the schema above
        return stream.map(
            func=lambda msg: imported_fn(msg),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

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

        if group_by:
            keyed_stream = time_stream.key_by(FlinkGroupBy(group_by))

            windowed_stream: Union[WindowedStream, AllWindowedStream] = keyed_stream.window(
                flink_window
            )

        else:
            windowed_stream = time_stream.window_all(flink_window)

        return_type = agg().get_value.__annotations__["return"]

        # TODO: Figure out a systematic way to convert types
        return windowed_stream.aggregate(
            FlinkAggregate(agg, agg_backend),
            output_type=(
                translate_to_flink_type(return_type)
                if is_standard_type(return_type)
                else translate_custom_type(return_type)
            ),
        )

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> MutableMapping[str, Any]:
        routing_table = step.routing_table
        routing_func = cast(Callable[..., RoutingFuncReturnType], step.routing_function)

        # routing functions should only have a single parameter since we're using
        # Flink's ProcessFunction which only takes a single value as input
        routing_func_attr = routing_func.__annotations__
        del routing_func_attr["return"]
        message_type = list(routing_func_attr.values())[0]

        output_tags = {
            key: OutputTag(
                tag_id=routing_table[key].name,
                type_info=(
                    translate_to_flink_type(message_type)
                    if is_standard_type(message_type)
                    else translate_custom_type(message_type)
                ),
            )
            for key in routing_table
        }
        routing_process_func = FlinkRoutingFunction(routing_func, output_tags)
        routing_stream = stream.process(routing_process_func)

        routes_map: MutableMapping[str, Any] = {}
        for key in output_tags:
            routes_map[output_tags[key].tag_id] = routing_stream.get_side_output(output_tags[key])
        return routes_map

    def run(self) -> None:
        self.env.execute()
