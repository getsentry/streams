import os
from typing import Self, TypeVar, Union

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
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
    Map,
    Reduce,
    Sink,
    Source,
)
from sentry_streams.pipeline.window import MeasurementUnit

from sentry_flink.flink.flink_translator import (
    FlinkAggregate,
    FlinkGroupBy,
    build_flink_window,
    is_standard_type,
)

T = TypeVar("T")


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

    # def load_function(self, step: TransformStep[T]) -> Callable[..., T]:
    #     """
    #     Takes a transform step containing a function, and either returns
    #     function (if it's a path to a module).
    #     """
    #     # TODO: break out the dynamic loading logic into a
    #     # normalization layer before the flink adapter
    #     if isinstance(step.function, str):
    #         fn_path = step.function
    #         mod, cls, fn = fn_path.rsplit(".", 2)

    #         try:
    #             module = get_module(mod)

    #         except ImportError:
    #             raise

    #         imported_cls = getattr(module, cls)
    #         imported_func = cast(Callable[..., T], getattr(imported_cls, fn))
    #         return imported_func
    #     else:
    #         return step.function

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
        return stream.filter(func=lambda msg: step.get_function(msg))

    def map(self, step: Map, stream: DataStream) -> DataStream:
        imported_fn = step.get_function(step)

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

    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: DataStream,
    ) -> DataStream:

        agg = step.aggregate_fn
        windowing = step.windowing

        flink_window = build_flink_window(windowing)

        # Optional parameters
        group_by = step.group_by_key
        agg_backend = step.aggregate_backend

        # TODO: Configure WatermarkStrategy as part of KafkaSource
        # Injecting strategy within a step like here produces
        # a new, watermarked stream
        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
        time_stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

        if group_by:
            group_by_key = step.group_by_key
            assert group_by_key is not None

            keyed_stream = time_stream.key_by(FlinkGroupBy(group_by_key))

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

    def run(self) -> None:
        self.env.execute()
