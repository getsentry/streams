from typing import Any, MutableMapping, Union

from pyflink.common import Types, WatermarkStrategy
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

from sentry_streams.adapters.stream_adapter import StreamAdapter
from sentry_streams.flink.flink_fn_translator import (
    FLINK_TYPE_MAP,
    FlinkAggregate,
    FlinkGroupBy,
    FlinkWindows,
)
from sentry_streams.modules import get_module
from sentry_streams.pipeline import Map, Reduce, Step


class FlinkAdapter(StreamAdapter[DataStream, DataStreamSink]):
    # TODO: make the (de)serialization schema configurable
    # TODO: infer the output type from steps which
    # perform transformations / maps.

    # NOTE: Output type must be specified for steps
    # that send data to a next step that
    # performs serialization (e.g. Map --> Sink)

    def __init__(self, config: MutableMapping[str, Any], env: StreamExecutionEnvironment) -> None:
        self.environment_config = config
        self.env = env

    def source(self, step: Step) -> DataStream:
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

    def sink(self, step: Step, stream: DataStream) -> DataStreamSink:
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

    def map(self, step: Map, stream: DataStream) -> DataStream:
        if isinstance(step.function, str):
            fn_path = step.function
            mod, cls, fn = fn_path.rsplit(".", 2)

            try:
                module = get_module(mod)

            except ImportError:
                raise

            imported_cls = getattr(module, cls)
            imported_fn = getattr(imported_cls, fn)
        else:
            imported_fn = step.function

        return_type = imported_fn.__annotations__["return"]
        # TODO: Ensure output type is configurable like the schema above
        return stream.map(
            func=lambda msg: imported_fn(msg), output_type=FLINK_TYPE_MAP[return_type]
        )

    def reduce(self, step: Reduce, stream: DataStream) -> DataStream:

        agg = step.aggregate_fn
        windowing = step.windowing
        flink_window = FlinkWindows(windowing)
        window_assigner = flink_window.build_window()

        # The only optional parameter
        group_by = step.group_by_key

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
                window_assigner
            )

        else:
            windowed_stream = time_stream.window_all(window_assigner)

        return_type = agg.get_output.__annotations__["return"]

        # TODO: Figure out a systematic way to convert types
        return windowed_stream.aggregate(
            FlinkAggregate(agg),
            accumulator_type=Types.TUPLE([Types.STRING(), Types.INT()]),
            output_type=FLINK_TYPE_MAP[return_type],
        )
