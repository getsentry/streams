from typing import Any, MutableMapping

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import (  # type: ignore[attr-defined]
    FlinkKafkaConsumer,
)
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)
from pyflink.datastream.window import TumblingEventTimeWindows
from sentry_streams.adapters.stream_adapter import StreamAdapter
from sentry_streams.flink.flink_agg_fn import FlinkAggregate, FlinkGroupBy
from sentry_streams.pipeline import Step
from sentry_streams.user_functions.function_template import Accumulator, GroupBy

FLINK_TYPE_MAP = {tuple[str, int]: Types.TUPLE([Types.STRING(), Types.INT()]), str: Types.STRING()}


class FlinkAdapter(StreamAdapter):
    # TODO: make the (de)serialization schema configurable
    # TODO: infer the output type from steps which
    # perform transformations / maps.

    # NOTE: Output type must be specified for steps
    # that send data to a next step that
    # performs serialization (e.g. Map --> Sink)

    def __init__(self, config: MutableMapping[str, Any], env: StreamExecutionEnvironment) -> None:
        self.environment_config = config
        self.env = env

    def source(self, step: Step) -> Any:
        assert hasattr(step, "logical_topic")
        topic = step.logical_topic

        deserialization_schema = SimpleStringSchema()
        kafka_consumer = FlinkKafkaConsumer(
            topics=self.environment_config["topics"][topic],
            deserialization_schema=deserialization_schema,
            properties={
                "bootstrap.servers": self.environment_config["broker"],
                "group.id": "python-flink-consumer",
            },
        )

        return self.env.add_source(kafka_consumer)

    def sink(self, step: Step, stream: Any) -> Any:
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

    def map(self, step: Step, stream: Any) -> Any:

        assert hasattr(step, "function")
        imported_fn = step.function
        return_type = imported_fn.__annotations__["return"]

        # TODO: Ensure output type is configurable like the schema above
        return stream.map(
            func=lambda msg: imported_fn(msg), output_type=FLINK_TYPE_MAP[return_type]
        )

    def reduce(self, step: Step, stream: Any) -> Any:
        assert hasattr(step, "group_by_key")
        key: GroupBy = step.group_by_key

        assert hasattr(step, "aggregate_fn")
        agg: Accumulator = step.aggregate_fn

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_idleness(
            Duration.of_seconds(5)
        )
        time_stream = stream.assign_timestamps_and_watermarks(watermark_strategy)

        keyed_stream = time_stream.key_by(FlinkGroupBy(key))
        windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(1)))

        return_type = agg.get_output.__annotations__["return"]

        # TODO: Figure out a systematic way to convert types
        return windowed_stream.aggregate(
            FlinkAggregate(agg),
            accumulator_type=Types.TUPLE([Types.STRING(), Types.INT()]),
            output_type=FLINK_TYPE_MAP[return_type],
        )
