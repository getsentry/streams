import os
from typing import Any, Self

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import (  # type: ignore[attr-defined]
    FlinkKafkaConsumer,
)
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)
from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.modules import get_module
from sentry_streams.pipeline import Map, Step


class FlinkAdapter(StreamAdapter):
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

    def map(self, step: Map, stream: Any) -> Any:
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

        # TODO: Ensure output type is configurable like the schema above
        return stream.map(func=lambda msg: imported_fn(msg), output_type=Types.STRING())

    def run(self) -> None:
        self.env.execute()
