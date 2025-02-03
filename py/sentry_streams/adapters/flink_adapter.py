from typing import Any, Mapping

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import (  # type: ignore[attr-defined]
    FlinkKafkaConsumer,
)
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)
from sentry_streams.adapters.stream_adapter import StreamAdapter


class FlinkAdapter(StreamAdapter):

    def __init__(self, config: dict[str, Any], env: StreamExecutionEnvironment) -> None:
        self.environment_config = config
        self.env = env

    def source(self, step_config: Mapping[str, Any]) -> Any:
        topic = step_config["topic"]

        kafka_consumer = FlinkKafkaConsumer(
            topics=self.environment_config["topics"][topic],
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": self.environment_config["broker"],
                "group.id": "python-flink-consumer",
            },
        )

        return self.env.add_source(kafka_consumer)

    def sink(self, step_config: Mapping[str, Any], stream: Any) -> Any:
        topic = step_config["topic"]

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
