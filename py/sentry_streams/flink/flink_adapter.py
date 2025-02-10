import importlib.util
import sys
from types import ModuleType
from typing import Any, MutableMapping

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
from sentry_streams.adapters.stream_adapter import StreamAdapter
from sentry_streams.pipeline import Step


class FlinkAdapter(StreamAdapter):
    # TODO: make the (de)serialization schema configurable

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
        fn_path = step.function
        mod, cls, fn = fn_path.rsplit(".", 2)

        module: ModuleType

        if mod in sys.modules:
            module = sys.modules[mod]

        elif (spec := importlib.util.find_spec(mod)) is not None:
            module = importlib.util.module_from_spec(spec)

        else:
            raise ImportError(f"Can't find module {mod}")

        # The output type must be specified
        # TODO: Remove hardcoded output type
        imported_cls = getattr(module, cls)
        imported_fn = getattr(imported_cls, fn)

        return stream.map(func=lambda msg: imported_fn(msg), output_type=Types.STRING())
