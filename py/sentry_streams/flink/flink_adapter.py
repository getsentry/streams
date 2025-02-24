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
from sentry_streams.modules import get_module
from sentry_streams.pipeline import Filter, Map, Step


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

    def load_function(self, step: Step) -> Any:
        """
        Takes a transform step containing a function, and either returns
        the function (if it's a Callable) or dynamically loads and returns the
        function (if it's a path to a module).
        """
        # TODO: break out the dynamic loading logic into a
        # normalization layer before the flink adapter
        assert hasattr(step, "function"), "load_function() requires a step containing a function."
        if isinstance(step.function, str):
            fn_path = step.function
            mod, cls, fn = fn_path.rsplit(".", 2)

            try:
                module = get_module(mod)

            except ImportError:
                raise

            imported_cls = getattr(module, cls)
            return getattr(imported_cls, fn)
        else:
            return step.function

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
        imported_fn = self.load_function(step)

        # TODO: Ensure output type is configurable like the schema above
        return stream.map(func=lambda msg: imported_fn(msg), output_type=Types.STRING())

    def filter(self, step: Filter, stream: Any) -> Any:
        imported_fn = self.load_function(step)
        return stream.filter(func=lambda msg: imported_fn(msg))
