from dataclasses import dataclass

class Pipeline:
    def __init__(self) -> None:
        self.steps = {}
        self.edges = {}
        self.sources = []

    def register(self, step):
        assert step.name not in self.steps
        self.steps[step.name] = step

    def register_edge(self, _from, _to):
        self.edges.setdefault(_from.name, []).append(_to.name)

    def register_source(self, step):
        self.sources.append(step)


@dataclass
class _Stage:
    name: str
    ctx: Pipeline

class Stage(_Stage):
    def __post_init__(self):
        self.ctx.register(self)

class Source(Stage):
    def __post_init__(self):
        super().__post_init__()
        self.ctx.register_source(self)

    def apply_source(self, env, environment_config):
        ...

@dataclass
class WithInput(Stage):
    inputs: list[Stage]

    def __post_init__(self):
        super().__post_init__()
        for input in self.inputs:
            self.ctx.register_edge(input, self)

    def apply_edge(self, stream, environment_config):
        ...


@dataclass
class RawKafkaSource(Source):
    logical_topic: str

    def apply_source(self, env, environment_config):
        # TODO: split this out into a completely separate file?
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream.connectors import FlinkKafkaConsumer  # type: ignore
        KAFKA_BROKER = "localhost:9092"
        kafka_consumer = FlinkKafkaConsumer(
            topics=environment_config['topics'][self.logical_topic],
            deserialization_schema=SimpleStringSchema(),
            properties={
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": "python-flink-consumer",
            },
        )

        kafka_consumer.set_start_from_earliest()
        return env.add_source(kafka_consumer)

@dataclass
class Printer(WithInput):
    def apply_edge(self, stream, environment_config):
        return stream.print()
