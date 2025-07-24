from abc import ABC, abstractmethod
from typing import Any, Callable, Mapping, Sequence

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.config_types import MultiProcessConfig
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import Broadcast, Filter, GCSSink, Reduce, Router
from sentry_streams.rust_streams import PyKafkaProducerConfig


class ArroyoConsumer(ABC):
    def __init__(self) -> None:
        self.__chain: Sequence[Mapping[str, Any]] = []

    @abstractmethod
    def add_gcs_sink(self, step_name: str, route: Route, bucket: str, step: GCSSink) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_stream_sink(
        self, step_name: str, route: Route, stream_name: str, kafka_config: PyKafkaProducerConfig
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_map(self, step_name: str, route: Route) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_filter(self, step_name: str, route: Route, step: Filter[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_multiprocess_map(
        self,
        step_name: str,
        route: Route,
        config: MultiProcessConfig,
        func: Callable[[Message[Any]], Message[Any]],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_broadcast(self, step_name: str, route: Route, step: Broadcast[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_router(self, step_name: str, route: Route, step: Router[Any, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def add_reduce(self, step_name: str, route: Route, step: Reduce[Any, Any, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def dump(self) -> Any:
        raise NotImplementedError


class DumpingConsumer(ArroyoConsumer):
    """
    A consumer that generates YAML descriptions instead of building actual consumers.
    This is useful for debugging and understanding pipeline configurations.
    """

    def __init__(self, source: str, kafka_config: Any, topic: str, schema: str | None) -> None:
        super().__init__()
        self.source = source
        self.kafka_config = kafka_config
        self.topic = topic
        self.schema = schema
        self.steps: list[dict[str, Any]] = []

    def add_gcs_sink(self, step_name: str, route: Route, bucket: str, step: GCSSink) -> None:
        """Add a GCS sink step to the YAML description."""
        step_description = {
            "type": "gcs_sink",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
            "bucket": bucket,
            "object_generator": str(step.object_generator),
        }
        self.steps.append(step_description)

    def add_stream_sink(
        self, step_name: str, route: Route, stream_name: str, kafka_config: PyKafkaProducerConfig
    ) -> None:
        """Add a stream sink step to the YAML description."""
        step_description = {
            "type": "stream_sink",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
            "stream_name": stream_name,
            "kafka_config": {
                "bootstrap_servers": kafka_config.bootstrap_servers,
                "override_params": (
                    dict(kafka_config.override_params) if kafka_config.override_params else {}
                ),
            },
        }
        self.steps.append(step_description)

    def add_map(self, step_name: str, route: Route) -> None:
        """Add a map step to the YAML description."""
        step_description = {
            "type": "map",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
        }
        self.steps.append(step_description)

    def add_filter(self, step_name: str, route: Route, step: Filter[Any]) -> None:
        """Add a filter step to the YAML description."""
        step_description = {
            "type": "filter",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
        }
        self.steps.append(step_description)

    def add_multiprocess_map(
        self,
        step_name: str,
        route: Route,
        config: MultiProcessConfig,
        func: Callable[[Message[Any]], Message[Any]],
    ) -> None:
        """Add a multiprocess map step to the YAML description."""
        step_description = {
            "type": "multiprocess_map",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
            "config": {
                "batch_size": config["batch_size"],
                "batch_time": config["batch_time"],
                "processes": config["processes"],
                "input_block_size": config.get("input_block_size"),
                "output_block_size": config.get("output_block_size"),
                "max_input_block_size": config.get("max_input_block_size"),
                "max_output_block_size": config.get("max_output_block_size"),
            },
        }
        self.steps.append(step_description)

    def add_broadcast(self, step_name: str, route: Route, step: Broadcast[Any]) -> None:
        """Add a broadcast step to the YAML description."""
        step_description = {
            "type": "broadcast",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
            "downstream_routes": [branch.root.name for branch in step.routes],
        }
        self.steps.append(step_description)

    def add_router(self, step_name: str, route: Route, step: Router[Any, Any]) -> None:
        """Add a router step to the YAML description."""
        step_description = {
            "type": "router",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
            "downstream_routes": [branch.root.name for branch in step.routing_table.values()],
        }
        self.steps.append(step_description)

    def add_reduce(self, step_name: str, route: Route, step: Reduce[Any, Any, Any]) -> None:
        """Add a reduce step to the YAML description."""
        step_description = {
            "type": "reduce",
            "step_name": step_name,
            "route": {"source": route.source, "waypoints": route.waypoints},
        }
        self.steps.append(step_description)

    def run(self) -> None:
        """This method does nothing for the dumping consumer."""
        pass

    def dump(self) -> dict[str, Any]:
        """Return the complete YAML description of the consumer."""
        return {
            "consumer": {
                "source": self.source,
                "topic": self.topic,
                "schema": self.schema,
                "kafka_config": {
                    "bootstrap_servers": self.kafka_config.bootstrap_servers,
                    "group_id": self.kafka_config.group_id,
                    "auto_offset_reset": str(self.kafka_config.auto_offset_reset),
                    "strict_offset_reset": self.kafka_config.strict_offset_reset,
                    "max_poll_interval_ms": self.kafka_config.max_poll_interval_ms,
                    "override_params": (
                        dict(self.kafka_config.override_params)
                        if self.kafka_config.override_params
                        else {}
                    ),
                },
                "steps": self.steps,
            }
        }
