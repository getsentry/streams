from typing import Any, Callable

from arroyo.processing.strategies.run_task_with_multiprocessing import (
    MultiprocessingPool,
)

from sentry_streams.adapters.arroyo.dumping_consumer import ArroyoConsumer
from sentry_streams.adapters.arroyo.multi_process_delegate import (
    MultiprocessDelegateFactory,
)
from sentry_streams.adapters.arroyo.reduce_delegate import ReduceDelegateFactory
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.config_types import MultiProcessConfig
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    GCSSink,
    Map,
    Reduce,
    Router,
)
from sentry_streams.rust_streams import ArroyoConsumer as RustNativeArroyoConsumer
from sentry_streams.rust_streams import (
    PyKafkaConsumerConfig,
    PyKafkaProducerConfig,
)
from sentry_streams.rust_streams import Route as RustRoute
from sentry_streams.rust_streams import (
    RuntimeOperator,
)


class RustArroyoConsumer(ArroyoConsumer):

    def __init__(
        self, source: str, kafka_config: PyKafkaConsumerConfig, topic: str, schema: str | None
    ) -> None:
        self.__consumer = RustNativeArroyoConsumer(source, kafka_config, topic, schema)

    def add_gcs_sink(self, step_name: str, route: Route, bucket: str, step: GCSSink) -> None:
        self.__consumer.add_step(RuntimeOperator.GCSSink(route, bucket, step.object_generator))

    def add_stream_sink(
        self, step_name: str, route: Route, stream_name: str, kafka_config: PyKafkaProducerConfig
    ) -> None:
        self.__consumer.add_step(RuntimeOperator.StreamSink(route, stream_name, kafka_config))

    def add_map(self, step_name: str, route: Route, step: Map[Any, Any]) -> None:
        pass

    def add_filter(self, step_name: str, route: Route, step: Filter[Any]) -> None:
        def filter_msg(msg: Message[Any]) -> bool:
            return step.resolved_function(msg)

        self.__consumer.add_step(RuntimeOperator.Filter(route, filter_msg))

    def add_multiprocess_map(
        self,
        step_name: str,
        route: Route,
        config: MultiProcessConfig,
        func: Callable[[Message[Any]], Message[Any]],
    ) -> None:
        self.__consumer.add_step(
            RuntimeOperator.PythonAdapter(
                RustRoute(route.source, route.waypoints),
                MultiprocessDelegateFactory(
                    func,
                    config["batch_size"],
                    config["batch_time"],
                    MultiprocessingPool(
                        num_processes=config["processes"],
                    ),
                    input_block_size=config.get("input_block_size"),
                    output_block_size=config.get("output_block_size"),
                    max_input_block_size=config.get("max_input_block_size"),
                    max_output_block_size=config.get("max_output_block_size"),
                ),
            )
        )

    def add_broadcast(self, step_name: str, route: Route, step: Broadcast[Any]) -> None:
        self.__consumer.add_step(
            RuntimeOperator.Broadcast(
                RustRoute(route.source, route.waypoints),
                downstream_routes=[branch.root.name for branch in step.routes],
            )
        )

    def add_router(self, step_name: str, route: Route, step: Router[Any, Any]) -> None:
        def routing_function(msg: Message[Any]) -> str:
            waypoint = step.routing_function(msg)
            branch = step.routing_table[waypoint]
            return branch.root.name

        self.__consumer.add_step(
            RuntimeOperator.Router(
                RustRoute(route.source, route.waypoints),
                routing_function,
                downstream_routes=[branch.root.name for branch in step.routing_table.values()],
            )
        )

    def add_reduce(self, step_name: str, route: Route, step: Reduce[Any, Any, Any]) -> None:
        self.__consumer.add_step(
            RuntimeOperator.PythonAdapter(
                RustRoute(route.source, route.waypoints), ReduceDelegateFactory(step)
            )
        )

    def run(self) -> None:
        self.__consumer.run()

    def dump(self) -> Any:
        raise NotImplementedError
