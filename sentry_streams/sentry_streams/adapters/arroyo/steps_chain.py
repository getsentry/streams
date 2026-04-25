import logging
import pickle
from dataclasses import dataclass
from functools import partial
import time
from typing import Any, Callable, MutableMapping, MutableSequence, Sequence, Tuple

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.config_types import MultiProcessConfig
from sentry_streams.metrics import get_metrics, get_size, Metric
from sentry_streams.pipeline.message import Message, PyMessage, PyRawMessage
from sentry_streams.pipeline.msg_codecs import msg_parser
from sentry_streams.pipeline.pipeline import Map
import time

logger = logging.getLogger(__name__)


def input_metrics(name: str, message_size: int | None) -> float:
    t = time.time()
    metrics = get_metrics()
    tags = {"step": name}
    metrics.increment(Metric.INPUT_MESSAGES, tags=tags)
    if message_size is not None:
        # metrics.increment(Metric.INPUT_BYTES, tags=tags, value=message_size)
        pass
    return t


def output_metrics(
    name: str, error: str | None, start_time: float, message_size: int | None
) -> None:
    t = time.time()
    metrics = get_metrics()
    tags = {"step": name}
    if error:
        tags["error"] = error
        metrics.increment(Metric.ERRORS, tags=tags)

    metrics.increment(Metric.OUTPUT_MESSAGES, tags=tags)
    if message_size is not None:
        metrics.increment(Metric.OUTPUT_BYTES, tags=tags, value=message_size)
        pass
    metrics.timing(Metric.DURATION, t - start_time, tags=tags)
    pass


def fake_transform(message: Message[Any]) -> Message[Any]:
    next_msg = message
    msg_size = get_size(next_msg.payload) if hasattr(next_msg, "payload") else None
    # msg_size = 100
    start_time = input_metrics("fake_step", msg_size)
    has_error = output_size = None
    try:
        result = msg_parser(next_msg)
        output_size = get_size(result)
        # output_size = 100
    except Exception as e:
        has_error = str(e.__class__.__name__)
        raise e
    finally:
        # output_metrics("fake_step", has_error, start_time, output_size)
        pass
    ret = result

    if isinstance(ret, bytes):
        # If `ret`` is bytes then function is Callable[Message[TMapIn], bytes].
        # Thus TMapOut = bytes.
        next_msg = PyRawMessage(
            payload=ret,
            headers=[],
            timestamp=next_msg.timestamp,
            schema=next_msg.schema,
        )
    else:
        next_msg = PyMessage(
            payload=ret,
            headers=[],
            timestamp=next_msg.timestamp,
            schema=next_msg.schema,
        )
    return next_msg


def transform(chain: Sequence[Map[Any, Any]], message: Message[Any]) -> Message[Any]:
    """
    Executes a series of chained transformations.
    This function needs to be outside of the `StepsChain` class to
    make it possible to pass it to a MultiProcess pool.
    """
    next_msg = message
    for step in chain:
        ret = step.resolved_function(next_msg)
        if isinstance(ret, bytes):
            # If `ret`` is bytes then function is Callable[Message[TMapIn], bytes].
            # Thus TMapOut = bytes.
            next_msg = PyRawMessage(
                payload=ret,
                headers=[],
                timestamp=next_msg.timestamp,
                schema=next_msg.schema,
            )
        else:
            next_msg = PyMessage(
                payload=ret,
                headers=[],
                timestamp=next_msg.timestamp,
                schema=next_msg.schema,
            )
    return next_msg


# Route is not hashable (it contains a list) so it cannot be the key
# of a Mapping.
HashableRoute = Tuple[str, Tuple[str, ...]]


def _hashable_route(route: Route) -> HashableRoute:
    return (route.source, tuple(route.waypoints))


@dataclass
class ChainConfig:
    steps: MutableSequence[Map[Any, Any]]
    # TODO: Support abstract config for multi threading and
    # single threaded. As of writing there is nothing to configure
    # for those cases.
    parallelism: MultiProcessConfig | None


class TransformChains:
    """
    Builds chains of transformations to be executed in the same
    Arroyo strategy in parallel.

    The main use case is to execute multiple sequential transformations
    like parse, process, serialize, in the same multi process step.
    In order to achieve this, such transformations have to be packaged
    into a single function that is passed to the multiprocess step.

    As of now this only supports map as the multiprocess transformer
    only supports 1:1 transformations. We should expand that step
    to support n:m so we can parallelize reduce and filter.
    """

    def __init__(self) -> None:
        self.__chains: MutableMapping[HashableRoute, ChainConfig] = {}

    def init_chain(self, route: Route, config: MultiProcessConfig | None) -> None:
        logger.info(f"Initializing chain {route}")
        hashable_route = _hashable_route(route)
        if hashable_route in self.__chains:
            raise ValueError(f"Chain {route} already initialized")
        self.__chains[hashable_route] = ChainConfig([], config)

    def add_map(self, route: Route, step: Map[Any, Any]) -> None:
        logger.info(f"Chaining map {step.name} to transform chain")
        hashable_route = _hashable_route(route)
        if hashable_route not in self.__chains:
            raise ValueError(f"Chain {route} not initialized")
        self.__chains[hashable_route].steps.append(step)

    def finalize(
        self, route: Route
    ) -> Tuple[MultiProcessConfig | None, Callable[[Message[Any]], Message[Any]]]:
        hashable_route = _hashable_route(route)
        if hashable_route not in self.__chains:
            raise ValueError(f"Chain {route} not initialized")
        chain = self.__chains[hashable_route]
        del self.__chains[hashable_route]

        func = partial(transform, chain.steps)

        # Always verify the function is picklable, even if multiprocessing is not currently enabled.
        # This ensures that users can safely enable multiprocessing in production without
        # encountering pickling errors.
        try:
            pickle.dumps(func)
        except (pickle.PicklingError, AttributeError, TypeError) as e:
            step_names = ", ".join(step.name for step in chain.steps)
            raise TypeError(
                f"Transform chain for route {route} is not picklable (steps: {step_names}). "
                f"This is required for multiprocessing support. "
                f"Ensure all step functions are defined at module level and not as local functions. "
                f"Error: {e}"
            ) from e

        return (chain.parallelism, fake_transform)
        # return (chain.parallelism, func)

    def exists(self, route: Route) -> bool:
        return _hashable_route(route) in self.__chains
