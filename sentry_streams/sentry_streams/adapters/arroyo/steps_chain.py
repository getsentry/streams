from functools import partial
from typing import Any, Callable, MutableMapping, MutableSequence, Sequence, Tuple

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.pipeline.message import Message, PyMessage, PyRawMessage
from sentry_streams.pipeline.pipeline import Map


def transform(chain: Sequence[Map], message: Message[Any]) -> Message[Any]:
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
                headers=next_msg.headers,
                timestamp=next_msg.timestamp,
                schema=next_msg.schema,
            )
        else:
            next_msg = PyMessage(
                payload=ret,
                headers=next_msg.headers,
                timestamp=next_msg.timestamp,
                schema=next_msg.schema,
            )
    return next_msg


# Route is not hashable (it contains a list) so it cannot be the key
# of a Mapping.
HashableRoute = Tuple[str, Tuple[str, ...]]


def _hashable_route(route: Route) -> HashableRoute:
    return (route.source, tuple(route.waypoints))


class StepsChains:
    """
    Builds chains of transformations to be executed in the same
    Arroyo strategy.

    The main use case is to execute multiple sequential transformations
    like parse, process, serialize, in the same multi process step.
    In order to achieve this, such transformations have to be packaged
    into a single function that is passed to the multiprocess step.

    As of now this only supports map as the multiprocess transformer
    only supports 1:1 transformations. We should expand that step
    to support n:m so we can parallelize reduce and filter.
    """

    def __init__(self) -> None:
        self.__chains: MutableMapping[HashableRoute, MutableSequence[Map]] = {}

    def add_map(self, route: Route, step: Map) -> None:
        hashable_route = _hashable_route(route)
        if hashable_route not in self.__chains:
            self.__chains[hashable_route] = []
        self.__chains[hashable_route].append(step)

    def finalize(self, route: Route) -> Callable[[Message[Any]], Message[Any]]:
        hashable_route = _hashable_route(route)
        chain = self.__chains[hashable_route]
        del self.__chains[hashable_route]
        return partial(transform, chain)

    def exists(self, route: Route) -> bool:
        return _hashable_route(route) in self.__chains
