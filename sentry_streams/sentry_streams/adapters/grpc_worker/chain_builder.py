from typing import Any, Mapping, MutableMapping, Optional, Tuple

from sentry_streams.adapters.grpc_worker.chain import ChainSegment, ChainStep
from sentry_streams.rust_streams import Route


def build_route_key(route: Route) -> str:
    return f"{route.source}, {route.waypoints}"


class ChainBuilder:

    def __init__(self) -> None:
        self.__incomplete_chains: MutableMapping[str, Tuple[str, ChainSegment]] = {}
        self.__chains: MutableMapping[str, ChainSegment] = {}

    def init_chain(self, route: Route, step_name: str, schema: Optional[str] = None) -> None:
        route_key = build_route_key(route)
        if route_key in self.__incomplete_chains:
            raise ValueError(f"Chain {route} already initialized")
        self.__incomplete_chains[route_key] = (step_name, ChainSegment([], schema))

    def add_step(self, route: Route, step: ChainStep[Any, Any]) -> None:
        route_key = build_route_key(route)
        if route_key not in self.__incomplete_chains:
            raise ValueError(f"Chain {route} not initialized")

        self.__incomplete_chains[route_key][1].steps.append(step)

    def finalize_chain(self, route: Route) -> None:
        route_key = build_route_key(route)
        self.finalize_chain_by_key(route_key)

    def finalize_chain_by_key(self, route_key: str) -> None:
        if route_key not in self.__incomplete_chains:
            raise ValueError(f"Chain {route_key} not initialized")
        step, chain = self.__incomplete_chains[route_key]
        self.__chains[step] = chain
        del self.__incomplete_chains[route_key]

    def finalize_all(self) -> Mapping[int, ChainSegment]:
        for route_key in list(self.__incomplete_chains.keys()):
            self.finalize_chain_by_key(route_key)

        route_names = list(self.__chains.keys())
        for key, chain in self.__chains.items():
            print(f"Chain {key}: {chain.steps}")

        return {i: self.__chains[route_names[i]] for i in range(len(route_names))}

    def exists(self, route: Route) -> bool:
        return build_route_key(route) in self.__incomplete_chains
