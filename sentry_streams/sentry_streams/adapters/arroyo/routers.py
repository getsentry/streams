from typing import Iterable, Mapping

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.pipeline.pipeline import Pipeline


def build_branches(current_route: Route, branches: Iterable[Pipeline]) -> Mapping[str, Route]:
    """
    Build branches for the given route.
    """
    ret = {}
    for branch in branches:
        assert branch.root is not None
        ret[branch.root.name] = Route(
            source=current_route.source,
            waypoints=[*current_route.waypoints, branch.root.name],
        )
    return ret
