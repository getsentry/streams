from typing import Any, Iterable, Mapping

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.pipeline.pipeline import Pipeline


def build_branches(current_route: Route, branches: Iterable[Pipeline[Any]]) -> Mapping[str, Route]:
    """
    Build branches for the given route.
    """
    ret = {}
    for branch in branches:
        ret[branch.root.name] = Route(
            source=current_route.source,
            waypoints=[*current_route.waypoints, branch.root.name],
        )
    return ret
