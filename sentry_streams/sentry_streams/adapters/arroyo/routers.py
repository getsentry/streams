from typing import Iterable, Mapping

from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.pipeline.pipeline import Branch


def build_branches(current_route: Route, branches: Iterable[Branch]) -> Mapping[str, Route]:
    """
    Build branches for the given route.
    """
    return {
        branch.name: Route(
            source=current_route.source,
            waypoints=[*current_route.waypoints, branch.name],
        )
        for branch in branches
    }
