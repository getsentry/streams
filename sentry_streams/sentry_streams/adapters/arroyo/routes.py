from dataclasses import dataclass
from typing import Any, MutableSequence


@dataclass(frozen=True)
class Route:
    """
    Represents the route taken by a message in the pipeline when
    there are branches or multiple sources.

    Each pipeline step is assigned a route so it only processes
    messages that belong to it giving the illusion that Arroyo
    supports branches which it does not.

    The waypoints sequence contains the branches taken by the message
    in order following the pipeline.
    """

    source: str
    waypoints: MutableSequence[str]


@dataclass(frozen=True)
class RoutedValue:
    route: Route
    payload: Any
