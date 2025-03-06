import json
from dataclasses import dataclass
from typing import Any, Self

from sentry_streams.user_functions.function_template import Accumulator


@dataclass
class Event:
    project_id: int
    latency: int
    timestamp: int


@dataclass
class AlertData:
    p95_latency: int
    event_count: int

    def to_dict(self) -> dict[str, int]:
        return {
            "p95_latency": self.p95_latency,
            "event_count": self.event_count,
        }


def build_event(value: str) -> Event:
    """
    Build a Span object from a JSON str
    """

    d: dict[str, Any] = json.loads(value)

    return Event(d["project_id"], d["latency"], d["timestamp"])


def build_alert_json(alert: AlertData) -> str:

    d = alert.to_dict()

    return json.dumps(d)


class AlertsBuffer(Accumulator[Event, AlertData]):

    def __init__(self) -> None:
        self.latencies: list[int] = []
        self.count = 0

    def add(self, value: Event) -> Self:
        self.latencies.append(value.latency)
        self.count += 1

        return self

    def get_value(self) -> AlertData:
        # A fake p95 calculation, to serve as an example
        fake_p95 = max(self.latencies)

        return AlertData(p95_latency=fake_p95, event_count=self.count)

    def merge(self, other: Self) -> Self:
        # TODO: Use DataSketches
        self.latencies = self.latencies + other.latencies
        self.count = self.count + other.count

        return self
