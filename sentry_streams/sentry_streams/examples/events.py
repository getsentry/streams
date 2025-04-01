import json
from dataclasses import dataclass
from typing import Any, Generator, Self, Union

from sentry_streams.pipeline.function_template import Accumulator, GroupBy


@dataclass
class Event:
    project_id: int
    latency: int
    timestamp: int
    tags: list[str]
    value: int


@dataclass
class TimeSeriesDataPoint:
    alert_id: int
    latency: int
    alert_type: str


@dataclass
class p95AlertData:
    alert_id: int
    p95_latency: int
    name: str = "p95alert"

    def to_dict(self) -> dict[str, Union[str, int]]:
        return {
            "alert_id": self.alert_id,
            "alert_name": self.name,
            "p95_latency": self.p95_latency,
        }


@dataclass
class CountAlertData:
    alert_id: int
    event_count: int
    name: str = "eventcount"

    def to_dict(self) -> dict[str, Union[str, int]]:
        return {
            "alert_id": self.alert_id,
            "alert_name": self.name,
            "event_count": self.event_count,
        }


def build_event(value: str) -> Event:
    """
    Build a Span object from a JSON str
    """

    d: dict[str, Any] = json.loads(value)

    return Event(d["project_id"], d["latency"], d["timestamp"], d["tags"], d["value"])


def build_alert_json(alert: Union[p95AlertData, CountAlertData]) -> str:

    d = alert.to_dict()

    return json.dumps(d)


class AlertsBuffer(Accumulator[TimeSeriesDataPoint, Union[p95AlertData, CountAlertData]]):
    """
    An AlertsBuffer, which is created per-alert ID. Manages the aggregation of event data
    that pertains to each particular registered alert ID.
    """

    def __init__(self) -> None:
        self.latencies: list[int] = []
        self.count = 0
        self.alert_type: str
        self.alert_id: int

    def add(self, value: TimeSeriesDataPoint) -> Self:
        if value.alert_type == "count":
            self.count += 1
            self.alert_type = value.alert_type
            self.alert_id = value.alert_id

        if value.alert_type == "p95":
            self.latencies.append(value.latency)
            self.alert_type = value.alert_type
            self.alert_id = value.alert_id

        return self

    def get_value(self) -> Union[p95AlertData, CountAlertData]:
        # A fake p95 calculation, to serve as an example
        if self.alert_type == "count":
            return CountAlertData(alert_id=self.alert_id, event_count=self.count)

        else:
            return p95AlertData(alert_id=self.alert_id, p95_latency=max(self.latencies))

    def merge(self, other: Self) -> Self:
        # TODO: Use DataSketches
        self.latencies = self.latencies + other.latencies
        self.count = self.count + other.count

        return self


# alert rules for our app
REGISTERED_ALERTS = {
    4: {"type": "count", "threshold": 4},
    5: {"type": "count", "threshold": 2},
    6: {"type": "p95", "threshold": 4},
}
# maps project_id to alert rules
REGISTERED_PROJECT_ALERTS = {2: {"tag_a": 4, "tag_b": 6}, 1: 6}


def materialize_alerts(event: Event) -> Generator[TimeSeriesDataPoint, None, None]:
    """
    Generates (potentially multiple) time series data points per event data point.
    Looks up attributes of the event data point (in this case, project_id) to determine
    which registered alert(s) correspond to the current event. One event may be registered
    with multiple alert rules.
    """

    project_id = event.project_id
    alerts_for_project = REGISTERED_PROJECT_ALERTS[project_id]

    if isinstance(alerts_for_project, dict):
        tags = event.tags
        for tag in tags:
            alert_id = alerts_for_project[tag]
            alert_rule = REGISTERED_ALERTS[alert_id]
            alert_type = alert_rule["type"]
            assert isinstance(alert_type, str)

            alerting_event = TimeSeriesDataPoint(
                alert_id=alert_id,
                latency=event.latency,
                alert_type=alert_type,
            )
            yield alerting_event

    else:
        assert isinstance(alerts_for_project, int)
        alert_rule = REGISTERED_ALERTS[alerts_for_project]
        alert_type = alert_rule["type"]
        assert isinstance(alert_type, str)

        alerting_event = TimeSeriesDataPoint(
            alert_id=alerts_for_project,
            latency=event.latency,
            alert_type=alert_type,
        )
        yield alerting_event


class GroupByAlertID(GroupBy):

    def get_group_by_key(self, alerting_event: TimeSeriesDataPoint) -> int:
        return alerting_event.alert_id
