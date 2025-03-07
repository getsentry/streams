import json
from dataclasses import dataclass
from typing import Any, Self

from sentry_streams.pipeline.function_template import (
    Accumulator,
)


@dataclass
class Span:
    span_id: int
    trace_id: int
    duration: int
    timestamp: int

    def to_dict(self) -> dict[str, int]:
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "duration": self.duration,
            "timestamp": self.timestamp,
        }


def build_span(value: str) -> Span:
    """
    Build a Span object from a JSON str
    """

    d: dict[str, Any] = json.loads(value)

    return Span(d["span_id"], d["trace_id"], d["duration"], d["timestamp"])


@dataclass
class Segment:
    total_duration: int
    spans: list[Span]


def build_segment_json(value: Segment) -> str:
    """
    Build a JSON str from a Segment
    """

    d = {"segment": [], "total_duration": value.total_duration}

    for span in value.spans:
        span_d = span.to_dict()

        assert isinstance(d["segment"], list)
        d["segment"].append(span_d)

    return json.dumps(d)


class SpansBuffer(Accumulator[Span, Segment]):
    """
    Ingests spans into a window. Builds a Segment from each
    window, which contains the list of Spans seen as well
    as the total duration across Spans.

    TODO: Group by trace_id
    """

    def __init__(self) -> None:
        self.spans_list: list[Span] = []
        self.total_duration = 0

    def add(self, value: Span) -> Self:
        self.spans_list.append(value)
        self.total_duration += value.duration

        return self

    def get_value(self) -> Segment:

        return Segment(self.total_duration, self.spans_list)

    def merge(self, other: Self) -> Self:
        self.spans_list = self.spans_list + other.spans_list
        self.total_duration = self.total_duration + other.total_duration

        return self
