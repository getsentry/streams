import json
from dataclasses import dataclass
from typing import Self

from sentry_kafka_schemas.schema_types.snuba_spans_v1 import SpanEvent

from sentry_streams.pipeline.function_template import (
    Accumulator,
)
from sentry_streams.pipeline.message import Message


@dataclass
class Segment:
    total_duration: int
    spans: list[SpanEvent]


def build_segment_json(message: Message[Segment]) -> str:
    """
    Build a JSON str from a Segment
    """
    value = message.payload
    d = {"segment": value.spans, "total_duration": value.total_duration}

    return json.dumps(d)


class SpansBuffer(Accumulator[Message[SpanEvent], Segment]):
    """
    Ingests spans into a window. Builds a Segment from each
    window, which contains the list of SpanEvents seen as well
    as the total duration across SpanEvents.

    TODO: Group by trace_id
    """

    def __init__(self) -> None:
        self.spans_list: list[SpanEvent] = []
        self.total_duration = 0

    def add(self, value: Message[SpanEvent]) -> Self:
        self.spans_list.append(value.payload)
        self.total_duration += value.payload["duration_ms"]

        return self

    def get_value(self) -> Segment:
        return Segment(self.total_duration, self.spans_list)

    def merge(self, other: Self) -> Self:
        self.spans_list = self.spans_list + other.spans_list
        self.total_duration = self.total_duration + other.total_duration

        return self
