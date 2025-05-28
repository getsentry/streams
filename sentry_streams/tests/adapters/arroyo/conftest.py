from datetime import timedelta
from typing import Callable, MutableSequence, Self

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Topic
from arroyo.utils.clock import MockedClock
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.chain import (
    Filter,
    Map,
    Parser,
    Reducer,
    Serializer,
    StreamSink,
    segment,
    streaming_source,
)
from sentry_streams.pipeline.function_template import Accumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import Pipeline
from sentry_streams.pipeline.window import SlidingWindow

# def decode(msg: bytes) -> str:
#     return msg.decode("utf-8")


def basic_map(msg: Message[IngestMetric]) -> IngestMetric:
    payload = msg.payload
    payload["name"] = "new_metric"

    return payload


@pytest.fixture
def broker() -> LocalBroker[KafkaPayload]:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, MockedClock())
    broker.create_topic(Topic("events"), 1)
    broker.create_topic(Topic("transformed-events"), 1)
    broker.create_topic(Topic("transformed-events-2"), 1)
    broker.create_topic(Topic("ingest-metrics"), 1)
    return broker


@pytest.fixture
def metric() -> IngestMetric:
    return {
        "org_id": 420,
        "project_id": 420,
        "name": "s:sessions/user@none",
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
        },
        "timestamp": 1846062325,
        "type": "s",
        "retention_days": 90,
        "value": [1617781333],
    }


@pytest.fixture
def transformed_metric() -> IngestMetric:
    return {
        "org_id": 420,
        "project_id": 420,
        "name": "new_metric",
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
        },
        "timestamp": 1846062325,
        "type": "s",
        "retention_days": 90,
        "value": [1617781333],
    }


class TestTransformerBatch(Accumulator[Message[IngestMetric], MutableSequence[IngestMetric]]):

    def __init__(self) -> None:
        self.batch: MutableSequence[IngestMetric] = []

    def add(self, value: Message[IngestMetric]) -> Self:
        self.batch.append(value.payload)

        return self

    def get_value(self) -> MutableSequence[IngestMetric]:
        return self.batch

    def merge(self, other: Self) -> Self:
        self.batch.extend(other.batch)

        return self


@pytest.fixture
def transformer() -> Callable[[], TestTransformerBatch]:
    return TestTransformerBatch


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = (
        streaming_source("myinput", stream_name="ingest-metrics")
        .apply("decoder", Parser(msg_type=IngestMetric))
        .apply("myfilter", Filter(lambda msg: msg.payload["type"] == "s"))
        .apply("mymap", Map(basic_map))
        .apply("serializer", Serializer())
        .sink("kafkasink", StreamSink(stream_name="transformed-events"))
    )

    return pipeline


@pytest.fixture
def reduce_pipeline(transformer: Callable[[], TestTransformerBatch]) -> Pipeline:
    reduce_window = SlidingWindow(
        window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2)
    )

    pipeline = (
        streaming_source("myinput", stream_name="ingest-metrics")
        .apply("decoder", Parser(msg_type=IngestMetric))
        .apply("mymap", Map(basic_map))
        .apply("myreduce", Reducer(reduce_window, transformer))
        .apply("serializer", Serializer())
        .sink("kafkasink", StreamSink(stream_name="transformed-events"))
    )

    return pipeline


@pytest.fixture
def router_pipeline() -> Pipeline:
    branch_1 = (
        segment("set_branch", IngestMetric)
        .apply("serializer", Serializer())
        .sink("kafkasink1", StreamSink(stream_name="transformed-events"))
    )
    branch_2 = (
        segment("not_set_branch", IngestMetric)
        .apply("serializer2", Serializer())
        .sink("kafkasink2", StreamSink(stream_name="transformed-events-2"))
    )

    pipeline = (
        streaming_source(
            name="ingest",
            stream_name="ingest-metrics",
        )
        .apply("decoder", Parser(msg_type=IngestMetric))
        .route(
            "router",
            routing_function=lambda msg: "set" if msg.payload["type"] == "s" else "not_set",
            routes={
                "set": branch_1,
                "not_set": branch_2,
            },
        )
    )

    return pipeline


@pytest.fixture
def broadcast_pipeline() -> Pipeline:
    branch_1 = (
        segment("even_branch", IngestMetric)
        .apply("mymap1", Map(basic_map))
        .apply("serializer", Serializer())
        .sink("kafkasink1", StreamSink(stream_name="transformed-events"))
    )
    branch_2 = (
        segment("odd_branch", IngestMetric)
        .apply("mymap2", Map(basic_map))
        .apply("serializer2", Serializer())
        .sink("kafkasink2", StreamSink(stream_name="transformed-events-2"))
    )

    pipeline = (
        streaming_source(
            name="ingest",
            stream_name="ingest-metrics",
        )
        .apply("decoder", Parser(msg_type=IngestMetric))
        .broadcast(
            "broadcast",
            routes=[
                branch_1,
                branch_2,
            ],
        )
    )

    return pipeline
