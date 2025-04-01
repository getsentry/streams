import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.pipeline.chain import Filter, Map, segment, streaming_source
from sentry_streams.pipeline.pipeline import (
    Pipeline,
)


def decode(msg: bytes) -> str:
    return msg.decode("utf-8")


def basic_map(msg: str) -> str:
    return msg + "_mapped"


@pytest.fixture
def broker() -> LocalBroker[KafkaPayload]:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, MockedClock())
    broker.create_topic(Topic("events"), 1)
    broker.create_topic(Topic("transformed-events"), 1)
    broker.create_topic(Topic("transformed-events-2"), 1)
    return broker


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = (
        streaming_source("myinput", stream_name="events")
        .apply("decoder", Map(decode))
        .apply("myfilter", Filter(lambda msg: msg == "go_ahead"))
        .apply("mymap", Map(basic_map))
        .sink("kafkasink", stream_name="transformed-events")
    )

    return pipeline


@pytest.fixture
def router_pipeline() -> Pipeline:
    branch_1 = (
        segment("even_branch")
        .apply("myfilter", Filter(lambda msg: msg == "go_ahead"))
        .sink("kafkasink1", stream_name="transformed-events")
    )
    branch_2 = (
        segment("odd_branch")
        .apply("mymap", Map(basic_map))
        .sink("kafkasink2", stream_name="transformed-events-2")
    )

    pipeline = (
        streaming_source(
            name="ingest",
            stream_name="events",
        )
        .apply("decoder", Map(decode))
        .route(
            "router",
            routing_function=lambda msg: "even" if len(msg) % 2 == 0 else "odd",
            routes={
                "even": branch_1,
                "odd": branch_2,
            },
        )
    )

    return pipeline


@pytest.fixture
def broadcast_pipeline() -> Pipeline:
    branch_1 = (
        segment("even_branch")
        .apply("mymap1", Map(basic_map))
        .sink("kafkasink1", stream_name="transformed-events")
    )
    branch_2 = (
        segment("odd_branch")
        .apply("mymap2", Map(basic_map))
        .sink("kafkasink2", stream_name="transformed-events-2")
    )

    pipeline = (
        streaming_source(
            name="ingest",
            stream_name="events",
        )
        .apply("decoder", Map(decode))
        .broadcast(
            "broadcast",
            routes=[
                branch_1,
                branch_2,
            ],
        )
    )

    return pipeline
