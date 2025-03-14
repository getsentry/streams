import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.pipeline.pipeline import (
    Filter,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
)


@pytest.fixture
def broker() -> LocalBroker[KafkaPayload]:
    storage: MemoryMessageStorage[KafkaPayload] = MemoryMessageStorage()
    broker = LocalBroker(storage, MockedClock())
    broker.create_topic(Topic("logical-events"), 1)
    broker.create_topic(Topic("transformed-events"), 1)
    return broker


@pytest.fixture
def pipeline() -> Pipeline:
    pipeline = Pipeline()
    source = KafkaSource(
        name="myinput",
        ctx=pipeline,
        logical_topic="logical-events",
    )
    decoder = Map(
        name="decoder",
        ctx=pipeline,
        inputs=[source],
        function=lambda msg: msg.decode("utf-8"),
    )
    filter = Filter(
        name="myfilter", ctx=pipeline, inputs=[decoder], function=lambda msg: msg == "go_ahead"
    )
    map = Map(
        name="mymap",
        ctx=pipeline,
        inputs=[filter],
        function=lambda msg: msg + "_mapped",
    )
    _ = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        logical_topic="transformed-events",
    )

    return pipeline
