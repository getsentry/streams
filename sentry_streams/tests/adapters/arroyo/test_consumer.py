from datetime import datetime
from typing import Any
from unittest import mock
from unittest.mock import call

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.types import BrokerValue, Commit, Message, Partition, Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.adapters.arroyo.consumer import (
    ArroyoConsumer,
    ArroyoStreamingFactory,
)
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import FilterStep, KafkaSinkStep, MapStep
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


def make_msg(
    payload: str,
    topic: str,
    offset: int,
) -> Message[Any]:
    return Message(
        BrokerValue(
            payload=KafkaPayload(None, payload.encode("utf-8"), []),
            partition=Partition(Topic(topic), 0),
            offset=offset,
            timestamp=datetime.now(),
        )
    )


def test_single_route(broker: LocalBroker[KafkaPayload]) -> None:
    """
    Test the creation of an Arroyo Consumer from a number of
    pipeline steps.
    """
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
    sink = KafkaSink(
        name="kafkasink",
        ctx=pipeline,
        inputs=[map],
        logical_topic="transformed-events",
    )

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(MapStep(route=Route(source="source1", waypoints=[]), pipeline_step=decoder))
    consumer.add_step(FilterStep(route=Route(source="source1", waypoints=[]), pipeline_step=filter))
    consumer.add_step(MapStep(route=Route(source="source1", waypoints=[]), pipeline_step=map))
    consumer.add_step(
        KafkaSinkStep(
            route=Route(source="source1", waypoints=[]),
            producer=broker.get_producer(),
            topic_name=sink.logical_topic,
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("logical-events"), 0): 0})

    strategy.submit(make_msg("go_ahead", "logical-events", 0))
    strategy.submit(make_msg("do_not_go_ahead", "logical-events", 2))
    strategy.submit(make_msg("go_ahead", "logical-events", 3))
    strategy.poll()

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == "go_ahead_mapped".encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == "go_ahead_mapped".encode("utf-8")
    assert broker.consume(Partition(topic, 0), 2) is None

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("logical-events"), 0): 1}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 3}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 4}),
        ]
    )
