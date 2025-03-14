from datetime import datetime
from typing import Any, cast
from unittest import mock
from unittest.mock import call

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import BrokerValue, Commit, Message, Partition, Topic

from sentry_streams.adapters.arroyo.consumer import (
    ArroyoConsumer,
    ArroyoStreamingFactory,
)
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import FilterStep, KafkaSinkStep, MapStep
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Pipeline,
)


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


def test_single_route(broker: LocalBroker[KafkaPayload], pipeline: Pipeline) -> None:
    """
    Test the creation of an Arroyo Consumer from a number of
    pipeline steps.
    """

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        FilterStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Filter, pipeline.steps["myfilter"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, pipeline.steps["mymap"]),
        )
    )
    consumer.add_step(
        KafkaSinkStep(
            route=Route(source="source1", waypoints=[]),
            producer=broker.get_producer(),
            topic_name="transformed-events",
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
