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
from sentry_streams.adapters.arroyo.steps import (
    BroadcastStep,
    FilterStep,
    MapStep,
    RouterStep,
    StreamSinkStep,
)
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    Map,
    Pipeline,
    Router,
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
        StreamSinkStep(
            route=Route(source="source1", waypoints=[]),
            producer=broker.get_producer(),
            topic_name="transformed-events",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("events"), 0): 0})

    strategy.submit(make_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_msg("go_ahead", "events", 3))
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
            call({Partition(Topic("events"), 0): 1}),
            call({}),
            call({Partition(Topic("events"), 0): 3}),
            call({}),
            call({}),
            call({}),
            call({Partition(Topic("events"), 0): 4}),
            call({}),
        ],
    )


def test_broadcast(broker: LocalBroker[KafkaPayload], broadcast_pipeline: Pipeline) -> None:
    """
    Test the creation of an Arroyo Consumer from pipeline steps which
    contain a Broadcast.
    """

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, broadcast_pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        BroadcastStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Broadcast, broadcast_pipeline.steps["broadcast"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=["even_branch"]),
            pipeline_step=cast(Map, broadcast_pipeline.steps["mymap1"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=["odd_branch"]),
            pipeline_step=cast(Map, broadcast_pipeline.steps["mymap2"]),
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["even_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events",
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["odd_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events-2",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("events"), 0): 0})

    strategy.submit(make_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_msg("go_ahead", "events", 3))
    strategy.poll()

    topics = [Topic("transformed-events"), Topic("transformed-events-2")]

    for topic in topics:
        msg1 = broker.consume(Partition(topic, 0), 0)
        assert msg1 is not None and msg1.payload.value == "go_ahead_mapped".encode("utf-8")
        msg2 = broker.consume(Partition(topic, 0), 1)
        assert msg2 is not None and msg2.payload.value == "do_not_go_ahead_mapped".encode("utf-8")
        msg3 = broker.consume(Partition(topic, 0), 2)
        assert msg3 is not None and msg3.payload.value == "go_ahead_mapped".encode("utf-8")


def test_multiple_routes(broker: LocalBroker[KafkaPayload], router_pipeline: Pipeline) -> None:
    """
    Test the creation of an Arroyo Consumer from pipeline steps which
    contain branching routes.
    """

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, router_pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        RouterStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Router[str], router_pipeline.steps["router"]),
        )
    )
    consumer.add_step(
        FilterStep(
            route=Route(source="source1", waypoints=["even_branch"]),
            pipeline_step=cast(Filter, router_pipeline.steps["myfilter"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=["odd_branch"]),
            pipeline_step=cast(Map, router_pipeline.steps["mymap"]),
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["even_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events",
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["odd_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events-2",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("events"), 0): 0})

    strategy.submit(make_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_msg("go_ahead", "events", 3))
    strategy.poll()

    topic = Topic("transformed-events")
    topic2 = Topic("transformed-events-2")

    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == "go_ahead".encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == "go_ahead".encode("utf-8")
    msg3 = broker.consume(Partition(topic2, 0), 0)
    assert msg3 is not None and msg3.payload.value == "do_not_go_ahead_mapped".encode("utf-8")

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(topic=Topic(name="events"), index=0): 1}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="events"), index=0): 3}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="events"), index=0): 4}),
            call({}),
            call({}),
        ],
    )
