import time
from datetime import timedelta
from typing import Any, cast
from unittest import mock
from unittest.mock import call

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import Commit, Partition, Topic

from sentry_streams.adapters.arroyo.consumer import (
    ArroyoConsumer,
    ArroyoStreamingFactory,
)
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import (
    BroadcastStep,
    FilterStep,
    MapStep,
    ReduceStep,
    RouterStep,
    StreamSinkStep,
)
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    Map,
    Pipeline,
    Reduce,
    Router,
)
from tests.adapters.arroyo.message_helpers import make_kafka_msg


def test_single_route(broker: LocalBroker[KafkaPayload], pipeline: Pipeline) -> None:
    """
    Test the creation of an Arroyo Consumer from a number of
    pipeline steps.
    """
    empty_route = Route(source="source1", waypoints=[])

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=empty_route,
            pipeline_step=cast(Map, pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        FilterStep(
            route=empty_route,
            pipeline_step=cast(Filter, pipeline.steps["myfilter"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=empty_route,
            pipeline_step=cast(Map, pipeline.steps["mymap"]),
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=empty_route,
            producer=broker.get_producer(),
            topic_name="transformed-events",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("events"), 0): 0})

    strategy.submit(make_kafka_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg("go_ahead", "events", 3))
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

    strategy.submit(make_kafka_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg("go_ahead", "events", 3))
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

    strategy.submit(make_kafka_msg("go_ahead", "events", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg("do_not_go_ahead", "events", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg("go_ahead", "events", 3))
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


def test_standard_reduce(broker: LocalBroker[KafkaPayload], reduce_pipeline: Pipeline) -> None:
    """
    Test a full "loop" of the sliding window algorithm. Checks for correct results, timestamps,
    and offset management strategy
    """

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["mymap"]),
        )
    )
    consumer.add_step(
        ReduceStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Reduce[timedelta, Any, Any], reduce_pipeline.steps["myreduce"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("logical-events"), 0): 0})

    cur_time = time.time()

    # 6 messages
    # Accumulators: [0,1] [2,3] [4,5] [6,7] [8,9]
    for i in range(6):
        msg = f"msg{i}"
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(msg, "logical-events", i))

    # Last submit was at T+10, which means we've only flushed the first 3 windows
    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == "msg0_mappedmsg1_mappedmsg2_mapped".encode(
        "utf-8"
    )

    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == "msg1_mappedmsg2_mappedmsg3_mapped".encode(
        "utf-8"
    )

    msg3 = broker.consume(Partition(topic, 0), 2)
    assert msg3 is not None and msg3.payload.value == "msg2_mappedmsg3_mappedmsg4_mapped".encode(
        "utf-8"
    )

    # Poll 3 times now for the remaining 3 windows to flush
    # This time, there are no more submit() calls for making progress
    for i in range(6, 9):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.poll()

    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is not None and msg4.payload.value == "msg3_mappedmsg4_mappedmsg5_mapped".encode(
        "utf-8"
    )

    msg5 = broker.consume(Partition(topic, 0), 4)
    assert msg5 is not None and msg5.payload.value == "msg4_mappedmsg5_mapped".encode("utf-8")

    msg6 = broker.consume(Partition(topic, 0), 5)
    assert msg6 is not None and msg6.payload.value == "msg5_mapped".encode("utf-8")

    # Up to this point everything is flushed out

    # Submit data at T+24, T+26 (data comes in at a gap)
    for i in range(12, 14):
        msg = f"msg{i}"
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(msg, "logical-events", i))

    msg12 = broker.consume(Partition(topic, 0), 6)
    assert msg12 is not None and msg12.payload.value == "msg12_mapped".encode("utf-8")

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is None

    with mock.patch("time.time", return_value=cur_time + 2 * 14):
        strategy.poll()

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is not None and msg13.payload.value == "msg12_mappedmsg13_mapped".encode("utf-8")

    msg14 = broker.consume(Partition(topic, 0), 8)
    assert msg14 is None

    with mock.patch("time.time", return_value=cur_time + 2 * 15):
        strategy.poll()

    msg14 = broker.consume(Partition(topic, 0), 8)
    assert msg14 is not None and msg14.payload.value == "msg12_mappedmsg13_mapped".encode("utf-8")

    with mock.patch("time.time", return_value=cur_time + 2 * 16):
        strategy.poll()

    msg15 = broker.consume(Partition(topic, 0), 9)
    assert msg15 is not None and msg15.payload.value == "msg13_mapped".encode("utf-8")

    with mock.patch("time.time", return_value=cur_time + 2 * 17):
        strategy.poll()

    msg16 = broker.consume(Partition(topic, 0), 10)
    assert msg16 is None

    # Commit strategy is this: Commit the largest offset that contributes to a window that will be flushed
    commit.assert_has_calls(
        [
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 1}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 2}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 3}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 4}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 5}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 6}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 13}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="logical-events"), index=0): 14}),
            call({}),
            call({}),
            call({}),
        ]
    )


def test_reduce_with_gap(broker: LocalBroker[KafkaPayload], reduce_pipeline: Pipeline) -> None:
    """
    Test a full "loop" of the sliding window algorithm. Checks for correct results, timestamps,
    and offset management strategy
    """

    consumer = ArroyoConsumer(source="source1")
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["decoder"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["mymap"]),
        )
    )
    consumer.add_step(
        ReduceStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Reduce[timedelta, Any, Any], reduce_pipeline.steps["myreduce"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("logical-events"), 0): 0})

    cur_time = time.time()

    # 6 messages
    # Accumulators: [0,1] [2,3] [4,5] [6,7] [8,9]
    for i in range(6):
        msg = f"msg{i}"
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(msg, "logical-events", i))

    # Last submit was at T+10, which means we've only flushed the first 3 windows

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == "msg0_mappedmsg1_mappedmsg2_mapped".encode(
        "utf-8"
    )

    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == "msg1_mappedmsg2_mappedmsg3_mapped".encode(
        "utf-8"
    )

    msg3 = broker.consume(Partition(topic, 0), 2)
    assert msg3 is not None and msg3.payload.value == "msg2_mappedmsg3_mappedmsg4_mapped".encode(
        "utf-8"
    )

    # We did not make it past the first 3 windows yet
    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is None

    # A single poll call which comes after a large gap
    with mock.patch("time.time", return_value=cur_time + 50):
        strategy.poll()

    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is not None and msg4.payload.value == "msg3_mappedmsg4_mappedmsg5_mapped".encode(
        "utf-8"
    )

    msg5 = broker.consume(Partition(topic, 0), 4)
    assert msg5 is not None and msg5.payload.value == "msg4_mappedmsg5_mapped".encode("utf-8")

    msg6 = broker.consume(Partition(topic, 0), 5)
    assert msg6 is not None and msg6.payload.value == "msg5_mapped".encode("utf-8")

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("logical-events"), 0): 1}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 2}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 3}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 4}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 5}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
        ]
    )
