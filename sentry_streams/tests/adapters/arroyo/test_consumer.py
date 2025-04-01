import time
from datetime import datetime, timedelta
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
    FilterStep,
    MapStep,
    ReduceStep,
    StreamSinkStep,
)
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Pipeline,
    Reduce,
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
            strategy.submit(make_msg(msg, "logical-events", i))

    # Last submit was at T+10, which means we've only flushed the first 3 windows

    # Poll 3 times now for the remaining 3 windows to flush
    # This time, there are no more submit() calls for making progress
    for i in range(6, 9):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.poll()

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

    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is not None and msg4.payload.value == "msg3_mappedmsg4_mappedmsg5_mapped".encode(
        "utf-8"
    )

    msg5 = broker.consume(Partition(topic, 0), 4)
    assert msg5 is not None and msg5.payload.value == "msg4_mappedmsg5_mapped".encode("utf-8")

    msg6 = broker.consume(Partition(topic, 0), 5)
    assert msg6 is not None and msg6.payload.value == "msg5_mapped".encode("utf-8")

    # Up to this point everything is flushed out
    # Submit data at T+24, T+26
    for i in range(12, 14):
        msg = f"msg{i}"
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_msg(msg, "logical-events", i))

    msg12 = broker.consume(Partition(topic, 0), 6)
    assert msg12 is not None and msg12.payload.value == "msg12_mapped".encode("utf-8")

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is None

    with mock.patch("time.time", return_value=cur_time + 2 * 14):
        strategy.poll()

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is not None and msg13.payload.value == "msg12_mappedmsg13_mapped".encode("utf-8")

    # Commit strategy is this: Commit the largest offset that contributes to a window that will be flushed
    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("logical-events"), 0): 3}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 4}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 5}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 13}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 14}),
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
            strategy.submit(make_msg(msg, "logical-events", i))

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
            call({Partition(Topic("logical-events"), 0): 3}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 4}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 5}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
            call({}),
            call({Partition(Topic("logical-events"), 0): 6}),
        ]
    )
