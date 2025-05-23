import json
import time
from copy import deepcopy
from datetime import timedelta
from typing import Any, cast
from unittest import mock
from unittest.mock import call

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import Commit, Partition, Topic
from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

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

SCHEMA = get_codec("ingest-metrics")


def test_single_route(
    broker: LocalBroker[KafkaPayload],
    pipeline: Pipeline,
    metric: IngestMetric,
    transformed_metric: IngestMetric,
) -> None:
    """
    Test the creation of an Arroyo Consumer from a number of
    pipeline steps.
    """
    empty_route = Route(source="source1", waypoints=[])

    consumer = ArroyoConsumer(
        source="source1", stream_name="ingest-metrics", schema="ingest-metrics"
    )
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
        MapStep(
            route=empty_route,
            pipeline_step=cast(Map, pipeline.steps["serializer"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("ingest-metrics"), 0): 0})

    counter_metric = deepcopy(metric)
    counter_metric["type"] = "c"

    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(counter_metric), "ingest-metrics", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 3))
    strategy.poll()

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == json.dumps(transformed_metric).encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == json.dumps(transformed_metric).encode("utf-8")
    assert broker.consume(Partition(topic, 0), 2) is None

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 1}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 3}),
            call({}),
            call({}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 4}),
            call({}),
        ],
    )


def test_broadcast(
    broker: LocalBroker[KafkaPayload],
    broadcast_pipeline: Pipeline,
    metric: IngestMetric,
    transformed_metric: IngestMetric,
) -> None:
    """
    Test the creation of an Arroyo Consumer from pipeline steps which
    contain a Broadcast.
    """

    consumer = ArroyoConsumer(
        source="source1", stream_name="ingest-metrics", schema="ingest-metrics"
    )
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
        MapStep(
            route=Route(source="source1", waypoints=["even_branch"]),
            pipeline_step=cast(Map, broadcast_pipeline.steps["serializer"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=["odd_branch"]),
            pipeline_step=cast(Map, broadcast_pipeline.steps["serializer2"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("ingest-metrics"), 0): 0})

    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 3))
    strategy.poll()

    topics = [Topic("transformed-events"), Topic("transformed-events-2")]

    for topic in topics:
        msg1 = broker.consume(Partition(topic, 0), 0)
        assert msg1 is not None and msg1.payload.value == json.dumps(transformed_metric).encode(
            "utf-8"
        )
        msg2 = broker.consume(Partition(topic, 0), 1)
        assert msg2 is not None and msg2.payload.value == json.dumps(transformed_metric).encode(
            "utf-8"
        )
        msg3 = broker.consume(Partition(topic, 0), 2)
        assert msg3 is not None and msg3.payload.value == json.dumps(transformed_metric).encode(
            "utf-8"
        )


def test_multiple_routes(
    broker: LocalBroker[KafkaPayload], router_pipeline: Pipeline, metric: IngestMetric
) -> None:
    """
    Test the creation of an Arroyo Consumer from pipeline steps which
    contain branching routes.
    """

    consumer = ArroyoConsumer(
        source="source1", stream_name="ingest-metrics", schema="ingest-metrics"
    )
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
        MapStep(
            route=Route(source="source1", waypoints=["set_branch"]),
            pipeline_step=cast(Map, router_pipeline.steps["serializer"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=Route(source="source1", waypoints=["not_set_branch"]),
            pipeline_step=cast(Map, router_pipeline.steps["serializer2"]),
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["set_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events",
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=Route(source="source1", waypoints=["not_set_branch"]),
            producer=broker.get_producer(),
            topic_name="transformed-events-2",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("ingest-metrics"), 0): 0})

    counter_metric = deepcopy(metric)
    counter_metric["type"] = "c"

    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(counter_metric), "ingest-metrics", 2))
    strategy.poll()
    strategy.submit(make_kafka_msg(json.dumps(metric), "ingest-metrics", 3))
    strategy.poll()

    topic = Topic("transformed-events")  # for set messages
    topic2 = Topic("transformed-events-2")  # for non-set messages

    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == json.dumps(metric).encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == json.dumps(metric).encode("utf-8")
    msg3 = broker.consume(Partition(topic2, 0), 0)
    assert msg3 is not None and msg3.payload.value == json.dumps(counter_metric).encode("utf-8")

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 1}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 3}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 4}),
            call({}),
            call({}),
        ],
    )


def test_standard_reduce(
    broker: LocalBroker[KafkaPayload],
    reduce_pipeline: Pipeline,
    metric: IngestMetric,
    transformed_metric: IngestMetric,
) -> None:
    """
    Test a full "loop" of the sliding window algorithm. Checks for correct results, timestamps,
    and offset management strategy
    """

    consumer = ArroyoConsumer(
        source="source1", stream_name="ingest-metrics", schema="ingest-metrics"
    )
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
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["serializer"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("ingest-metrics"), 0): 0})

    cur_time = time.time()

    # 6 messages
    messages = []
    for i in range(6):
        modified_metric = deepcopy(metric)
        modified_metric["org_id"] = i
        messages.append(modified_metric)

    # Accumulators: [0,1] [2,3] [4,5] [6,7] [8,9]
    for i in range(6):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(json.dumps(messages[i]), "ingest-metrics", i))

    # Last submit was at T+10, which means we've only flushed the first 3 windows

    transformed_msgs = []
    for i in range(6):
        modified_metric = deepcopy(transformed_metric)
        modified_metric["org_id"] = i
        transformed_msgs.append(modified_metric)

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)

    assert msg1 is not None and msg1.payload.value == json.dumps(transformed_msgs[:3]).encode(
        "utf-8"
    )

    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == json.dumps(transformed_msgs[1:4]).encode(
        "utf-8"
    )

    msg3 = broker.consume(Partition(topic, 0), 2)
    assert msg3 is not None and msg3.payload.value == json.dumps(transformed_msgs[2:5]).encode(
        "utf-8"
    )

    # Poll 3 times now for the remaining 3 windows to flush
    # This time, there are no more submit() calls for making progress
    for i in range(6, 9):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.poll()

    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is not None and msg4.payload.value == json.dumps(transformed_msgs[3:6]).encode(
        "utf-8"
    )

    msg5 = broker.consume(Partition(topic, 0), 4)
    assert msg5 is not None and msg5.payload.value == json.dumps(transformed_msgs[4:6]).encode(
        "utf-8"
    )

    msg6 = broker.consume(Partition(topic, 0), 5)
    assert msg6 is not None and msg6.payload.value == json.dumps([transformed_msgs[5]]).encode(
        "utf-8"
    )

    # Up to this point everything is flushed out
    messages = []
    for i in range(12, 14):
        modified_metric = deepcopy(metric)
        modified_metric["org_id"] = i
        messages.append(modified_metric)

    # Submit data at T+24, T+26 (data comes in at a gap)
    for i in range(12, 14):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(json.dumps(messages[i - 12]), "ingest-metrics", i))

    transformed_msgs = []
    for i in range(12, 14):
        modified_metric = deepcopy(transformed_metric)
        modified_metric["org_id"] = i
        transformed_msgs.append(modified_metric)

    msg12 = broker.consume(Partition(topic, 0), 6)
    assert msg12 is not None and msg12.payload.value == json.dumps([transformed_msgs[0]]).encode(
        "utf-8"
    )

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is None

    with mock.patch("time.time", return_value=cur_time + 2 * 14):
        strategy.poll()

    msg13 = broker.consume(Partition(topic, 0), 7)
    assert msg13 is not None and msg13.payload.value == json.dumps(transformed_msgs[:2]).encode(
        "utf-8"
    )

    msg14 = broker.consume(Partition(topic, 0), 8)
    assert msg14 is None

    with mock.patch("time.time", return_value=cur_time + 2 * 15):
        strategy.poll()

    msg14 = broker.consume(Partition(topic, 0), 8)
    assert msg14 is not None and msg14.payload.value == json.dumps(transformed_msgs[:2]).encode(
        "utf-8"
    )

    with mock.patch("time.time", return_value=cur_time + 2 * 16):
        strategy.poll()

    msg15 = broker.consume(Partition(topic, 0), 9)
    assert msg15 is not None and msg15.payload.value == json.dumps([transformed_msgs[1]]).encode(
        "utf-8"
    )

    with mock.patch("time.time", return_value=cur_time + 2 * 17):
        strategy.poll()

    msg16 = broker.consume(Partition(topic, 0), 10)
    assert msg16 is None

    # Commit strategy is this: Commit the largest offset that contributes to a window that will be flushed
    commit.assert_has_calls(
        [
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 1}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 2}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 3}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 4}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 5}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 6}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 13}),
            call({}),
            call({}),
            call({Partition(topic=Topic(name="ingest-metrics"), index=0): 14}),
            call({}),
            call({}),
            call({}),
        ]
    )


def test_reduce_with_gap(
    broker: LocalBroker[KafkaPayload],
    reduce_pipeline: Pipeline,
    metric: IngestMetric,
    transformed_metric: IngestMetric,
) -> None:
    """
    Test a full "loop" of the sliding window algorithm. Checks for correct results, timestamps,
    and offset management strategy
    """

    consumer = ArroyoConsumer(
        source="source1", stream_name="ingest-metrics", schema="ingest-metrics"
    )
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
        MapStep(
            route=Route(source="source1", waypoints=[]),
            pipeline_step=cast(Map, reduce_pipeline.steps["serializer"]),
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
    strategy = factory.create_with_partitions(commit, {Partition(Topic("ingest-metrics"), 0): 0})

    cur_time = time.time()

    # 6 messages to use in this test
    # Give them an "ID" so we can test for correctness in the algorithm
    messages = []
    for i in range(6):
        modified_metric = deepcopy(metric)
        modified_metric["org_id"] = i
        messages.append(modified_metric)

    # Accumulators: [0,1] [2,3] [4,5] [6,7] [8,9]
    for i in range(6):
        with mock.patch("time.time", return_value=cur_time + 2 * i):
            strategy.submit(make_kafka_msg(json.dumps(messages[i]), "ingest-metrics", i))

    # Last submit was at T+10, which means we've only flushed the first 3 windows

    transformed_msgs = []
    for i in range(6):
        modified_metric = deepcopy(transformed_metric)
        modified_metric["org_id"] = i
        transformed_msgs.append(modified_metric)

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == json.dumps(transformed_msgs[:3]).encode(
        "utf-8"
    )

    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == json.dumps(transformed_msgs[1:4]).encode(
        "utf-8"
    )

    msg3 = broker.consume(Partition(topic, 0), 2)
    assert msg3 is not None and msg3.payload.value == json.dumps(transformed_msgs[2:5]).encode(
        "utf-8"
    )

    # We did not make it past the first 3 windows yet
    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is None

    # A single poll call which comes after a large gap
    with mock.patch("time.time", return_value=cur_time + 50):
        strategy.poll()

    msg4 = broker.consume(Partition(topic, 0), 3)
    assert msg4 is not None and msg4.payload.value == json.dumps(transformed_msgs[3:6]).encode(
        "utf-8"
    )

    msg5 = broker.consume(Partition(topic, 0), 4)
    assert msg5 is not None and msg5.payload.value == json.dumps(transformed_msgs[4:6]).encode(
        "utf-8"
    )

    msg6 = broker.consume(Partition(topic, 0), 5)
    assert msg6 is not None and msg6.payload.value == json.dumps([transformed_msgs[5]]).encode(
        "utf-8"
    )

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 1}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 2}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 3}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 4}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 5}),
            call({}),
            call({Partition(Topic("ingest-metrics"), 0): 6}),
        ]
    )
