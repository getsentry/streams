import json
from typing import cast
from unittest import mock

import pytest
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import Partition, Topic
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.adapters.arroyo.adapter import (
    ArroyoAdapter,
    StreamSources,
)
from sentry_streams.adapters.stream_adapter import (
    RuntimeState,
    RuntimeTranslator,
)
from sentry_streams.config_types import KafkaConsumerConfig
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    StreamSource,
)
from sentry_streams.runner import iterate_edges


def test_kafka_sources() -> None:
    sources_config = {
        "source1": KafkaConsumerConfig(
            starts_segment=None,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_group="test_group",
            override_params={},
        ),
    }
    consumers = {
        "source2": mock.Mock(),
    }
    sources = StreamSources(
        steps_config=sources_config,
        sources_override=consumers,
    )

    with pytest.raises(KeyError):
        sources.get_consumer("source1")

    assert sources.get_consumer("source2") == consumers["source2"]
    with pytest.raises(KeyError):
        sources.get_topic("source2")

    sources.add_source(StreamSource("source1", "test_topic"))

    assert sources.get_topic("source1") == Topic("test_topic")
    assert sources.get_consumer("source1") is not None


def test_adapter(
    broker: LocalBroker[KafkaPayload],
    pipeline: Pipeline[bytes],
    metric: IngestMetric,
    transformed_metric: IngestMetric,
) -> None:
    adapter = ArroyoAdapter.build(
        {
            "env": {},
            "steps_config": {
                "myinput": {"myinput": {}},
                "kafkasink": {"kafkasink": {}},
            },
        },
        {"myinput": cast(KafkaConsumer, broker.get_consumer("ingest-metrics"))},
        {"kafkasink": cast(KafkaProducer, broker.get_producer())},
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    adapter.create_processors()
    processor = adapter.get_processor("myinput")

    broker.produce(
        Partition(Topic("ingest-metrics"), 0),
        KafkaPayload(None, json.dumps(metric).encode("utf-8"), []),
    )
    broker.produce(
        Partition(Topic("ingest-metrics"), 0),
        KafkaPayload(None, json.dumps(metric).encode("utf-8"), []),
    )
    metric["type"] = "c"
    broker.produce(
        Partition(Topic("ingest-metrics"), 0),
        KafkaPayload(None, json.dumps(metric).encode("utf-8"), []),
    )

    processor._run_once()
    processor._run_once()
    processor._run_once()

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)

    assert msg1 is not None and msg1.payload.value == json.dumps(transformed_metric).encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == json.dumps(transformed_metric).encode("utf-8")
    assert broker.consume(Partition(topic, 0), 2) is None


def _adapter_with_stub_processor(processor: mock.Mock) -> ArroyoAdapter:
    adapter = ArroyoAdapter({})
    setattr(adapter, "_ArroyoAdapter__consumers", {"source": mock.Mock()})
    setattr(
        adapter,
        "create_processors",
        lambda: setattr(adapter, "_ArroyoAdapter__processors", {"source": processor}),
    )
    return adapter


def test_shutdown_while_building_the_processor_is_applied_to_it() -> None:
    processor = mock.Mock()
    adapter = _adapter_with_stub_processor(processor)
    processor.run.side_effect = lambda: (
        adapter.status.state is RuntimeState.STOPPING
        or pytest.fail(f"unexpected status: {adapter.status.state}")
    )

    def shutdown_while_building() -> None:
        adapter.shutdown()
        setattr(adapter, "_ArroyoAdapter__processors", {"source": processor})

    setattr(adapter, "create_processors", shutdown_while_building)
    adapter.begin_start()
    adapter.run()

    processor.signal_shutdown.assert_called_once_with()
    processor.run.assert_called_once_with()
    assert adapter.status.state is RuntimeState.STOPPED


def test_shutdown_before_start_never_builds_a_processor() -> None:
    processor = mock.Mock()
    adapter = _adapter_with_stub_processor(processor)

    adapter.shutdown()
    assert adapter.status.state is RuntimeState.STOPPED

    with pytest.raises(RuntimeError, match="cannot run runtime while it is stopped"):
        adapter.run()

    processor.run.assert_not_called()
    assert adapter.status.state is RuntimeState.STOPPED


def test_shutdown_during_startup_never_builds_a_processor() -> None:
    processor = mock.Mock()
    adapter = _adapter_with_stub_processor(processor)

    adapter.begin_start()
    adapter.shutdown()
    adapter.shutdown()
    adapter.run()

    processor.run.assert_not_called()
    assert adapter.status.state is RuntimeState.STOPPED
