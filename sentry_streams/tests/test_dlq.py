from typing import Mapping, Sequence

import pytest

from sentry_streams.adapters.arroyo.rust_arroyo import build_dlq_config
from sentry_streams.pipeline.pipeline import StreamSource
from sentry_streams.rust_streams import (
    ArroyoConsumer,
    DlqConfig,
    InitialOffset,
    PyKafkaConsumerConfig,
    PyKafkaProducerConfig,
)


@pytest.mark.parametrize(
    "input_dlqconfig, expected_topic, expected_bootstrap_servers",
    [
        pytest.param(None, None, None, id="without_dlq"),
        pytest.param(
            DlqConfig(
                topic="test-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["localhost:9092"],
                    override_params=None,
                ),
            ),
            "test-dlq",
            ["localhost:9092"],
            id="with_full_config",
        ),
    ],
)
def test_consumer_creation(
    input_dlqconfig: DlqConfig | None,
    expected_topic: str | None,
    expected_bootstrap_servers: Sequence[str] | None,
) -> None:
    kafka_consumer_config = PyKafkaConsumerConfig(
        bootstrap_servers=["localhost:9092"],
        group_id="test-group",
        auto_offset_reset=InitialOffset.latest,
        strict_offset_reset=False,
        max_poll_interval_ms=60000,
        override_params=None,
    )

    consumer = ArroyoConsumer(
        source="test_source",
        kafka_config=kafka_consumer_config,
        topic="test-topic",
        schema=None,
        metric_config=None,
        write_healthcheck=False,
        dlq_config=input_dlqconfig,
    )

    if input_dlqconfig is None:
        assert consumer.dlq_config is None
    else:
        assert consumer.dlq_config is not None
        assert consumer.dlq_config.topic == expected_topic
        assert consumer.dlq_config.producer_config.bootstrap_servers == expected_bootstrap_servers


def test_stream_source_dlq_config_not_constructor_param() -> None:
    """Test that dlq_config cannot be passed as a constructor argument."""
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        StreamSource(  # type: ignore
            name="test_source",
            stream_name="test-topic",
            dlq_config="anything",
        )


def test_stream_source_no_dlq() -> None:
    """Test StreamSource without DLQ."""
    source = StreamSource(
        name="test_source",
        stream_name="test-topic",
    )
    assert source.dlq_stream_name is None


@pytest.mark.parametrize(
    "dlq_stream_name, override_dlq, expected_topic, expected_bootstrap_servers, expected_override_params",
    [
        pytest.param(
            "my-dlq",
            {
                "bootstrap_servers": ["broker1:9092"],
            },
            "my-dlq",
            ["broker1:9092"],
            None,
            id="default_topic_from_stream_name",
        ),
        pytest.param(
            "my-dlq",
            {
                "topic": "overridden-dlq-topic",
                "bootstrap_servers": ["broker1:9092"],
            },
            "overridden-dlq-topic",
            ["broker1:9092"],
            None,
            id="topic_override_from_config",
        ),
        pytest.param(
            "my-dlq",
            {
                "bootstrap_servers": ["broker1:9092", "broker2:9092"],
                "override_params": {
                    "security.protocol": "sasl_plaintext",
                    "sasl.mechanism": "SCRAM-SHA-256",
                    "sasl.username": "user",
                    "sasl.password": "pass",
                },
            },
            "my-dlq",
            ["broker1:9092", "broker2:9092"],
            {
                "security.protocol": "sasl_plaintext",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
            id="with_override_params",
        ),
    ],
)
def test_build_dlq_config(
    dlq_stream_name: str,
    override_dlq: Mapping[str, object],
    expected_topic: str,
    expected_bootstrap_servers: Sequence[str],
    expected_override_params: Mapping[str, str] | None,
) -> None:
    """Test that build_dlq_config constructs DlqConfig from deployment config."""
    result = build_dlq_config(dlq_stream_name, {"dlq": override_dlq})

    assert result is not None
    assert result.topic == expected_topic
    assert result.producer_config.bootstrap_servers == expected_bootstrap_servers
    assert result.producer_config.override_params == expected_override_params


def test_build_dlq_config_missing_bootstrap_servers() -> None:
    """Test that build_dlq_config raises ValueError when bootstrap_servers is missing."""
    with pytest.raises(
        ValueError,
        match="DLQ config requires 'bootstrap_servers' in deployment configuration",
    ):
        build_dlq_config("my-dlq", {"dlq": {"topic": "my-dlq"}})


def test_build_dlq_config_no_dlq_section() -> None:
    """Test that build_dlq_config returns None when no dlq section in config."""
    assert build_dlq_config("my-dlq", {}) is None
