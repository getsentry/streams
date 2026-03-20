from typing import Mapping, Sequence

import pytest

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
    """Test that Rust ArroyoConsumer correctly accepts and stores DLQ configuration.

    This parameterized test verifies:
    1. Backward compatibility when DLQ param is omitted
    2. Full DLQ configuration is accepted and stored correctly
    """
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


@pytest.mark.parametrize(
    "dlq_config, expected_topic, expected_bootstrap_servers",
    [
        pytest.param(None, None, None, id="no_dlq_config"),
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
            id="single_bootstrap_server",
        ),
        pytest.param(
            DlqConfig(
                topic="my-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["broker1:9092", "broker2:9092", "broker3:9092"],
                    override_params=None,
                ),
            ),
            "my-dlq",
            ["broker1:9092", "broker2:9092", "broker3:9092"],
            id="multiple_bootstrap_servers",
        ),
    ],
)
def test_stream_source_dlq_config(
    dlq_config: DlqConfig | None,
    expected_topic: str | None,
    expected_bootstrap_servers: Sequence[str] | None,
) -> None:
    """Test StreamSource correctly stores DlqConfig."""
    source = StreamSource(
        name="test_source",
        stream_name="test-topic",
        dlq_config=dlq_config,
    )

    if expected_topic is None:
        assert source.dlq_config is None
    else:
        assert source.dlq_config is not None
        assert isinstance(source.dlq_config, DlqConfig)
        assert source.dlq_config.topic == expected_topic
        assert source.dlq_config.producer_config is not None
        assert source.dlq_config.producer_config.bootstrap_servers == expected_bootstrap_servers


@pytest.mark.parametrize(
    "initial_dlq_config, override_dlq, expected_topic, expected_bootstrap_servers, expected_override_params",
    [
        pytest.param(
            DlqConfig(
                topic="new-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["broker1:9092"],
                    override_params={
                        "security.protocol": "sasl_plaintext",
                        "sasl.mechanism": "SCRAM-SHA-256",
                        "sasl.username": "user",
                        "sasl.password": "pass",
                    },
                ),
            ),
            None,
            "new-dlq",
            ["broker1:9092"],
            {
                "security.protocol": "sasl_plaintext",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
            id="no_config_override",
        ),
        pytest.param(
            None,
            {
                "topic": "new-dlq",
                "producer_config": {
                    "bootstrap_servers": ["broker1:9092"],
                    "override_params": {
                        "security.protocol": "sasl_plaintext",
                        "sasl.mechanism": "SCRAM-SHA-256",
                        "sasl.username": "user",
                        "sasl.password": "pass",
                    },
                },
            },
            "new-dlq",
            ["broker1:9092"],
            {
                "security.protocol": "sasl_plaintext",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
            id="config_override_only",
        ),
        pytest.param(
            DlqConfig(
                topic="old-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["old-broker:9092"],
                    override_params=None,
                ),
            ),
            {"topic": "new-dlq"},
            "new-dlq",
            ["old-broker:9092"],
            None,
            id="override_topic_only",
        ),
        pytest.param(
            DlqConfig(
                topic="old-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["old-broker:9092"],
                    override_params=None,
                ),
            ),
            {"producer_config": {"bootstrap_servers": ["new-broker:9092", "new-broker2:9092"]}},
            "old-dlq",
            ["new-broker:9092", "new-broker2:9092"],
            None,
            id="override_bootstrap_servers_only",
        ),
        pytest.param(
            DlqConfig(
                topic="old-dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["old-broker:9092"],
                    override_params=None,
                ),
            ),
            {"topic": "new-dlq", "producer_config": {"bootstrap_servers": ["new-broker:9092"]}},
            "new-dlq",
            ["new-broker:9092"],
            None,
            id="override_topic_and_bootstrap_servers",
        ),
        pytest.param(
            DlqConfig(
                topic="dlq",
                producer_config=PyKafkaProducerConfig(
                    bootstrap_servers=["broker:9092"],
                    override_params=None,
                ),
            ),
            {
                "producer_config": {
                    "override_params": {
                        "security.protocol": "sasl_plaintext",
                        "sasl.mechanism": "SCRAM-SHA-256",
                        "sasl.username": "user",
                        "sasl.password": "pass",
                    }
                }
            },
            "dlq",
            ["broker:9092"],
            {
                "security.protocol": "sasl_plaintext",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
            id="override_override_params_only",
        ),
    ],
)
def test_stream_source_yaml_override_config_dlq(
    initial_dlq_config: DlqConfig | None,
    override_dlq: Mapping[str, str | Sequence[str]] | None,
    expected_topic: str,
    expected_bootstrap_servers: Sequence[str],
    expected_override_params: Mapping[str, str],
) -> None:
    """Test that StreamSource.override_config correctly overrides DLQ settings."""
    source = StreamSource(
        name="my_source",
        stream_name="my-topic",
        dlq_config=initial_dlq_config,
    )

    if override_dlq is not None:
        source.override_config({"dlq": override_dlq})

    assert source.dlq_config is not None
    assert source.dlq_config.topic == expected_topic
    assert source.dlq_config.producer_config.bootstrap_servers == expected_bootstrap_servers
    assert source.dlq_config.producer_config.override_params == expected_override_params


@pytest.mark.parametrize(
    "initial_dlq_config, override_dlq",
    [
        pytest.param(
            None,
            {"producer_config": {"bootstrap_servers": ["broker:9092"]}},
            id="no_topic_with_bootstrap_servers",
        ),
        pytest.param(
            None,
            {"topic": "my-dlq"},
            id="has_topic_no_bootstrap_servers",
        ),
    ],
)
def test_stream_source_override_config_dlq_missing_required_fields(
    initial_dlq_config: DlqConfig | None,
    override_dlq: Mapping[str, str | Sequence[str]],
) -> None:
    """Test that StreamSource.override_config raises ValueError when required fields are missing."""
    source = StreamSource(
        name="my_source",
        stream_name="my-topic",
        dlq_config=initial_dlq_config,
    )

    with pytest.raises(
        ValueError, match="DLQ config requires both 'topic' and 'bootstrap_servers'"
    ):
        source.override_config({"dlq": override_dlq})
