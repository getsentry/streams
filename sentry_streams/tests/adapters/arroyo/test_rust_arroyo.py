from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.pipeline import (
    Pipeline,
)
from sentry_streams.runner import iterate_edges


def test_rust_arroyo_adapter_write_healthcheck_from_runtime_config() -> None:
    """Adapter respects pipeline.runtime_config.arroyo.write_healthcheck when building."""
    bootstrap_servers = ["localhost:9092"]
    config = {
        "runtime_config": {
            "arroyo": {
                "write_healthcheck": True,
            },
        },
        "steps_config": {
            "myinput": {
                "bootstrap_servers": bootstrap_servers,
                "starts_segment": True,
            },
            "kafkasink": {"bootstrap_servers": bootstrap_servers},
        },
    }
    adapter = RustArroyoAdapter.build(config)
    assert adapter.write_healthcheck is True


def test_rust_arroyo_adapter_write_healthcheck_default_false() -> None:
    """Adapter has write_healthcheck False when runtime_config is absent."""
    bootstrap_servers = ["localhost:9092"]
    config = {
        "steps_config": {
            "myinput": {
                "bootstrap_servers": bootstrap_servers,
                "starts_segment": True,
            },
            "kafkasink": {"bootstrap_servers": bootstrap_servers},
        },
    }
    adapter = RustArroyoAdapter.build(config)
    assert adapter.write_healthcheck is False


def test_rust_arroyo_adapter(
    pipeline: Pipeline[bytes],
) -> None:
    bootstrap_servers = ["localhost:9092"]  # Test Kafka servers

    adapter = RustArroyoAdapter.build(
        {
            "steps_config": {
                "myinput": {
                    "bootstrap_servers": bootstrap_servers,
                    "auto_offset_reset": "earliest",
                    "consumer_group": "test_group",
                    "additional_settings": {},
                },
                "kafkasink": {"bootstrap_servers": bootstrap_servers, "additional_settings": {}},
            },
        },
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    # Most of the logic lives in the Rust code, so it can't be inspected here.
    # The consumer that this adapter uses is a pyo3 wrapper around the Rust consumer,
    # so it also can't be replaced with the in-memory broker or triggered manually.
    assert adapter.get_consumer("myinput") is not None
