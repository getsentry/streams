"""Adapter wiring for :class:`ArrowBatchParser`.

The decoding itself is tested in Rust; what is worth testing here is the
placement contract, which is the part a pipeline author can get wrong.
"""

from typing import Any, Mapping

import pytest

from sentry_streams.adapters.arroyo.adapter import ArroyoAdapter
from sentry_streams.adapters.arroyo.rust_arroyo import RustArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.pipeline import (
    ArrowBatchParser,
    Map,
    Pipeline,
    StreamSink,
    streaming_source,
)
from sentry_streams.runner import iterate_edges

SOURCE_TOPIC = "snuba-items"

STEPS_CONFIG: Mapping[str, Any] = {
    "myinput": {
        "bootstrap_servers": ["localhost:9092"],
        "auto_offset_reset": "earliest",
        "consumer_group": "test_group",
        "override_params": {},
    },
    "kafkasink": {"bootstrap_servers": ["localhost:9092"], "override_params": {}},
}


def consume_batch(msg: Message[Any]) -> Any:
    """Module level, because the adapter pickle-checks transform chains."""
    return msg.payload


def keep_everything(msg: Message[Any]) -> bool:
    return True


def build_adapter(steps_config: Mapping[str, Any] = STEPS_CONFIG) -> RustArroyoAdapter:
    return RustArroyoAdapter.build(
        {"steps_config": steps_config},
        {"type": "dummy"},
    )


def parser_directly_after_source() -> Pipeline[Any]:
    return (
        streaming_source(name="myinput", stream_name=SOURCE_TOPIC)
        .apply(ArrowBatchParser(name="parse", batch_size=100))
        .apply(Map(name="consume", function=consume_batch))
        .sink(StreamSink(name="kafkasink", stream_name="transformed-events"))
    )


def parser_after_a_map() -> Pipeline[Any]:
    return (
        streaming_source(name="myinput", stream_name=SOURCE_TOPIC)
        .apply(Map(name="decode", function=consume_batch))
        .apply(ArrowBatchParser(name="parse", batch_size=100))
        .sink(StreamSink(name="kafkasink", stream_name="transformed-events"))
    )


def test_parser_directly_after_source_builds() -> None:
    adapter = build_adapter()
    iterate_edges(parser_directly_after_source(), RuntimeTranslator(adapter))
    assert adapter.get_consumer("myinput") is not None


def test_parser_after_a_python_step_is_rejected_at_build_time() -> None:
    """The step reads bytes off the wire, so a preceding Map has already thrown
    them away. Better a build error than a panic in production."""
    adapter = build_adapter()
    with pytest.raises(ValueError) as excinfo:
        iterate_edges(parser_after_a_map(), RuntimeTranslator(adapter))

    message = str(excinfo.value)
    assert "parse" in message, message
    assert "already" in message and "Python objects" in message, message


def test_a_filter_between_source_and_parser_is_allowed() -> None:
    """Filters forward messages untouched, so the raw payload survives."""
    from sentry_streams.pipeline.pipeline import PredicateFilter

    pipeline = (
        streaming_source(name="myinput", stream_name=SOURCE_TOPIC)
        .apply(PredicateFilter(name="keep", function=keep_everything))
        .apply(ArrowBatchParser(name="parse", batch_size=100))
        .sink(StreamSink(name="kafkasink", stream_name="transformed-events"))
    )
    adapter = build_adapter()
    iterate_edges(pipeline, RuntimeTranslator(adapter))
    assert adapter.get_consumer("myinput") is not None


def test_schema_survives_a_deployment_topic_override() -> None:
    """The extractor is resolved from the logical stream name, so overriding the
    physical topic in the deployment config must not change which schema is used.
    A wrong name here would be a startup panic in Rust."""
    steps_config = dict(STEPS_CONFIG)
    steps_config["myinput"] = {**STEPS_CONFIG["myinput"], "topic": "snuba-items-rerouted"}

    adapter = build_adapter(steps_config)
    iterate_edges(parser_directly_after_source(), RuntimeTranslator(adapter))
    assert adapter.get_consumer("myinput") is not None


def test_pure_python_adapter_refuses_the_step() -> None:
    adapter = ArroyoAdapter.build({"steps_config": STEPS_CONFIG})
    with pytest.raises(NotImplementedError, match="rust_arroyo"):
        iterate_edges(parser_directly_after_source(), RuntimeTranslator(adapter))


def test_validate_requires_a_size_or_a_time_bound() -> None:
    step: ArrowBatchParser[Any, Any] = ArrowBatchParser(
        name="parse", batch_size=None, batch_timedelta=None
    )
    with pytest.raises(ValueError, match="batch_size or batch_timedelta"):
        step.validate()


def test_override_config_applies_deployment_settings() -> None:
    step: ArrowBatchParser[Any, Any] = ArrowBatchParser(name="parse", batch_size=10)
    step.override_config({"batch_size": 500, "batch_timedelta": {"seconds": 3}})
    step.validate()
    assert step.batch_size == 500
    assert step.batch_timedelta is not None
    assert step.batch_timedelta.total_seconds() == 3.0
