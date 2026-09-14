"""Adapter wiring for :class:`ArrowBatchParser`.

The decoding itself is tested in Rust; what is worth testing here is the
placement contract, which is the part a pipeline author can get wrong.
"""

import subprocess
import sys
import tempfile
from pathlib import Path
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
        .apply(ArrowBatchParser(name="parse", schema_name=SOURCE_TOPIC, batch_size=100))
        .apply(Map(name="consume", function=consume_batch))
        .sink(StreamSink(name="kafkasink", stream_name="transformed-events"))
    )


def test_parser_directly_after_source_builds() -> None:
    adapter = build_adapter()
    iterate_edges(parser_directly_after_source(), RuntimeTranslator(adapter))
    assert adapter.get_consumer("myinput") is not None


def test_parser_after_a_python_step_is_a_type_error() -> None:
    """The step takes bytes, so placing it after a step that produces something
    else is a type error. There is no build-time check: mypy catches it, and if
    the types are bypassed the Rust step panics on the first batch.

    Run mypy out of process, because the mistake is by construction not
    detectable at runtime."""
    code = """
from sentry_streams.pipeline.pipeline import (
    ArrowBatchParser,
    Map,
    StreamSink,
    streaming_source,
)
from sentry_streams.pipeline.message import Message


def to_text(msg: Message[bytes]) -> str:
    return msg.payload.decode()


pipeline = (
    streaming_source("myinput", "snuba-items")
    .apply(Map("decode", function=to_text))                 # bytes -> str
    .apply(ArrowBatchParser("parse", schema_name="snuba-items"))  # wants bytes!
    .sink(StreamSink("mysink", "transformed-events"))
)
"""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "misplaced.py"
        path.write_text(code)
        result = subprocess.run(
            [sys.executable, "-m", "mypy", str(path), "--show-error-codes"],
            capture_output=True,
            text=True,
        )

    assert result.returncode > 0, result.stdout
    # Specifically the placement error, not merely "mypy was unhappy".
    assert (
        'Argument 1 to "apply" of "Pipeline" has incompatible type' in result.stdout
    ), result.stdout
    assert "ArrowBatchParser" in result.stdout, result.stdout


def test_parser_directly_after_source_type_checks() -> None:
    """The mirror of the test above: correct placement is accepted."""
    code = """
from sentry_streams.pipeline.pipeline import ArrowBatchParser, StreamSink, streaming_source

pipeline = (
    streaming_source("myinput", "snuba-items")
    .apply(ArrowBatchParser("parse", schema_name="snuba-items"))
    .sink(StreamSink("mysink", "transformed-events"))
)
"""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "placed.py"
        path.write_text(code)
        result = subprocess.run(
            [sys.executable, "-m", "mypy", str(path), "--show-error-codes"],
            capture_output=True,
            text=True,
        )

    assert result.returncode == 0, result.stdout


def test_a_filter_between_source_and_parser_is_allowed() -> None:
    """Filters forward messages untouched, so the raw payload survives."""
    from sentry_streams.pipeline.pipeline import PredicateFilter

    pipeline = (
        streaming_source(name="myinput", stream_name=SOURCE_TOPIC)
        .apply(PredicateFilter(name="keep", function=keep_everything))
        .apply(ArrowBatchParser(name="parse", schema_name=SOURCE_TOPIC, batch_size=100))
        .sink(StreamSink(name="kafkasink", stream_name="transformed-events"))
    )
    adapter = build_adapter()
    iterate_edges(pipeline, RuntimeTranslator(adapter))
    assert adapter.get_consumer("myinput") is not None


def test_schema_is_independent_of_the_deployment_topic() -> None:
    """schema_name is declared on the step, so overriding the physical topic in
    the deployment config cannot change which extractor is resolved."""
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
    step: ArrowBatchParser[Any] = ArrowBatchParser(
        name="parse", schema_name=SOURCE_TOPIC, batch_size=None, batch_timedelta=None
    )
    with pytest.raises(ValueError, match="batch_size or batch_timedelta"):
        step.validate()


def test_override_config_applies_deployment_settings() -> None:
    step: ArrowBatchParser[Any] = ArrowBatchParser(
        name="parse", schema_name=SOURCE_TOPIC, batch_size=10
    )
    step.override_config({"batch_size": 500, "batch_timedelta": {"seconds": 3}})
    step.validate()
    assert step.batch_size == 500
    assert step.batch_timedelta is not None
    assert step.batch_timedelta.total_seconds() == 3.0
