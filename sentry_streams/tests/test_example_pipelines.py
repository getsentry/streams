import json
import os
from dataclasses import dataclass
from typing import Any

import pytest

from sentry_streams.testing import run_pipeline_test


def create_ingest_message(**kwargs: Any) -> str:
    message = {
        "org_id": 420,
        "project_id": 420,
        "name": "s:sessions/user@none",
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
        },
        "timestamp": 1846062325,
        "type": "c",
        "retention_days": 90,
        "value": [1617781333],
    }
    message.update(kwargs)
    return json.dumps(message)


@dataclass
class ExampleTest:
    name: str
    source_topic: str
    sink_topics: list[str]
    input_messages: list[str]
    expected_messages: dict[str, int]
    pipeline_module: str
    advance_time_seconds: float = 0  # Time to advance for windowed operations


def run_example_test(test: ExampleTest) -> None:
    """Run an example pipeline test using LocalBroker."""
    import importlib.util

    # Dynamically import the pipeline module
    spec = importlib.util.spec_from_file_location(f"example_{test.name}", test.pipeline_module)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {test.pipeline_module}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get the pipeline from the module
    pipeline = module.pipeline

    # Convert string messages to bytes
    input_messages_bytes = [msg.encode("utf-8") for msg in test.input_messages]

    # Run the test using the helper
    run_pipeline_test(
        pipeline=pipeline,
        source_topic=test.source_topic,
        sink_topics=test.sink_topics,
        input_messages=input_messages_bytes,
        expected_messages=test.expected_messages,
        advance_time_after_processing=test.advance_time_seconds,
    )


example_tests = [
    pytest.param(
        ExampleTest(
            name="simple_map_filter",
            source_topic="ingest-metrics",
            sink_topics=["transformed-events"],
            input_messages=[create_ingest_message(type="c")],
            expected_messages={"transformed-events": 1},
            pipeline_module=os.path.join(
                os.path.dirname(__file__),
                "..",
                "sentry_streams",
                "examples",
                "simple_map_filter.py",
            ),
        ),
        id="simple_map_filter",
    ),
    pytest.param(
        ExampleTest(
            name="simple_batching",
            source_topic="ingest-metrics",
            sink_topics=["transformed-events"],
            input_messages=[
                create_ingest_message(type="c"),
                create_ingest_message(type="c"),
                create_ingest_message(type="c"),
                create_ingest_message(type="c"),
            ],
            expected_messages={"transformed-events": 2},
            pipeline_module=os.path.join(
                os.path.dirname(__file__),
                "..",
                "sentry_streams",
                "examples",
                "simple_batching.py",
            ),
            advance_time_seconds=101,  # Batch timeout is 100 seconds
        ),
        id="simple_batching",
        marks=pytest.mark.skip(reason="Batching has issues with LocalBroker, needs investigation"),
    ),
    pytest.param(
        ExampleTest(
            name="parallel_processing",
            source_topic="ingest-metrics",
            sink_topics=["transformed-events"],
            input_messages=[create_ingest_message(type="c")],
            expected_messages={"transformed-events": 1},
            pipeline_module=os.path.join(
                os.path.dirname(__file__),
                "..",
                "sentry_streams",
                "examples",
                "parallel_processing.py",
            ),
        ),
        id="parallel_processing",
    ),
    pytest.param(
        ExampleTest(
            name="transformer",
            source_topic="ingest-metrics",
            sink_topics=["transformed-events"],
            input_messages=[create_ingest_message(type="c")],
            expected_messages={"transformed-events": 1},
            pipeline_module=os.path.join(
                os.path.dirname(__file__),
                "..",
                "sentry_streams",
                "examples",
                "transformer.py",
            ),
            advance_time_seconds=7,  # Window size is 6 seconds, need to advance past it
        ),
        id="transformer",
        marks=pytest.mark.skip(reason="Windowing has issues with LocalBroker, needs investigation"),
    ),
]


@pytest.mark.parametrize("example_test", example_tests)
def test_examples(example_test: ExampleTest) -> None:
    run_example_test(example_test)
