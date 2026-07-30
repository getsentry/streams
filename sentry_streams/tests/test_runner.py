import os
import signal
import threading
from enum import Enum
from typing import Any, cast

import pytest

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    PipelineConfig,
    RuntimeState,
    RuntimeTranslator,
)
from sentry_streams.control import PipelineController
from sentry_streams.dummy.dummy_adapter import DummyAdapter
from sentry_streams.pipeline import Map, PredicateFilter, branch, streaming_source
from sentry_streams.pipeline.pipeline import (
    DevNullSink,
    Pipeline,
)
from sentry_streams.runner import (
    _install_signal_handlers,
    _run_pipeline,
    iterate_edges,
)
from tests.adapters.fake_adapter import FakeAdapter


class RouterBranch(Enum):
    BRANCH1 = "branch1"
    BRANCH2 = "branch2"


@pytest.fixture
def create_pipeline() -> Pipeline[bytes]:
    broadcast_branch_1 = (
        branch("branch1")
        .apply(Map("map2", function=lambda x: x.payload))
        .route(
            "router1",
            routing_function=lambda x: RouterBranch.BRANCH1.value,
            routing_table={
                RouterBranch.BRANCH1.value: branch("map4_segment")
                .apply(Map("map4", function=lambda x: x.payload))
                .sink(DevNullSink("sink_map4")),
                RouterBranch.BRANCH2.value: branch("map5_segment")
                .apply(Map("map5", function=lambda x: x.payload))
                .sink(DevNullSink("sink_map5")),
            },
        )
    )
    broadcast_branch_2 = (
        branch("branch2")
        .apply(Map("map3", function=lambda x: x.payload))
        .sink(DevNullSink("sink_map3"))
    )

    test_pipeline = (
        streaming_source("source1", stream_name="foo")
        .apply(Map("map1", function=lambda x: x.payload))
        .apply(PredicateFilter("filter1", function=lambda x: True))
        .broadcast(
            "broadcast_to_maps",
            routes=[
                broadcast_branch_1,
                broadcast_branch_2,
            ],
        )
    )

    return test_pipeline


def test_iterate_edges(create_pipeline: Pipeline[bytes]) -> None:
    dummy_config: PipelineConfig = {}
    runtime = cast(
        DummyAdapter[Any, Any],
        load_adapter(
            "dummy",
            dummy_config,
            {"type": "dummy"},
            None,
        ),
    )
    translator: RuntimeTranslator[Any, Any] = RuntimeTranslator(runtime)
    iterate_edges(create_pipeline, translator)
    assert runtime.input_streams == [
        "source1",
        "map1",
        "filter1",
        "broadcast_to_maps",
        "map2",
        "map3",
        "router1",
        "sink_map3",
        "map4",
        "map5",
        "sink_map4",
        "sink_map5",
    ]
    assert runtime.branches == [
        "branch1",
        "branch2",
        "branch1",
        "branch2",
        "map4_segment",
        "map5_segment",
    ]


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_run_pipeline_terminal_signals(signum: int) -> None:
    runtime = FakeAdapter()
    shutdown_requested = threading.Event()
    _install_signal_handlers(shutdown_requested)

    def send_signal() -> None:
        assert runtime.run_started.wait(3.0)
        os.kill(os.getpid(), signum)

    signal_thread = threading.Thread(target=send_signal)
    signal_thread.start()
    try:
        snapshot = _run_pipeline(PipelineController(runtime), shutdown_requested)
    finally:
        signal_thread.join(timeout=3.0)
        signal.signal(signal.SIGINT, signal.default_int_handler)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    assert snapshot.state is RuntimeState.STOPPED
    assert runtime.shutdown_calls == 1
