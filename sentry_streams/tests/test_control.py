from __future__ import annotations

import threading
import time

import pytest

from sentry_streams.adapters.stream_adapter import RuntimeState, RuntimeStatus
from sentry_streams.control import PipelineController, PipelineStateError
from tests.adapters.fake_adapter import FakeAdapter


def _wait_for_state(
    controller: PipelineController, state: RuntimeState, timeout: float = 3.0
) -> RuntimeStatus:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = controller.snapshot
        if snapshot.state is state:
            return snapshot
        time.sleep(0.01)
    raise AssertionError(f"pipeline is {controller.snapshot.state}, expected {state}")


def _stop(controller: PipelineController) -> None:
    controller.request_stop()
    controller.wait_until_stopped(3.0)


def test_start_and_stop_are_non_blocking() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    try:
        assert controller.request_start().state is RuntimeState.STARTING
        assert controller.request_start().state in (
            RuntimeState.STARTING,
            RuntimeState.CONSUMING,
        )
        assert runtime.run_started.wait(3.0)
        _wait_for_state(controller, RuntimeState.CONSUMING)

        assert controller.request_stop().state is RuntimeState.STOPPING
        assert controller.request_stop().state in (
            RuntimeState.STOPPING,
            RuntimeState.STOPPED,
        )
        assert controller.wait_until_stopped(3.0).state is RuntimeState.STOPPED
        assert runtime.run_calls == 1
        assert runtime.shutdown_calls == 1
    finally:
        _stop(controller)


def test_controller_reports_adapter_runtime_status() -> None:
    runtime = FakeAdapter(block_before_consuming=True)
    controller = PipelineController(runtime)
    try:
        controller.request_start()
        assert runtime.run_started.wait(3.0)
        assert controller.snapshot.state is RuntimeState.STARTING

        runtime.allow_consume.set()
        _wait_for_state(controller, RuntimeState.CONSUMING)
    finally:
        runtime.allow_consume.set()
        _stop(controller)


def test_stop_during_start_is_not_lost() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    try:
        controller.request_start()
        assert controller.request_stop().state is RuntimeState.STOPPING

        assert controller.wait_until_stopped(3.0).state is RuntimeState.STOPPED
        assert runtime.shutdown_calls == 1
    finally:
        _stop(controller)


def test_stop_before_start_shuts_the_runtime_down() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    try:
        assert controller.request_stop().state is RuntimeState.STOPPED
        assert runtime.shutdown_calls == 1
        assert runtime.run_calls == 0
    finally:
        _stop(controller)


def test_waiting_for_an_idle_pipeline_ends_when_it_is_stopped() -> None:
    controller = PipelineController(FakeAdapter())
    try:
        stopper = threading.Thread(target=controller.request_stop)
        stopper.start()
        assert controller.wait_until_finished(3.0).state is RuntimeState.STOPPED
        stopper.join(3.0)
    finally:
        _stop(controller)


def test_waiting_for_a_running_pipeline_ends_when_its_run_loop_exits() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    try:
        controller.request_start()
        assert runtime.run_started.wait(3.0)
        controller.request_stop()

        assert controller.wait_until_finished(3.0).state is RuntimeState.STOPPED
        assert runtime.run_finished.is_set()
    finally:
        _stop(controller)


def test_stopping_a_failed_runtime_keeps_the_failure() -> None:
    runtime = FakeAdapter(fail=True)
    controller = PipelineController(runtime)
    try:
        assert controller.request_start().state is RuntimeState.STARTING
        assert controller.wait_until_stopped(3.0).error == "runtime failed"

        controller.request_stop()
        _stop(controller)

        snapshot = controller.snapshot
        assert snapshot.state is RuntimeState.ERRORED
        assert snapshot.error == "runtime failed"
    finally:
        _stop(controller)


def test_stopped_runtime_cannot_restart() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    try:
        controller.request_start()
        assert runtime.run_started.wait(3.0)
        controller.request_stop()
        controller.wait_until_stopped(3.0)

        with pytest.raises(PipelineStateError, match="cannot restart"):
            controller.request_start()
        assert runtime.run_calls == 1
        assert runtime.shutdown_calls == 1
    finally:
        _stop(controller)


def test_shutdown_before_the_run_loop_never_starts() -> None:
    runtime = FakeAdapter()
    runtime.begin_start()
    runtime.shutdown()

    runtime.run()

    assert runtime.status.state is RuntimeState.STOPPED
    assert runtime.run_calls == 0
