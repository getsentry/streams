from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from sentry_streams.adapters.stream_adapter import (
    RuntimeStatus,
    StreamAdapter,
)


class PipelineController:
    """
    Control panel for one streaming pipeline. Starts the adapter's blocking run loop
    in a background thread and allows concurrent requests to start/stop the pipeline.

    A pipeline is single-use: after it stops or fails, it cannot be restarted.
    A replacement deployment must create a new controller and adapter.

    Start and stop requests can arrive concurrently from HTTP handler and process
    shutdown threads. The lock prevents state transition race conditions.
    """

    def __init__(self, runtime: StreamAdapter[Any, Any]) -> None:
        self._runtime = runtime
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pipeline-run")
        self._run_future: Future[None] | None = None
        self._finished = threading.Event()

    @property
    def snapshot(self) -> RuntimeStatus:
        return self._runtime.status

    def request_start(self) -> RuntimeStatus:
        """
        Ask the pipeline to start (non-blocking).
        """
        with self._lock:
            status = self._runtime.begin_start()
            if self._run_future is None:
                self._run_future = self._executor.submit(self._runtime.run)
                self._run_future.add_done_callback(self._run_finished)
            return status

    def request_stop(self) -> RuntimeStatus:
        """
        Ask the pipeline to stop (non-blocking).
        """
        with self._lock:
            status = self._runtime.shutdown()
            if self._run_future is None:
                self._finished.set()
                self._executor.shutdown(wait=False)

            return status

    def wait_until_finished(self, timeout: float | None = None) -> RuntimeStatus:
        """
        Wait until this controller is completely finished.
        """
        if not self._finished.wait(timeout):
            return self._runtime.status

        with self._lock:
            run_future = self._run_future

        if run_future is not None:
            run_future.result()

        return self._runtime.status

    def wait_until_stopped(self, timeout: float | None = None) -> RuntimeStatus:
        """
        Wait for a started pipeline's background thread to exit.
        """
        self._finished.wait(timeout)
        return self._runtime.status

    def _run_finished(self, _future: Future[None]) -> None:
        self._finished.set()
        self._executor.shutdown(wait=False)
