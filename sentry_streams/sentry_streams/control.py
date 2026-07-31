from __future__ import annotations

import logging
import threading
from typing import Any

from sentry_streams.adapters.stream_adapter import (
    RuntimeStatus,
    StreamAdapter,
)

logger = logging.getLogger(__name__)


class PipelineController:
    """
    Control panel for one streaming pipeline. Starts the adapter's blocking run loop
    in a background thread and allows concurrent requests to start/stop the pipeline.

    A pipeline is single-use: after it stops or fails, it cannot be restarted.
    A replacement deployment must create a new controller and adapter.
    """

    def __init__(self, runtime: StreamAdapter[Any, Any]) -> None:
        self._runtime = runtime
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
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
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run_runtime,
                    name="pipeline-run",
                    daemon=False,
                )
                self._thread.start()
            return status

    def request_stop(self) -> RuntimeStatus:
        """
        Ask the pipeline to stop (non-blocking).
        """
        with self._lock:
            status = self._runtime.shutdown()
            if self._thread is None:
                self._finished.set()

            return status

    def wait_until_finished(self, timeout: float | None = None) -> RuntimeStatus:
        """
        Wait until this controller is completely finished.
        """
        self._finished.wait(timeout)
        return self._runtime.status

    def wait_until_stopped(self, timeout: float | None = None) -> RuntimeStatus:
        """
        Wait for a started pipeline's background thread to exit.
        """
        with self._lock:
            thread = self._thread

        if thread is not None:
            thread.join(timeout)

        return self._runtime.status

    def _run_runtime(self) -> None:
        try:
            self._runtime.run()
        except Exception:
            logger.exception("pipeline run loop failed")
        finally:
            self._finished.set()
