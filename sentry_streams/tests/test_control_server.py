from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable

from sentry_streams.adapters.stream_adapter import RuntimeState
from sentry_streams.control import PipelineController
from sentry_streams.server.control_server import make_server
from tests.adapters.fake_adapter import FakeAdapter


def _wait_for(predicate: Callable[[], bool], timeout: float = 3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _request(port: int, path: str, method: str) -> tuple[int, dict[str, Any]]:
    data = b"" if method == "POST" else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, (json.loads(raw) if raw else {})


def _stop(controller: PipelineController) -> None:
    controller.request_stop()
    controller.wait_until_stopped(3.0)


def test_control_server_endpoints() -> None:
    runtime = FakeAdapter()
    controller = PipelineController(runtime)
    server = make_server(controller, "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        code, body = _request(port, "/readyz", "GET")
        assert code == 200 and body["state"] == RuntimeState.IDLE

        code, _ = _request(port, "/does-not-exist", "GET")
        assert code == 404

        code, _ = _request(port, "/start", "POST")
        assert code == 202
        assert _wait_for(
            lambda: _request(port, "/status", "GET")[1]["state"] == RuntimeState.CONSUMING
        )

        code, body = _request(port, "/stop", "POST")
        assert code == 202 and body["state"] == RuntimeState.STOPPING
        assert controller.wait_until_stopped(3.0).state is RuntimeState.STOPPED
        assert _request(port, "/does-not-exist", "POST")[0] == 404
        assert runtime.shutdown_calls == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3.0)
        _stop(controller)


def test_readyz_reports_runtime_failure() -> None:
    runtime = FakeAdapter(fail=True)
    controller = PipelineController(runtime)
    server = make_server(controller, "127.0.0.1", 0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert _request(port, "/start", "POST")[0] == 202
        assert controller.wait_until_stopped(3.0).state is RuntimeState.ERRORED
        code, body = _request(port, "/readyz", "GET")
        assert code == 503
        assert body["state"] == RuntimeState.ERRORED
        assert body["error"] == "runtime failed"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3.0)
        _stop(controller)
