from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import urlparse

from sentry_streams.adapters.stream_adapter import (
    RuntimeStateError,
    RuntimeStatus,
)
from sentry_streams.control import PipelineController

logger = logging.getLogger(__name__)


class ControlHandler(BaseHTTPRequestHandler):
    @property
    def _controller(self) -> PipelineController:
        return cast(ControlServer, self.server).controller

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("control-server %s - %s", self.address_string(), format % args)

    def _respond(self, code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path == "/readyz":
                snapshot = self._controller.snapshot
                code = 503 if snapshot.is_terminal else 200
                self._respond(code, snapshot.as_dict())
            elif path == "/status":
                self._respond(200, self._controller.snapshot.as_dict())
            else:
                self._respond(404, {"error": "not found"})
        except Exception as exc:
            self._respond_to_failure(exc)

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            if path == "/start":
                self._respond(202, self._controller.request_start().as_dict())
            elif path == "/stop":
                self._respond_to_stop(self._controller.request_stop())
            else:
                self._respond(404, {"error": "not found"})
        except Exception as exc:
            self._respond_to_failure(exc)

    def _respond_to_stop(self, snapshot: RuntimeStatus) -> None:
        code = 200 if snapshot.is_terminal else 202
        self._respond(code, snapshot.as_dict())

    def _respond_to_failure(self, exc: Exception) -> None:
        if isinstance(exc, RuntimeStateError):
            logger.info("control-server rejected %s: %s", self.path, exc)
            self._respond(409, {"error": str(exc)})
        else:
            logger.exception("control-server request failed: %s", self.path)
            self._respond(500, {"error": str(exc)})


class ControlServer(ThreadingHTTPServer):
    """
    Expose lifecycle control for a streaming consumer process over HTTP.

    In operator-controlled mode, the consumer process loads its pipeline but leaves
    it idle. This server wraps its PipelineController so an external manager
    (like the operator) can manually start consuming and observe/change state.

    - POST /start: Start consuming.
    - POST /stop: Stop consuming.

    - GET /status: Get the current state.
    - GET /readyz: Get a readiness response.

    Invalid state transitions are rejected.
    """

    def __init__(self, address: tuple[str, int], controller: PipelineController) -> None:
        self.controller = controller
        super().__init__(address, ControlHandler)


def make_server(controller: PipelineController, host: str, port: int) -> ControlServer:
    return ControlServer((host, port), controller)


def serve(server: ThreadingHTTPServer) -> None:
    host, port = server.server_address[:2]
    logger.info(
        "Streams control server listening on %s:%d (pipeline idle, awaiting /start)",
        host,
        port,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
