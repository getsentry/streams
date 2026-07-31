from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from enum import StrEnum
from typing import Any, cast

from sentry_streams_k8s.operator.constants import CONTROL_REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


class RuntimeState(StrEnum):
    # Mirrors sentry_streams.adapters.stream_adapter.RuntimeState.
    # TODO: Remove  once sentry_streams is updated to expose RuntimeState.

    IDLE = "idle"
    STARTING = "starting"
    CONSUMING = "consuming"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"

    @property
    def is_terminal(self) -> bool:
        return self in (RuntimeState.STOPPED, RuntimeState.ERRORED)


class ControlError(Exception):
    """Raised when a control request could not be completed."""


class ControlClient:
    def __init__(
        self,
        port: int,
        timeout: float = CONTROL_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._port = port
        self._timeout = timeout

    def _request(self, ip: str, path: str, method: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else b""
        request = urllib.request.Request(
            f"http://{ip}:{self._port}{path}",
            method=method,
            data=data,
            headers={"Content-Type": "application/json"} if body is not None else {},
        )
        with urllib.request.urlopen(request, timeout=self._timeout) as response:
            raw = response.read()
        return json.loads(raw) if raw else {}

    def status(self, ip: str) -> RuntimeState | None:
        try:
            payload = self._request(ip, "/status", "GET")
        except (urllib.error.URLError, OSError, ValueError) as error:
            logger.debug("control server at %s is unreachable: %s", ip, error)
            return None

        state = cast(dict[str, Any], payload).get("state")
        try:
            return RuntimeState(state) if isinstance(state, str) else None
        except ValueError:
            logger.warning("control server at %s reported unknown state %r", ip, state)
            return None

    def readyz(self, ip: str) -> bool:
        try:
            self._request(ip, "/readyz", "GET")
            return True
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def start(self, ip: str, group_instance_id: str) -> None:
        try:
            self._request(ip, "/start", "POST", {"group_instance_id": group_instance_id})
        except urllib.error.HTTPError as error:
            if error.code == 409:
                logger.info("consumer at %s was not idle when started: %s", ip, error)
                return
            raise ControlError(f"failed to start consumer at {ip}: {error}") from error
        except (urllib.error.URLError, OSError, ValueError) as error:
            raise ControlError(f"failed to start consumer at {ip}: {error}") from error

    def stop(self, ip: str) -> bool:
        try:
            self._request(ip, "/stop", "POST")
            return True
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError) as error:
            logger.info("could not stop consumer at %s, treating it as stopped: %s", ip, error)
            return False
