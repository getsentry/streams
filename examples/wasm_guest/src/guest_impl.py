"""Sample WASM processor guest: passthrough with host logging."""

from __future__ import annotations

from typing import List, Optional

from wit_world.exports import Processor as ProcessorProtocol
from wit_world.exports import processor as proc
from wit_world.imports import log as host_log


class Processor(ProcessorProtocol):
    """Buffers submitted messages and watermarks; returns them on ``poll``."""

    def __init__(self) -> None:
        self._pending: List[proc.Output] = []

    def submit(self, msg: proc.Message) -> None:
        host_log.info(f"submit message len={len(msg.payload)}")
        self._pending.append(proc.Output_Msg(value=msg))

    def submit_watermark(self, wm: proc.Watermark) -> None:
        host_log.info("submit_watermark")
        self._pending.append(proc.Output_Wm(value=wm))

    def poll(self) -> Optional[List[proc.Output]]:
        if not self._pending:
            return None
        out = self._pending
        self._pending = []
        return out
