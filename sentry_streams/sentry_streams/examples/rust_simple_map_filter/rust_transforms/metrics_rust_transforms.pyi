"""
Manually written type stubs for Rust functions. These type stubs are not used
by the streaming runtime but are mainly there to maintain type safety within
the pipeline definition.

We hope that in a future version the runtime can generate those out of the rust
macros.
"""

from typing import Any

from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.rust_function_protocol import RustFunction

class RustFilterEvents(RustFunction[IngestMetric, bool]):
    def __init__(self) -> None: ...
    def __call__(self, msg: Message[IngestMetric]) -> bool: ...

class RustTransformMsg(RustFunction[IngestMetric, Any]):
    def __init__(self) -> None: ...
    def __call__(self, msg: Message[IngestMetric]) -> Any: ...
