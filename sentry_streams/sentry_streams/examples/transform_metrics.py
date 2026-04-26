import logging
import os
import uuid
from typing import Any, Mapping, MutableMapping, Optional, Sequence

import pyarrow as pa
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue, TraceItem
from sentry_streams.pipeline.msg_codecs import msg_parser
from sentry_streams.pipeline.datatypes import (
    Binary,
    Boolean,
    DataType,
    Datetime,
    Field,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    String,
    Struct,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
)
from sentry_streams.pipeline.message import Message

num = 0


def _convert_type(arrow_type: pa.DataType) -> DataType:
    if arrow_type == pa.bool_():
        return Boolean()
    elif arrow_type == pa.int8():
        return Int8()
    elif arrow_type == pa.int16():
        return Int16()
    elif arrow_type == pa.int32():
        return Int32()
    elif arrow_type == pa.int64():
        return Int64()
    elif arrow_type == pa.uint8():
        return Uint8()
    elif arrow_type == pa.uint16():
        return Uint16()
    elif arrow_type == pa.uint32():
        return Uint32()
    elif arrow_type == pa.uint64():
        return Uint64()
    elif arrow_type == pa.float32():
        return Float32()
    elif arrow_type == pa.float64():
        return Float64()
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return String()
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return Binary()
    elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return Datetime(None)
    elif pa.types.is_timestamp(arrow_type):
        return Datetime(None)
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return List(_convert_type(arrow_type.value_type))
    elif pa.types.is_struct(arrow_type):
        return Struct([Field(f.name, _convert_type(f.type)) for f in arrow_type])
    else:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")


def arrow_to_streams_datatypes(fields: list[tuple[str, pa.DataType]]) -> dict[str, DataType]:
    return {name: _convert_type(dtype) for name, dtype in fields}


def transform_msg(msg: Message[IngestMetric]) -> Mapping[str, Any]:
    global num
    num += 1
    print(f"Current PID: {os.getpid()} {num}")
    return {**msg.payload, "transformed": True}


def filter_events(msg: Message[IngestMetric]) -> bool:
    print(f"Filtering event: {msg.payload}")
    return bool(msg.payload["type"] == "c")


SENTRY_TAGS_TO_COLUMNS = {
    "sentry.system": "db_system",
    "sentry.op": "span_op",
    "sentry.transaction": "segment_name",
    "sentry.transaction.op": "segment_op",
    "sentry.sdk.name": "sdk_name",
    "sentry.sdk.version": "sdk_version",
    "sentry.platform": "platform",
}

GEN_AI_ATTRIBUTES_TO_COLUMNS = {
    "gen_ai.operation.type": "gen_ai_operation_type",
    "gen_ai.request.model": "gen_ai_request_model",
    "gen_ai.response.model": "gen_ai_response_model",
    "gen_ai.usage.input_tokens": "gen_ai_usage_input_tokens",
    "gen_ai.usage.output_tokens": "gen_ai_usage_output_tokens",
    "gen_ai.usage.total_tokens": "gen_ai_usage_total_tokens",
    "gen_ai.usage.input_tokens.cached": "gen_ai_usage_input_tokens_cached",
    "gen_ai.usage.output_tokens.reasoning": "gen_ai_usage_output_tokens_reasoning",
    "gen_ai.cost.input_tokens": "gen_ai_cost_input_tokens",
    "gen_ai.cost.output_tokens": "gen_ai_cost_output_tokens",
    "gen_ai.cost.total_tokens": "gen_ai_cost_total_tokens",
}


def do_nothing(msg: Message[bytes]) -> Any:
    return msg_parser(msg)


def do_something(msg: Message[TraceItem]) -> Any:
    span = msg.payload
    processed: MutableMapping[str, Any] = {}
    processed["organization_id"] = span.organization_id
    processed["project_id"] = span.project_id
    processed["trace_id"] = span.trace_id
    processed["span_id"] = uuid_from_item_id(span.item_id)[16:]
    processed["retention_days"] = span.retention_days
    return processed


def uuid_from_item_id(item_id: bytes) -> str:
    return uuid.UUID(int=int.from_bytes(item_id, byteorder="little", signed=False)).hex


def get_attribute(attributes: dict[AnyValue], key: str) -> Any:
    if key not in attributes:
        return None
    value = attributes[key]
    getter = value.WhichOneof("value")
    if not getter:
        return None
    return getattr(value, getter)


class ItemsSpanProcessor:
    def __init__(self) -> None:
        self._logger: Optional[logging.Logger] = None
        self.schema_fields = [
            ("db_system", pa.string()),
            ("description", pa.string()),
            ("duration_ms", pa.uint64()),
            ("is_segment", pa.bool_()),
            ("organization_id", pa.uint64()),
            ("origin", pa.string()),
            ("parent_span_id", pa.string()),
            ("platform", pa.string()),
            ("project_id", pa.uint64()),
            ("retention_days", pa.uint16()),
            ("sdk_name", pa.string()),
            ("sdk_version", pa.string()),
            ("segment_id", pa.string()),
            ("segment_name", pa.string()),
            ("segment_op", pa.string()),
            ("span_id", pa.string()),
            ("span_op", pa.string()),
            ("start_timestamp_ms", pa.uint64()),
            ("trace_id", pa.string()),
            ("ai_total_cost", pa.float64()),
            ("ai_total_tokens_used", pa.float64()),
            ("ai_prompt_tokens_used", pa.float64()),
            ("ai_completion_tokens_used", pa.float64()),
            ("gen_ai_operation_type", pa.string()),
            ("gen_ai_request_model", pa.string()),
            ("gen_ai_response_model", pa.string()),
            ("gen_ai_usage_input_tokens", pa.int64()),
            ("gen_ai_usage_output_tokens", pa.int64()),
            ("gen_ai_usage_total_tokens", pa.int64()),
            ("gen_ai_usage_input_tokens_cached", pa.int64()),
            ("gen_ai_usage_output_tokens_reasoning", pa.int64()),
            ("gen_ai_cost_input_tokens", pa.float64()),
            ("gen_ai_cost_output_tokens", pa.float64()),
            ("gen_ai_cost_total_tokens", pa.float64()),
        ]
        self.schema_fields_sentrystreams = arrow_to_streams_datatypes(self.schema_fields)

    @property
    def logger(self) -> logging.Logger:
        """Lazy initialization of logger to allow pickling."""
        if self._logger is None:
            self._logger = logging.getLogger("super-big-data-streaming-items-span")
        return self._logger

    def _process_item_span(self, processed: MutableMapping[str, Any], span: TraceItem) -> None:
        processed["organization_id"] = span.organization_id
        processed["project_id"] = span.project_id
        processed["trace_id"] = span.trace_id
        processed["span_id"] = uuid_from_item_id(span.item_id)[16:]
        processed["retention_days"] = span.retention_days

        start_timestamp = get_attribute(span.attributes, "sentry.start_timestamp_precise")
        processed["start_timestamp_ms"] = (
            int(start_timestamp * 1000) if start_timestamp is not None else None
        )
        processed["duration_ms"] = get_attribute(span.attributes, "sentry.duration_ms")
        processed["is_segment"] = get_attribute(span.attributes, "sentry.is_segment")
        processed["origin"] = get_attribute(span.attributes, "sentry.origin")
        processed["parent_span_id"] = get_attribute(span.attributes, "sentry.parent_span_id")
        processed["segment_id"] = get_attribute(span.attributes, "sentry.segment_id")

        description: Optional[str] = span.attributes["sentry.normalized_description"].string_value
        processed["description"] = "not blank" if description else "blank"

        for key, value in span.attributes.items():
            if key in {
                "ai_total_cost",
                "ai_total_tokens_used",
                "ai_prompt_tokens_used",
                "ai_completion_tokens_used",
            }:
                processed[key] = value.double_value

        for tag, column in SENTRY_TAGS_TO_COLUMNS.items():
            if tag in span.attributes:
                processed[column] = span.attributes[tag].string_value

    def _process_gen_ai(self, processed: MutableMapping[str, Any], span: TraceItem) -> None:
        for attr, column in GEN_AI_ATTRIBUTES_TO_COLUMNS.items():
            value = get_attribute(span.attributes, attr)
            if isinstance(value, str):
                if column.startswith("gen_ai_usage"):
                    try:
                        value = int(float(value))
                    except (ValueError, TypeError, OverflowError):
                        value = None
                elif column.startswith("gen_ai_cost"):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        value = None
            elif isinstance(value, float) and column.startswith("gen_ai_usage"):
                value = int(value)
            processed[column] = value

    def process_parsed_message(self, span: TraceItem) -> dict[str, Any]:
        processed: MutableMapping[str, Any] = {}
        self._process_item_span(processed, span)
        self._process_gen_ai(processed, span)
        self._prepare_gcs_message(processed)
        return processed

    def process_item(self, span: TraceItem) -> dict[str, Any]:
        try:
            return self.process_parsed_message(span)
        except Exception:
            self.logger.exception("error while processing item span")
            raise

    def __call__(self, message: Message[Sequence[TraceItem]]) -> Sequence[dict[str, Any]]:
        return self.process_batch_messages(message)

    def process_stream_message(self, message: Message[TraceItem]) -> dict[str, Any]:
        return self.process_item(message.payload)

    def process_batch_messages(
        self, msg_batch: Message[Sequence[TraceItem]]
    ) -> Sequence[dict[str, Any]]:
        return [self.process_item(item) for item in msg_batch.payload]

    def _prepare_gcs_message(self, processed: MutableMapping[str, Any]) -> None:
        for field in self.schema_fields:
            column = field[0]
            if column not in processed:
                processed[column] = None
