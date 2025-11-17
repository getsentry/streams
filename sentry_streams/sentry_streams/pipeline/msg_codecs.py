import io
import json
from collections.abc import Iterable
from datetime import datetime
from functools import partial
from typing import (
    Any,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
)

import polars as pl
from polars import Schema as PolarsSchema
from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.codecs import Codec

from sentry_streams.pipeline.datatypes import (
    DataType,
)
from sentry_streams.pipeline.message import Message

# TODO: Push the following to docs
# Standard message decoders and encoders live here
# These are used in the defintions of Parser() and Serializer() steps, see chain/

CODECS: MutableMapping[str, Codec[Any]] = {}


def _get_codec_from_msg(msg: Message[Any]) -> Codec[Any]:
    stream_schema = msg.schema
    assert (
        stream_schema is not None
    )  # Message cannot be deserialized without a schema, it is automatically inferred from the stream source

    try:
        codec = CODECS.get(stream_schema, get_codec(stream_schema))
    except Exception:
        raise ValueError(f"Kafka topic {stream_schema} has no associated schema")
    return codec


def msg_parser(msg: Message[bytes]) -> Any:
    codec = _get_codec_from_msg(msg)
    payload = msg.payload
    decoded = codec.decode(payload, True)

    return decoded


def batch_msg_parser(msg: Message[Sequence[bytes]]) -> Sequence[Any]:
    payloads = msg.payload
    codec = _get_codec_from_msg(msg)
    return [codec.decode(payload, True) for payload in payloads]


def msg_serializer(msg: Message[Any], dt_format: Optional[str] = None) -> bytes:
    payload = msg.payload

    def custom_serializer(obj: Any, dt_format: Optional[str] = None) -> str:
        if isinstance(obj, datetime):
            if dt_format:
                return obj.strftime(dt_format)
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    serializer = partial(custom_serializer, dt_format=dt_format)
    return json.dumps(payload, default=serializer).encode("utf-8")


ParquetCompression = Literal["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]


def resolve_polars_schema(schema_fields: Mapping[str, DataType]) -> PolarsSchema:
    resolved_schema = {key: dtype.resolve() for key, dtype in schema_fields.items()}
    polars_schema = pl.Schema(resolved_schema)
    return polars_schema


def serialize_to_parquet(
    msg: Message[Iterable[Any]],
    polars_schema: PolarsSchema,
    compression: ParquetCompression,
) -> bytes:
    df = pl.DataFrame([i for i in msg.payload if i is not None], schema=polars_schema)
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression=compression, statistics=False, use_pyarrow=False)
    return bytes(buffer.getvalue())
