import io
import json
from datetime import datetime
from enum import Enum
from functools import partial
from typing import Any, MutableMapping, Optional, Sequence, Tuple, Union

import polars as pl
import pyarrow.types as types
from polars import Schema as PolarsSchema
from polars._typing import PolarsDataType
from pyarrow import DataType as PADataType
from pyarrow import DictionaryType, ListType, StructType
from sentry_kafka_schemas import get_codec
from sentry_kafka_schemas.codecs import Codec

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


def _map_arrow_to_polars_dtype(pa_type: PADataType) -> PolarsDataType:
    """
    Convert pyarrow type to Polars data type.
    adapted from pyarrow to spark conversion - https://github.com/apache/spark/blob/master/python/pyspark/sql/pandas/types.py
    """
    if types.is_boolean(pa_type):
        return pl.Boolean
    elif types.is_int8(pa_type):
        return pl.Int8
    elif types.is_int16(pa_type):
        return pl.Int16
    elif types.is_int32(pa_type):
        return pl.Int32
    elif types.is_int64(pa_type):
        return pl.Int64
    elif types.is_uint8(pa_type):
        return pl.UInt8
    elif types.is_uint16(pa_type):
        return pl.UInt16
    elif types.is_uint32(pa_type):
        return pl.UInt32
    elif types.is_uint64(pa_type):
        return pl.UInt64
    elif types.is_float32(pa_type):
        return pl.Float32
    elif types.is_float64(pa_type):
        return pl.Float64
    elif types.is_decimal(pa_type):
        return pl.Float64
    elif types.is_string(pa_type) or types.is_large_string(pa_type):
        return pl.Utf8
    elif (
        types.is_binary(pa_type)
        or types.is_large_binary(pa_type)
        or types.is_fixed_size_binary(pa_type)
    ):
        return pl.Binary
    elif types.is_date32(pa_type):
        return pl.Date
    elif types.is_date64(pa_type):
        return pl.Datetime(time_unit="us", time_zone="UTC")  # is this ok?
    elif types.is_timestamp(pa_type):
        return pl.Datetime(time_unit="us", time_zone="UTC")
    elif types.is_duration(pa_type):
        return pl.Duration("ns")  # Defaulting to nanoseconds
    elif (
        types.is_list(pa_type) or types.is_large_list(pa_type) or types.is_fixed_size_list(pa_type)
    ):
        assert isinstance(pa_type, ListType)
        return pl.List(_map_arrow_to_polars_dtype(pa_type.value_type))
    elif types.is_struct(pa_type):
        assert isinstance(pa_type, StructType)
        fields = [pl.Field(field.name, _map_arrow_to_polars_dtype(field.type)) for field in pa_type]
        return pl.Struct(fields)
    elif types.is_dictionary(pa_type):
        assert isinstance(pa_type, DictionaryType)
        return _map_arrow_to_polars_dtype(pa_type.value_type)
    elif types.is_null(pa_type):
        return pl.Null
    else:
        raise TypeError(f"Unsupported Arrow type for conversion: {pa_type}")


def _map_arrow_to_polars_schema(schema: Sequence[Tuple[str, PADataType]]) -> PolarsSchema:
    return pl.Schema(
        {field_schema[0]: _map_arrow_to_polars_dtype(field_schema[1]) for field_schema in schema}
    )


def _get_parquet(msg: Message[Any], schema_fields: PolarsSchema) -> bytes:
    df = pl.DataFrame(
        [i for i in msg.payload if i is not None],
        schema=schema_fields,
    )
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="lz4", statistics=False, use_pyarrow=False)
    return bytes(buffer.getvalue())


def parquet_serializer(
    msg: Message[Any], schema_fields: Union[Sequence[Tuple[str, PADataType]], PolarsSchema]
) -> bytes:
    if isinstance(schema_fields, PolarsSchema):
        return _get_parquet(msg, schema_fields)
    else:
        polars_schema = _map_arrow_to_polars_schema(schema_fields)
        return _get_parquet(msg, polars_schema)
