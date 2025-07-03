from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import tzinfo
from typing import Literal, Optional, Sequence, TypeAlias, Union

import polars as pl

PolarsDataType: TypeAlias = Union[type[pl.DataType], pl.DataType]


class DataType(ABC):
    @abstractmethod
    def resolve(self) -> PolarsDataType:
        raise NotImplementedError


class Boolean(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Boolean


class Int8(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Int8


class Int16(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Int16


class Int32(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Int32


class Int64(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Int64


class Uint8(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.UInt8


class Uint16(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.UInt16


class Uint32(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.UInt32


class Uint64(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.UInt64


class Float32(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Float32


class Float64(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Float64


class String(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.String


class Utf8(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Utf8


class Binary(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Binary


class Date(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Date


class Null(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Null


class Decimal(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Decimal


class Time(DataType):
    def resolve(self) -> type[pl.DataType]:
        return pl.Time


TimeUnit: TypeAlias = Literal["ns", "us", "ms"]


@dataclass
class Datetime(DataType):
    time_zone: str | tzinfo | None
    time_unit: Optional[TimeUnit] = "us"

    def resolve(self) -> pl.DataType:
        assert self.time_unit is not None
        return pl.Datetime(self.time_unit, self.time_zone)


@dataclass
class List(DataType):
    inner: DataType

    def resolve(self) -> pl.DataType:
        return pl.List(self.inner.resolve())


@dataclass
class Duration(DataType):
    time_unit: Optional[TimeUnit] = "us"

    def resolve(self) -> pl.DataType:
        assert self.time_unit is not None
        return pl.Duration(self.time_unit)


@dataclass
class Field:
    """
    Only to be used inside Mappings
    """

    name: str
    dtype: DataType

    def resolve(self) -> pl.Field:
        return pl.Field(self.name, self.dtype.resolve())


@dataclass
class Struct(DataType):
    fields: Sequence[Field]

    def resolve(self) -> pl.Struct:
        return pl.Struct([field.resolve() for field in self.fields])
