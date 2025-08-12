"""
Protocol definitions for Rust functions to integrate with Python's type system
"""

from typing import Protocol, TypeVar, runtime_checkable

from sentry_streams.pipeline.message import Message

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput", covariant=True)


# External interface that we need users to use in their stubs for type-safety
@runtime_checkable
class RustFunction(Protocol[TInput, TOutput]):
    def __call__(self, msg: Message[TInput]) -> TOutput: ...


# Methods that we use internally, but don't want the user to see (or have to write out in their stubfiles)
class InternalRustFunction(RustFunction[TInput, TOutput], Protocol):
    def input_type(self) -> str: ...
    def output_type(self) -> str: ...
    def rust_function_version(self) -> int: ...
