"""
Protocol definitions for Rust functions to integrate with Python's type system
"""

from typing import Protocol, TypeVar, runtime_checkable

from sentry_streams.pipeline.message import Message

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


@runtime_checkable
class RustFilterFunction(Protocol[TInput]):
    """Protocol for Rust filter functions"""

    def __call__(self, msg: Message[TInput]) -> bool:
        """Filter function that returns True to keep the message"""
        ...


@runtime_checkable
class RustMapFunction(Protocol[TInput, TOutput]):
    """Protocol for Rust map functions"""

    def __call__(self, msg: Message[TInput]) -> Message[TOutput]:
        """Map function that transforms the message"""
        ...
