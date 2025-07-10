from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.rust_function_protocol import (
    RustFilterFunction,
    RustMapFunction,
)

class TestMessage: ...

class TestFilterCorrect(RustFilterFunction[TestMessage]):
    def __call__(self, msg: Message[TestMessage]) -> bool: ...

class TestMapCorrect(RustMapFunction[TestMessage, str]):
    def __call__(self, msg: Message[TestMessage]) -> Message[str]: ...

class TestMapWrongType(RustMapFunction[bool, str]):
    def __call__(self, msg: Message[bool]) -> Message[str]: ...

class TestMapString(RustMapFunction[str, int]):
    def __call__(self, msg: Message[str]) -> Message[int]: ...
