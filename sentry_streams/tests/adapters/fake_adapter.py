from __future__ import annotations

import threading
from typing import Any, Callable, Mapping, Self, Type

from sentry_streams.adapters.stream_adapter import (
    PipelineConfig,
    RuntimeState,
    StreamAdapter,
)
from sentry_streams.pipeline.function_template import InputType, OutputType
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    ComplexStep,
    Filter,
    FlatMap,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Source,
)
from sentry_streams.pipeline.window import MeasurementUnit


class FakeAdapter(StreamAdapter[Any, Any]):
    def __init__(self, fail: bool = False, block_before_consuming: bool = False) -> None:
        super().__init__()
        self._fail = fail
        self._block_before_consuming = block_before_consuming
        self._stop = threading.Event()
        self.allow_consume = threading.Event()
        self.run_started = threading.Event()
        self.run_finished = threading.Event()
        self.run_calls = 0
        self.shutdown_calls = 0

    def _run(self) -> None:
        self.run_calls += 1
        self.run_started.set()
        if self._fail:
            raise RuntimeError("runtime failed")
        if self._block_before_consuming:
            assert self.allow_consume.wait(3.0)
        self._set_status(RuntimeState.CONSUMING)
        self._stop.wait()
        self.run_finished.set()

    def _shutdown(self) -> None:
        self.shutdown_calls += 1
        self._stop.set()

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls()

    def complex_step_override(
        self,
    ) -> dict[Type[ComplexStep[Any, Any]], Callable[[ComplexStep[Any, Any]], Any]]:
        return {}

    def source(self, step: Source[Any]) -> Any:
        raise NotImplementedError

    def sink(self, step: Sink[Any], stream: Any) -> Any:
        raise NotImplementedError

    def map(self, step: Map[Any, Any], stream: Any) -> Any:
        raise NotImplementedError

    def flat_map(self, step: FlatMap[Any, Any], stream: Any) -> Any:
        raise NotImplementedError

    def filter(self, step: Filter[Any], stream: Any) -> Any:
        raise NotImplementedError

    def reduce(self, step: Reduce[MeasurementUnit, InputType, OutputType], stream: Any) -> Any:
        raise NotImplementedError

    def router(self, step: Router[RoutingFuncReturnType, Any], stream: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    def broadcast(self, step: Broadcast[Any], stream: Any) -> Mapping[str, Any]:
        raise NotImplementedError
