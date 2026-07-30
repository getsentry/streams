from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    Optional,
    Self,
    Type,
    TypeVar,
    Union,
    assert_never,
)

from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
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
    Step,
    StepType,
)
from sentry_streams.pipeline.window import MeasurementUnit

PipelineConfig = Mapping[str, Any]


StreamT = TypeVar("StreamT")
StreamSinkT = TypeVar("StreamSinkT")


class RuntimeStateError(RuntimeError):
    """
    Raised when an operation conflicts with the runtime's lifecycle state.
    """


class RuntimeState(StrEnum):
    IDLE = "idle"
    STARTING = "starting"
    CONSUMING = "consuming"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"

    @property
    def is_terminal(self) -> bool:
        return self in (RuntimeState.STOPPED, RuntimeState.ERRORED)

    @property
    def rank(self) -> int:
        if self is RuntimeState.IDLE:
            return 0
        if self is RuntimeState.STARTING:
            return 1
        if self is RuntimeState.CONSUMING:
            return 2
        if self is RuntimeState.STOPPING:
            return 3
        return 4

    def can_transition_to(self, state: RuntimeState) -> bool:
        return self.rank < state.rank


@dataclass(frozen=True)
class StartOptions:
    group_instance_id: str | None = None


@dataclass(frozen=True)
class RuntimeStatus:
    state: RuntimeState
    error: Exception | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state.is_terminal

    def as_dict(self) -> dict[str, str | None]:
        return {
            "state": self.state.value,
            "error": str(self.error) if self.error is not None else None,
        }


class StreamAdapter(ABC, Generic[StreamT, StreamSinkT]):
    """
    A generic adapter for mapping sentry_streams APIs
    and primitives to runtime-specific ones. This can
    be extended to specific runtimes.
    """

    def __init__(self) -> None:
        self.__status_lock = threading.Lock()
        self.__status = RuntimeStatus(RuntimeState.IDLE)

    @property
    def status(self) -> RuntimeStatus:
        with self.__status_lock:
            return self.__status

    def _set_status(self, state: RuntimeState, error: Exception | None = None) -> None:
        with self.__status_lock:
            if not self.__status.state.can_transition_to(state):
                return
            self.__status = RuntimeStatus(state, error)

    def begin_start(self) -> RuntimeStatus:
        with self.__status_lock:
            state = self.__status.state
            if state.is_terminal:
                raise RuntimeStateError(f"cannot restart runtime that is {state}")
            if state is not RuntimeState.IDLE:
                raise RuntimeStateError(f"cannot start runtime while it is {state}")
            self.__status = RuntimeStatus(RuntimeState.STARTING)
            return self.__status

    def run(self, options: StartOptions | None = None) -> None:
        state = self.status.state
        if state is RuntimeState.STOPPING:
            self._set_status(RuntimeState.STOPPED)
            return
        if state is not RuntimeState.STARTING:
            raise RuntimeStateError(f"cannot run runtime while it is {state}")

        try:
            self._run(options if options is not None else StartOptions())
        except Exception as exc:
            self._set_status(RuntimeState.ERRORED, exc)
            raise
        else:
            self._set_status(RuntimeState.STOPPED)

    def shutdown(self) -> RuntimeStatus:
        with self.__status_lock:
            state = self.__status.state
            if state is RuntimeState.STOPPING or state.is_terminal:
                return self.__status
            self.__status = RuntimeStatus(
                RuntimeState.STOPPED if state is RuntimeState.IDLE else RuntimeState.STOPPING
            )

        self._shutdown()
        return self.status

    @classmethod
    @abstractmethod
    def build(cls, config: PipelineConfig) -> Self:
        """
        Create an adapter and instantiate the runtime specific context.

        This method exists so that we can define the type of the
        Pipeline config.

        Pipeline config contains the fields needed to instantiate the
        pipeline.
        #TODO: Provide a more structured way to represent config.
        # currently we rely on the adapter to validate the content while
        # there are a lot of configuration elements that can be adapter
        # agnostic.
        """
        raise NotImplementedError

    @abstractmethod
    def complex_step_override(
        self,
    ) -> dict[Type[ComplexStep[Any, Any]], Callable[[ComplexStep[Any, Any]], StreamT]]:
        """
        Allows an adapter to directly handle certain complex steps, instead of converting them to simple steps. The keys of the dict should be
        the class of the specific step being handled.
        """
        raise NotImplementedError

    @abstractmethod
    def source(self, step: Source[Any]) -> StreamT:
        """
        Builds a stream source for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def sink(self, step: Sink[Any], stream: StreamT) -> StreamSinkT:
        """
        Builds a stream sink for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def map(self, step: Map[Any, Any], stream: StreamT) -> StreamT:
        """
        Builds a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def flat_map(self, step: FlatMap[Any, Any], stream: StreamT) -> StreamT:
        """
        Builds a flat-map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def filter(self, step: Filter[Any], stream: StreamT) -> StreamT:
        """
        Builds a filter operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def reduce(
        self,
        step: Reduce[MeasurementUnit, InputType, OutputType],
        stream: StreamT,
    ) -> StreamT:
        """
        Build a map operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def router(
        self,
        step: Router[RoutingFuncReturnType, Any],
        stream: StreamT,
    ) -> Mapping[str, StreamT]:
        """
        Build a router operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def broadcast(
        self,
        step: Broadcast[Any],
        stream: StreamT,
    ) -> Mapping[str, StreamT]:
        """
        Build a broadcast operator for the platform the adapter supports.
        """
        raise NotImplementedError

    @abstractmethod
    def _run(self, options: StartOptions) -> None:
        """
        Starts the pipeline
        """
        raise NotImplementedError

    @abstractmethod
    def _shutdown(self) -> None:
        """
        Cleanly shutdown the application.
        """
        raise NotImplementedError


class RuntimeTranslator(Generic[StreamT, StreamSinkT]):
    """
    A runtime-agnostic translator
    which can apply the physical steps and transformations
    to a stream. Uses a StreamAdapter to determine
    which underlying runtime to translate to.
    """

    def __init__(self, runtime_adapter: StreamAdapter[StreamT, StreamSinkT]):
        self.adapter = runtime_adapter

    def translate_step(
        self, step: Step, stream: Optional[StreamT] = None
    ) -> Mapping[str, Union[StreamT, StreamSinkT]]:
        step_name = step.name
        if isinstance(step, ComplexStep):
            overrides = self.adapter.complex_step_override()
            if step.__class__ in overrides:
                return {step_name: overrides[step.__class__](step)}
            else:
                step = step.convert()

        assert hasattr(step, "step_type")
        step_type = step.step_type

        if step_type is StepType.SOURCE:
            assert isinstance(step, Source)
            return {step_name: self.adapter.source(step)}

        elif step_type is StepType.SINK:
            assert isinstance(step, Sink) and stream is not None
            return {step_name: self.adapter.sink(step, stream)}

        elif step_type is StepType.MAP:
            assert isinstance(step, Map) and stream is not None
            return {step_name: self.adapter.map(step, stream)}

        elif step_type is StepType.FLAT_MAP:
            assert isinstance(step, FlatMap) and stream is not None
            return {step_name: self.adapter.flat_map(step, stream)}

        elif step_type is StepType.REDUCE:
            assert isinstance(step, Reduce) and stream is not None
            return {step_name: self.adapter.reduce(step, stream)}

        elif step_type is StepType.FILTER:
            assert stream is not None
            assert isinstance(step, Filter), f"Expected a Filter step, got {type(step)}"
            return {step_name: self.adapter.filter(step, stream)}

        elif step_type is StepType.ROUTER:
            assert isinstance(step, Router) and stream is not None
            return self.adapter.router(step, stream)

        elif step_type is StepType.BROADCAST:
            assert isinstance(step, Broadcast) and stream is not None
            return self.adapter.broadcast(step, stream)

        else:
            assert_never(step_type)
