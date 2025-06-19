from typing import Any, Callable, Optional, Self, Sequence, TypeVar, cast

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Branch,
    Broadcast,
    ComplexStep,
    Filter,
    FlatMap,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Step,
    WithInput,
)
from sentry_streams.pipeline.window import MeasurementUnit

DummyInput = TypeVar("DummyInput")
DummyOutput = TypeVar("DummyOutput")


class DummyAdapter(StreamAdapter[DummyInput, DummyOutput]):
    """
    An infinitely scalable adapter that throws away all the data it gets.
    The adapter tracks the 'streams' that each step of iterate_edges() returns in the form of
    lists of previous step names.
    """

    def __init__(self, _: PipelineConfig) -> None:
        self.input_streams: list[str] = []
        self.branches: list[str] = []

    def complex_step_override(self) -> dict[str, Callable[[ComplexStep], Any]]:
        return {}

    def track_input_streams(
        self, step: WithInput, branches: Optional[Sequence[Branch]] = None
    ) -> None:
        self.input_streams.append(step.name)
        if branches:
            self.branches.extend(branch.name for branch in branches)

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls(config)

    def source(self, step: Step) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def sink(self, step: Sink, stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def map(self, step: Map, stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def filter(self, step: Filter, stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def reduce(self, step: Reduce[MeasurementUnit, InputType, OutputType], stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def flat_map(self, step: FlatMap, stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        return self

    def broadcast(self, step: Broadcast, stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step), [cast(Branch, r.root) for r in step.routes])
        ret = {}
        for segment_branch in step.routes:
            assert segment_branch.root is not None
            self.branches.append(segment_branch.root.name)
            ret[segment_branch.root.name] = segment_branch
        return ret

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> Any:
        self.track_input_streams(cast(WithInput, step))
        ret = {}
        for branch in step.routing_table.values():
            assert branch.root is not None
            self.branches.append(branch.root.name)
            ret[branch.root.name] = branch
        return ret

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass
