from typing import Any, MutableMapping, Optional, Self, Sequence, Set, TypeVar

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Branch,
    Broadcast,
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
        self.input_streams: MutableMapping[str, Set[str]] = {}

    def track_input_streams(
        self, step: WithInput, branches: Optional[Sequence[Branch]] = None
    ) -> None:
        """
        Tracks the streams that each step receives as input.
        This can be used in tests to verify that steps downstream from a split in
        the stream (such as a Router) are being applied to the correct stream.

        For example, if we have:
        Source --> Router --> Branch1 --> Map1
                        |
                        --> Branch2 --> Map2

        We can verify that:
        input_streams["Map1"] == ["Source", "Router", "Branch1"]
        input_streams["Map2"] == ["Source", "Router", "Branch2"]
        """
        # TODO: update to support multiple inputs to a step
        # once we implement Union
        assert (
            len(step.inputs) == 1
        ), "Only steps with a single input are supported for DummyAdapter."

        input_step = step.inputs[0]
        input_step_name = input_step.name
        input_step_stream = self.input_streams[input_step_name]
        self.input_streams[step.name] = input_step_stream.union({input_step_name})

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls(config)

    def source(self, step: Step) -> Any:
        self.input_streams[step.name] = set()
        return self

    def sink(self, step: Sink, stream: Any) -> Any:
        self.track_input_streams(step)
        return self

    def map(self, step: Map, stream: Any) -> Any:
        self.track_input_streams(step)
        return self

    def filter(self, step: Filter, stream: Any) -> Any:
        self.track_input_streams(step)
        return self

    def reduce(self, step: Reduce[MeasurementUnit, InputType, OutputType], stream: Any) -> Any:
        self.track_input_streams(step)
        return self

    def flat_map(self, step: FlatMap, stream: Any) -> Any:
        self.track_input_streams(step)
        return self

    def broadcast(self, step: Broadcast, stream: Any) -> Any:
        self.track_input_streams(step, step.routes)
        for branch in step.routes:
            self.input_streams[branch.name] = self.input_streams[step.name].union({step.name})
        return {branch.name: branch for branch in step.routes}

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> Any:
        self.track_input_streams(step)
        for branch in step.routing_table.values():
            self.input_streams[branch.name] = self.input_streams[step.name].union({step.name})
        return {branch.name: branch for branch in step.routing_table.values()}

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass
