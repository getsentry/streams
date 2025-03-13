from typing import Any, List, MutableMapping, Self, TypeVar

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter
from sentry_streams.pipeline.function_template import (
    InputType,
    OutputType,
)
from sentry_streams.pipeline.pipeline import (
    Filter,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
    Sink,
    Step,
    StepType,
)
from sentry_streams.pipeline.window import MeasurementUnit

DummyInput = TypeVar("DummyInput")
DummyOutput = TypeVar("DummyOutput")


class DummyAdapter(StreamAdapter[DummyInput, DummyOutput]):
    """
    An infinitely scalable adapter that throws away all the data it gets.
    The adapter tracks the 'streams' that each step returns in the form of
    lists of previous step names.
    """

    def __init__(self, _: PipelineConfig) -> None:
        self.input_streams: MutableMapping[str, List[str]] = {}

    def add_step_input_streams(self, step: Any) -> None:
        # TODO: update to support multiple inputs to a step
        # once we implement Union

        input_step = step.inputs[0]
        input_step_name = input_step.name
        input_step_stream = self.input_streams[input_step_name]
        self.input_streams[step.name] = input_step_stream + [input_step_name]
        # if step is a Router, also add its Branch nodes to input_streams
        if step.step_type == StepType.ROUTER:
            for branch in step.routing_table.values():
                self.input_streams[branch.name] = self.input_streams[step.name] + [step.name]
        print(f"{step.name=}")
        print(self.input_streams, "\n")

    @classmethod
    def build(cls, config: PipelineConfig) -> Self:
        return cls(config)

    def source(self, step: Step) -> Any:
        self.input_streams[step.name] = []
        return self

    def sink(self, step: Sink, stream: Any) -> Any:
        self.add_step_input_streams(step)
        return self

    def map(self, step: Map, stream: Any) -> Any:
        self.add_step_input_streams(step)
        return self

    def filter(self, step: Filter, stream: Any) -> Any:
        self.add_step_input_streams(step)
        return self

    def reduce(self, step: Reduce[MeasurementUnit, InputType, OutputType], stream: Any) -> Any:
        self.add_step_input_streams(step)
        return self

    def router(self, step: Router[RoutingFuncReturnType], stream: Any) -> Any:
        self.add_step_input_streams(step)
        return {branch.name: branch for branch in step.routing_table.values()}

    def run(self) -> None:
        pass
