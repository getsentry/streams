import sys
from typing import Any, cast

from sentry_streams.adapters.stream_adapter import RuntimeTranslator, StreamAdapter
from sentry_streams.flink.flink_adapter import FlinkAdapter
from sentry_streams.pipeline import (
    Pipeline,
    WithInput,
)


def iterate_edges(p_graph: Pipeline, translator: RuntimeTranslator) -> None:
    """
    Traverses over edges in a PipelineGraph, building the
    stream incrementally by applying steps and transformations
    It currently has the structure to deal with, but has no
    real support for, fan-in and fan-out streams
    """

    step_streams = {}

    for source in p_graph.sources:
        print(f"Apply source: {source.name}")
        source_stream = translator.translate_step(source)
        step_streams[source.name] = source_stream

        while step_streams:
            for input_name in list(step_streams):
                output_steps = p_graph.outgoing_edges[input_name]
                input_stream = step_streams.pop(input_name)

                if not output_steps:
                    continue

                # check if the inputs are fanning out
                if len(output_steps) > 1:
                    pass

                else:
                    output_step_name = output_steps.pop()

                    # check if the inputs are fanning in
                    if len(p_graph.incoming_edges[output_step_name]) > 1:
                        pass

                    # 1:1 between input and output stream
                    else:
                        next_step: WithInput = cast(WithInput, p_graph.steps[output_step_name])
                        print(f"Apply step: {next_step.name}")
                        next_step_stream = translator.translate_step(next_step, input_stream)
                        step_streams[next_step.name] = next_step_stream


def main() -> None:
    pipeline_globals: dict[str, Any] = {}

    with open(sys.argv[1]) as f:
        exec(f.read(), pipeline_globals)

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": "localhost:9092",
    }

    pipeline: Pipeline = pipeline_globals["pipeline"]
    runtime: StreamAdapter = FlinkAdapter.build(environment_config)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    runtime.run()


if __name__ == "__main__":
    main()
