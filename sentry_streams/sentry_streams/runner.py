import argparse
from typing import Any, cast

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    RuntimeTranslator,
    Stream,
    StreamSink,
)
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    WithInput,
)


def iterate_edges(p_graph: Pipeline, translator: RuntimeTranslator[Stream, StreamSink]) -> None:
    """
    Traverses over edges in a PipelineGraph, building the
    stream incrementally by applying steps and transformations
    It currently has the structure to deal with, but has no
    real support for, fan-in streams
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

                for output in output_steps:
                    next_step: WithInput = cast(WithInput, p_graph.steps[output])
                    print(f"Apply step: {next_step.name}")
                    # TODO: Make the typing align with the streams being iterated through. Reconsider algorithm as needed.
                    next_step_stream = translator.translate_step(next_step, input_stream)  # type: ignore
                    step_streams[next_step.name] = next_step_stream


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs a Flink application.")
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        default="Flink Job",
        help="The name of the Flink Job",
    )
    parser.add_argument(
        "--broker",
        "-b",
        type=str,
        default="kafka:9093",
        help="The broker the job should connect to",
    )
    parser.add_argument(
        "--adapter",
        "-a",
        type=str,
        # TODO: Remove the support for dynamically load the class.
        # Add a runner CLI in the flink package instead that instantiates
        # the Flink adapter.
        help=(
            "The stream adapter to instantiate. It can be a value from "
            "the AdapterType enum or a fully qualified class name to "
            "load dynamically"
        ),
    )
    parser.add_argument(
        "application",
        type=str,
        help=(
            "The Sentry Stream application file. This has to be relative "
            "to the path mounted in the job manager as the /apps directory."
        ),
    )

    pipeline_globals: dict[str, Any] = {}

    args = parser.parse_args()

    with open(args.application) as f:
        exec(f.read(), pipeline_globals)

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
            "transformed-events-2": "transformed-events-2",
        },
        "broker": args.broker,
    }

    pipeline: Pipeline = pipeline_globals["pipeline"]
    runtime: Any = load_adapter(args.adapter, environment_config)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    runtime.run()


if __name__ == "__main__":
    main()
