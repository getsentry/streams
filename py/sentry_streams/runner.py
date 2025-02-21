import argparse
import os
from typing import Any, cast

from pyflink.datastream import StreamExecutionEnvironment

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

    libs_path = os.environ.get("FLINK_LIBS")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    if libs_path is not None:
        # If the libraries path is provided load the
        jar_file = os.path.join(
            os.path.abspath(libs_path), "flink-sql-connector-kafka-3.4.0-1.20.jar"
        )

        env.add_jars(f"file://{jar_file}")

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": args.broker,
    }

    pipeline: Pipeline = pipeline_globals["pipeline"]
    # This will not be harcdoded in the future
    runtime_config: StreamAdapter = FlinkAdapter(environment_config, env)
    translator = RuntimeTranslator(runtime_config)

    iterate_edges(pipeline, translator)

    # submit for execution
    env.execute(job_name=args.name)


if __name__ == "__main__":
    main()
