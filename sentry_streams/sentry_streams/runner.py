import argparse
import logging
import signal
from typing import Any, cast

import yaml

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

logger = logging.getLogger(__name__)


def iterate_edges(p_graph: Pipeline, translator: RuntimeTranslator[Stream, StreamSink]) -> None:
    """
    Traverses over edges in a PipelineGraph, building the
    stream incrementally by applying steps and transformations
    It currently has the structure to deal with, but has no
    real support for, fan-in streams
    """

    step_streams = {}

    for source in p_graph.sources:
        logger.info(f"Apply source: {source.name}")
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
                    logger.info(f"Apply step: {next_step.name}")
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
        "--log-level",
        "-l",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level",
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
        "--config",
        type=str,
        help=(
            "The deployment config file path. Each config file currently corresponds to a specific pipeline."
        ),
    )
    parser.add_argument(
        "-c",
        action="store_true",
        help=(
            "Run the application in container mode (i.e non-dev/locally). Right now this is only supported by Flink."
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

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(args.application) as f:
        exec(f.read(), pipeline_globals)

    with open(args.config, "r") as config_file:
        environment_config = yaml.safe_load(config_file)

    if args.c:
        # TODO: Make this configurable by runtime eventually
        flink_config = environment_config["flink"]
        environment_config["pipeline"].update(flink_config)

    pipeline: Pipeline = pipeline_globals["pipeline"]
    runtime: Any = load_adapter(args.adapter, environment_config)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Signal received, terminating the runner...")
        runtime.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    runtime.run()


if __name__ == "__main__":
    main()
