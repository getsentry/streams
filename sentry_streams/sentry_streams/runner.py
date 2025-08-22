import importlib
import json
import logging
import signal
from typing import Any, Optional, cast

import click
import jsonschema
import yaml

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    RuntimeTranslator,
    StreamSinkT,
    StreamT,
)
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    WithInput,
)

logger = logging.getLogger(__name__)


def iterate_edges(
    p_graph: Pipeline[Any], translator: RuntimeTranslator[StreamT, StreamSinkT]
) -> None:
    """
    Traverses over edges in a PipelineGraph, building the
    stream incrementally by applying steps and transformations
    It currently has the structure to deal with, but has no
    real support for, fan-in streams
    """

    step_streams = {}

    logger.info(f"Apply source: {p_graph.root.name}")
    source_streams = translator.translate_step(p_graph.root)
    for source_name in source_streams:
        step_streams[source_name] = source_streams[source_name]

    while step_streams:
        for input_name in list(step_streams):
            output_steps = p_graph.outgoing_edges[input_name]
            input_stream = step_streams.pop(input_name)

            if not output_steps:
                continue

            for output in output_steps:
                next_step: WithInput[Any] = cast(WithInput[Any], p_graph.steps[output])
                # TODO: Make the typing align with the streams being iterated through. Reconsider algorithm as needed.
                next_step_stream = translator.translate_step(next_step, input_stream)  # type: ignore
                for branch_name in next_step_stream:
                    step_streams[branch_name] = next_step_stream[branch_name]


def runner(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    pipeline_globals: dict[str, Any] = {}

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with open(application) as f:
        exec(f.read(), pipeline_globals)

    with open(config, "r") as config_file:
        environment_config = yaml.safe_load(config_file)

    config_template = importlib.resources.files("sentry_streams") / "config.json"
    with config_template.open("r") as file:
        schema = json.load(file)

        try:
            jsonschema.validate(environment_config, schema)
        except Exception:
            raise

    assigned_segment_id = int(segment_id) if segment_id else None
    pipeline: Pipeline[Any] = pipeline_globals["pipeline"]
    runtime: Any = load_adapter(adapter, environment_config, assigned_segment_id)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Signal received, terminating the runner...")
        runtime.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    runtime.run()


@click.command()
@click.option(
    "--name",
    "-n",
    default="Sentry Streams",
    show_default=True,
    help="The name of the Sentry Streams application",
)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    show_default=True,
    help="Set the logging level",
)
@click.option(
    "--adapter",
    "-a",
    # remove choices list in the future when custom local adapters are widely used
    # for now just arroyo and rust_arroyo will be commonly used
    type=click.Choice(["arroyo", "rust_arroyo", "grpc_worker", "flink_pipeline"]),
    # TODO: Remove the support for dynamically load the class.
    # Add a runner CLI in the flink package instead that instantiates
    # the Flink adapter.
    help=(
        "The stream adapter to instantiate. It can be one of the allowed values from "
        "the load_adapter function"
    ),
)
@click.option(
    "--config",
    required=True,
    help=(
        "The deployment config file path. Each config file currently corresponds to a specific pipeline."
    ),
)
@click.option(
    "--segment-id",
    "-s",
    type=str,
    help="The segment id to run the pipeline for",
)
@click.argument(
    "application",
    required=True,
)
def main(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    runner(name, log_level, adapter, config, segment_id, application)


if __name__ == "__main__":
    main()
