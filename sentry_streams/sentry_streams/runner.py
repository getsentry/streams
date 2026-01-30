import importlib
import json
import logging
import multiprocessing
import sys
from typing import Any, Mapping, Optional, cast

import click
import jsonschema
import sentry_sdk
import yaml

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    RuntimeTranslator,
    StreamSinkT,
    StreamT,
)
from sentry_streams.metrics import (
    DatadogMetricsBackend,
    DummyMetricsBackend,
    configure_metrics,
)
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    WithInput,
)
from sentry_streams.pipeline.validation import validate_all_branches_have_sinks

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


def _load_pipeline(application: str) -> Pipeline[Any]:
    """
    Worker function that runs in a separate process to load the pipeline.
    Returns the Pipeline object directly, or raises an exception on error.

    Customer code exceptions are allowed to propagate naturally so that the customer's
    Sentry SDK (if initialized) can capture them.
    """
    import contextlib

    pipeline_globals: dict[str, Any] = {}

    with contextlib.redirect_stdout(sys.stderr):
        with open(application, "r") as f:
            exec(f.read(), pipeline_globals)

    if "pipeline" not in pipeline_globals:
        raise ValueError("Application file must define a 'pipeline' variable")

    pipeline = cast(Pipeline[Any], pipeline_globals["pipeline"])
    return pipeline


def load_runtime(
    name: str,
    log_level: str,
    adapter: str,
    segment_id: Optional[str],
    application: str,
    environment_config: Mapping[str, Any],
) -> Any:

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with multiprocessing.Pool(processes=1) as pool:
            pipeline: Pipeline[Any] = pool.apply(_load_pipeline, (application,))
            logger.info("Successfully loaded pipeline from subprocess")
    except Exception:
        raise
    validate_all_branches_have_sinks(pipeline)

    metric_config = environment_config.get("metrics", {})
    if metric_config.get("type") == "datadog":
        default_tags = metric_config.get("tags", {})
        default_tags["pipeline"] = name

        metrics = DatadogMetricsBackend(
            metric_config["host"],
            metric_config["port"],
            "sentry_streams",
            default_tags,
        )
        configure_metrics(metrics)
        metric_config = {
            "host": metric_config["host"],
            "port": metric_config["port"],
            "tags": default_tags,
        }
    else:
        configure_metrics(DummyMetricsBackend())
        metric_config = {}

    assigned_segment_id = int(segment_id) if segment_id else None
    runtime: Any = load_adapter(adapter, environment_config, assigned_segment_id, metric_config)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    return runtime


def run_with_config_file(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    with open(config, "r") as f:
        environment_config = yaml.safe_load(f)

    config_template = importlib.resources.files("sentry_streams") / "config.json"
    with config_template.open("r") as file:
        schema = json.load(file)

        try:
            jsonschema.validate(environment_config, schema)
        except Exception:
            raise

    streaming_platform_config = environment_config.get("streaming_platform_config")
    if streaming_platform_config:
        sentry_sdk.init(dsn=streaming_platform_config["dsn"])

    runtime = load_runtime(name, log_level, adapter, segment_id, application, environment_config)
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
    type=click.Choice(["arroyo", "rust_arroyo"]),
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
def run_with_cli(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    run_with_config_file(name, log_level, adapter, config, segment_id, application)


if __name__ == "__main__":
    run_with_cli()
