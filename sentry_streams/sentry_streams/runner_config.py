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


@click.command()
@click.option(
    "--name",
    "-n",
    default="Sentry Streams Config",
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
    "--pipeline-structure",
    required=True,
    help=(
        "The YAML config file path containing the pipeline definition from DumpingConsumer.dump()."
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
def runner_config(
    name: str,
    log_level: str,
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

    # Load the pipeline definition from the Python file
    with open(application) as f:
        exec(f.read(), pipeline_globals)

    # Load the YAML config file containing the pipeline definition from DumpingConsumer.dump()
    with open(config, "r") as config_file:
        pipeline_config = yaml.safe_load(config_file)

    pipeline: Pipeline[Any] = pipeline_globals["pipeline"]
    
    # TODO: Now you have the YAML file that represent the topology of the pipeline.
    # This would allow you to build the Rust consumer except that the YAML file
    # cannot contain the Python application functions to be passed to the Rust consumer.
    # Those have to be loaded dynamically and added to the Consumer in Rust via
    # RuntimeOperator.
    # Whether this runner is implemented in Python or Rust python code has to be executed
    # to obtain references to those functions. Which is fine as they are used only in
    # hybrid or Python pipelines so we are already executing in a python interpreter there.
    #
    # The Python application functions can be loaded from the pipeline graph `pipeline_globals`.
    # They are referenced in each step. The YAML file contains the name opf the step for each step.


def main(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    runner_config(name, log_level, adapter, config, segment_id, application)


if __name__ == "__main__":
    main() 