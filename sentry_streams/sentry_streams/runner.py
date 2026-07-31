import logging
import multiprocessing
import signal
import sys
import threading
from http.server import ThreadingHTTPServer
from types import FrameType
from typing import Any, Callable, Mapping, Optional, cast

import click
import sentry_sdk

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    RuntimeState,
    RuntimeStatus,
    RuntimeTranslator,
    StreamAdapter,
    StreamSinkT,
    StreamT,
)
from sentry_streams.control import PipelineController
from sentry_streams.metrics import (
    DatadogMetricsConfig,
    MetricsConfig,
    configure_metrics,
)
from sentry_streams.pipeline.config import load_config
from sentry_streams.pipeline.pipeline import (
    Pipeline,
    WithInput,
)
from sentry_streams.pipeline.validation import validate_all_branches_have_sinks
from sentry_streams.server.control_server import make_server
from sentry_streams.server.control_server import serve as serve_control_server

logger = logging.getLogger(__name__)

SHUTDOWN_TIMEOUT_SEC = 60.0


def _install_signal_handlers(shutdown_requested: threading.Event) -> None:
    """Turn SIGINT/SIGTERM into a shutdown request."""

    def _handle_termination(signum: int, _frame: FrameType | None) -> None:
        logger.info("received signal %d; requesting pipeline shutdown", signum)
        shutdown_requested.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, _handle_termination)


def _raise_on_error(snapshot: RuntimeStatus) -> None:
    if snapshot.state is RuntimeState.ERRORED:
        if snapshot.error is not None:
            raise snapshot.error
        raise RuntimeError("pipeline run loop failed")


def _run_pipeline(
    controller: PipelineController,
    shutdown_requested: threading.Event,
    server: ThreadingHTTPServer | None = None,
    serve: Callable[[ThreadingHTTPServer], None] = serve_control_server,
) -> RuntimeStatus:
    def _stop_on_shutdown_request() -> None:
        shutdown_requested.wait()
        controller.request_stop()

    shutdown_thread = threading.Thread(
        target=_stop_on_shutdown_request,
        name="pipeline-signal",
        daemon=False,
    )
    shutdown_thread.start()

    serve_thread: threading.Thread | None = None
    serve_failure: list[BaseException] = []

    if server is not None:

        def _serve_until_shutdown() -> None:
            try:
                serve(server)
            except BaseException as exc:
                serve_failure.append(exc)
                logger.exception("control server failed")
            finally:
                shutdown_requested.set()

        serve_thread = threading.Thread(
            target=_serve_until_shutdown,
            name="control-server",
            daemon=True,
        )
        serve_thread.start()
    else:
        controller.request_start()

    try:
        controller.wait_until_finished()
    finally:
        shutdown_requested.set()
        shutdown_thread.join(SHUTDOWN_TIMEOUT_SEC)
        controller.request_stop()
        snapshot = controller.wait_until_stopped(SHUTDOWN_TIMEOUT_SEC)
        if not snapshot.is_terminal:
            logger.warning("pipeline did not stop within %ss, exiting anyway", SHUTDOWN_TIMEOUT_SEC)
        if server is not None and serve_thread is not None:
            if serve_thread.is_alive():
                server.shutdown()
            serve_thread.join(SHUTDOWN_TIMEOUT_SEC)

    if serve_failure:
        raise serve_failure[0]

    return controller.snapshot


def run_runtime(runtime: StreamAdapter[Any, Any]) -> None:
    shutdown_requested = threading.Event()
    _install_signal_handlers(shutdown_requested)
    _raise_on_error(_run_pipeline(PipelineController(runtime), shutdown_requested))


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
    with multiprocessing.Pool(processes=1) as pool:
        pipeline: Pipeline[Any] = pool.apply(_load_pipeline, (application,))

    validate_all_branches_have_sinks(pipeline)

    metric_config_raw = environment_config.get("metrics", {})
    streams_config: MetricsConfig
    if metric_config_raw.get("type") == "datadog":
        default_tags = dict(metric_config_raw.get("tags", {}))
        default_tags["pipeline"] = name

        base_dd: DatadogMetricsConfig = {
            "type": "datadog",
            "host": metric_config_raw["host"],
            "port": int(metric_config_raw["port"]),
            "tags": default_tags,
        }
        if metric_config_raw.get("udp_queue_size") is not None:
            base_dd["udp_queue_size"] = metric_config_raw["udp_queue_size"]
        if metric_config_raw.get("flush_interval_ms") is not None:
            base_dd["flush_interval_ms"] = int(metric_config_raw["flush_interval_ms"])
        streams_config = cast(MetricsConfig, base_dd)
        configure_metrics(streams_config)
    elif metric_config_raw.get("type") == "log":
        default_tags = dict(metric_config_raw.get("tags", {}))
        default_tags["pipeline"] = name

        streams_config = {
            "type": "log",
            "period_sec": float(metric_config_raw["period_sec"]),
            "tags": default_tags,
        }
        configure_metrics(streams_config)
    else:
        streams_config = {"type": "dummy"}
        configure_metrics(streams_config)

    assigned_segment_id = int(segment_id) if segment_id else None
    runtime: Any = load_adapter(
        adapter,
        environment_config,
        streams_config,
        assigned_segment_id,
    )
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    return runtime


def load_runtime_with_config_file(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> Any:
    """Load runtime from a config file path, returning the runtime object without calling run()."""
    environment_config = load_config(config)

    sentry_sdk_config = environment_config.get("sentry_sdk_config")
    if sentry_sdk_config:
        sentry_sdk.init(dsn=sentry_sdk_config["dsn"])

    return load_runtime(name, log_level, adapter, segment_id, application, environment_config)


def run_with_config_file(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
) -> None:
    """
    Load runtime from config file and run it. Used by the Python CLI.

    NOTE: This function is separate from load_runtime_with_config_file() for a reason:
    - load_runtime_with_config_file() returns the runtime WITHOUT calling .run()
    - This allows the Rust CLI (run.rs) to pass that runtime to run_runtime()
    - Do NOT combine these functions: both CLIs need the runner-owned controller
      to decide when .run() is called
    """
    runtime = load_runtime_with_config_file(
        name, log_level, adapter, config, segment_id, application
    )
    run_runtime(runtime)


def serve_with_config_file(
    name: str,
    log_level: str,
    adapter: str,
    config: str,
    segment_id: Optional[str],
    application: str,
    control_host: str,
    control_port: int,
) -> None:
    runtime = load_runtime_with_config_file(
        name, log_level, adapter, config, segment_id, application
    )
    controller = PipelineController(runtime)
    server = make_server(controller, control_host, control_port)
    shutdown_requested = threading.Event()
    _install_signal_handlers(shutdown_requested)
    _raise_on_error(_run_pipeline(controller, shutdown_requested, server))


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
@click.option(
    "--control-host",
    type=str,
    default=None,
    help=(
        "Runs in operator-controlled mode and serves the control server on this host."
        "Required with --control-port."
    ),
)
@click.option(
    "--control-port",
    type=int,
    default=None,
    help=(
        "Runs in operator-controlled mode and serves the control server on this port."
        "Required with --control-host."
    ),
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
    control_host: Optional[str],
    control_port: Optional[int],
    application: str,
) -> None:
    if control_host is not None or control_port is not None:
        if control_host is None or control_port is None:
            raise click.UsageError("--control-host and --control-port must be provided together")
        serve_with_config_file(
            name,
            log_level,
            adapter,
            config,
            segment_id,
            application,
            control_host,
            control_port,
        )
    else:
        run_with_config_file(name, log_level, adapter, config, segment_id, application)


if __name__ == "__main__":
    main()
