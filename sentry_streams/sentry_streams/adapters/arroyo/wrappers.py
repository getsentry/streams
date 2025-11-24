import time
from typing import Any, Callable

from sentry_streams.metrics import Metric, get_metrics, get_size
from sentry_streams.pipeline import Map
from sentry_streams.pipeline.message import Message


def input_metrics(name: str, message_size: int | None) -> float:
    metrics = get_metrics()
    tags = {"step": name}
    metrics.increment(Metric.INPUT_MESSAGES, tags=tags)
    if message_size is not None:
        metrics.increment(Metric.INPUT_BYTES, tags=tags, value=message_size)
    return time.time()


def output_metrics(
    name: str, error: str | None, start_time: float, message_size: int | None
) -> None:
    metrics = get_metrics()
    tags = {"step": name}
    if error:
        tags["error"] = error
        metrics.increment(Metric.ERRORS, tags=tags)

    metrics.increment(Metric.OUTPUT_MESSAGES, tags=tags)
    if message_size is not None:
        metrics.increment(Metric.OUTPUT_BYTES, tags=tags, value=message_size)
    metrics.timing(Metric.DURATION, time.time() - start_time, tags=tags)


def wrapped_function(
    step: Map[Any, Any], application_function: Callable[[Message[Any]], Any], msg: Message[Any]
) -> Any:
    msg_size = get_size(msg.payload) if hasattr(msg, "payload") else None
    start_time = input_metrics(step.name, msg_size)
    has_error = output_size = None
    try:
        result = application_function(msg)
        output_size = get_size(result)
        return result
    except Exception as e:
        has_error = str(e.__class__.__name__)
        raise e
    finally:
        output_metrics(step.name, has_error, start_time, output_size)
        pass
