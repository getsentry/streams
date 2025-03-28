from json import JSONDecodeError, dumps, loads
from typing import Any, Mapping, cast

from sentry_streams.pipeline import Map, multi_chain, streaming_source


def parse(msg: str) -> Mapping[str, Any]:
    try:
        parsed = loads(msg)
    except JSONDecodeError:
        return {"type": "invalid"}

    return cast(Mapping[str, Any], parsed)


def serialize(msg: Mapping[str, Any]) -> str:
    return dumps(msg)


def do_something(msg: Mapping[str, Any]) -> Mapping[str, Any]:
    # Do something with the message
    return msg


pipeline = multi_chain(
    [
        # Main Ingest chain
        streaming_source("ingest", stream_name="ingest-events")
        .apply("parse_msg", Map(parse))
        .apply("process", Map(do_something))
        .apply("serialize", Map(serialize))
        .sink("eventstream", stream_name="events"),
        # Snuba chain to Clickhouse
        streaming_source("snuba", stream_name="events")
        .apply("snuba_parse_msg", Map(parse))
        .sink(
            "clickhouse",
            stream_name="someewhere",
        ),
        # Super Big Consumer chain
        streaming_source("sbc", stream_name="events")
        .apply("sbc_parse_msg", Map(parse))
        .sink(
            "sbc_sink",
            stream_name="someewhere",
        ),
        # Post process chain
        streaming_source("post_process", stream_name="events")
        .apply("post_parse_msg", Map(parse))
        .apply("postprocess", Map(do_something))
        .sink(
            "devnull",
            stream_name="someewhereelse",
        ),
    ]
)
