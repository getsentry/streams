from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps_chain import StepsChains, transform
from sentry_streams.pipeline.message import PyMessage, PyRawMessage
from sentry_streams.pipeline.pipeline import (
    Map,
    Pipeline,
)


def make_message(payload: str) -> PyMessage[str]:
    return PyMessage(
        payload=payload, headers=[("h", "v".encode())], timestamp=1234567890, schema="myschema"
    )


def test_empty_chain() -> None:
    msg = make_message("foo")
    result = transform([], msg)
    assert result is msg


def test_transform_chain_with_two_steps() -> None:
    pipeline = Pipeline()
    chain = [
        Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t1"),
        Map(name="map2", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t2"),
    ]
    msg = make_message("bar")
    result = transform(chain, msg)
    assert isinstance(result, PyMessage)
    assert result.payload == "bar_t1_t2"


def test_transform_chain_with_bytes_output() -> None:
    pipeline = Pipeline()
    chain = [
        Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload.encode("utf-8")),
    ]
    msg = make_message("baz")
    result = transform(chain, msg)
    assert isinstance(result, PyRawMessage)
    assert result.payload == b"baz"


def test_map_with_multiple_chains() -> None:
    route = Route("route1", [])
    route2 = Route("route2", [])
    pipeline = Pipeline()
    sc = StepsChains()
    m1 = Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t1")
    m2 = Map(name="map2", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t2")
    sc.add_map(route, m1)
    sc.add_map(route2, m2)
    assert sc.exists(route)
    fn = sc.finalize(route)
    msg = make_message("msg")
    result = fn(msg)
    assert result.payload == "msg_t1"
    assert not sc.exists(route)
