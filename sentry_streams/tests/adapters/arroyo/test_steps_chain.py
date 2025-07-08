from typing import Any, Union

import pytest
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import FilteredPayload
from arroyo.types import Message as ArroyoMessage

from sentry_streams.adapters.arroyo.multi_process_delegate import (
    mapped_msg_to_rust,
    rust_to_arroyo_msg,
)
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.rust_step import (
    ArroyoStrategyDelegate,
    OutputRetriever,
)
from sentry_streams.adapters.arroyo.steps_chain import TransformChains, transform
from sentry_streams.config_types import (
    MultiProcessConfig,
)
from sentry_streams.pipeline.message import (
    Message,
    PyMessage,
    PyRawMessage,
)
from sentry_streams.pipeline.pipeline import (
    Map,
    Pipeline,
)
from sentry_streams.rust_streams import PyAnyMessage


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


CONFIG = MultiProcessConfig(
    {
        "processes": 2,
        "batch_size": 100,
        "batch_time": 10,
        "input_block_size": None,
        "max_input_block_size": None,
        "max_output_block_size": None,
        "output_block_size": None,
    }
)


def test_initialization() -> None:
    route = Route("route1", [])
    pipeline = Pipeline()
    sc = TransformChains()
    m1 = Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t1")

    assert not sc.exists(route)
    with pytest.raises(ValueError):
        sc.add_map(route, m1)

    sc.init_chain(
        route,
        CONFIG,
    )
    assert sc.exists(route)
    sc.add_map(route, m1)
    ret_config, _ = sc.finalize(route)
    assert ret_config == CONFIG
    assert not sc.exists(route)


def test_map_with_multiple_chains() -> None:
    route = Route("route1", [])
    route2 = Route("route2", [])
    pipeline = Pipeline()
    sc = TransformChains()
    m1 = Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t1")
    m2 = Map(name="map2", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t2")
    sc.init_chain(route, CONFIG)
    sc.init_chain(route2, CONFIG)
    sc.add_map(route, m1)
    sc.add_map(route2, m2)
    assert sc.exists(route)
    ret_conf, fn = sc.finalize(route)
    msg = make_message("msg")
    result = fn(msg)
    assert result.payload == "msg_t1"
    assert not sc.exists(route)


def test_integration() -> None:
    # TODO: Figure out a way to run the proper multi process strategy
    # in a stable way in a unit test.
    route = Route("route1", [])
    pipeline = Pipeline()
    sc = TransformChains()
    m1 = Map(name="map1", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t1")
    m2 = Map(name="map2", ctx=pipeline, inputs=[], function=lambda msg: msg.payload + "_t2")

    sc.init_chain(route, None)
    sc.add_map(route, m1)
    sc.add_map(route, m2)
    _, fn = sc.finalize(route)

    retriever = OutputRetriever[Union[FilteredPayload, Message[str]]](mapped_msg_to_rust)

    def transformer(msg: ArroyoMessage[Message[Any]]) -> Message[Any]:
        return fn(msg.payload)

    delegate = ArroyoStrategyDelegate(
        RunTask(transformer, retriever), rust_to_arroyo_msg, retriever
    )

    delegate.submit(
        PyAnyMessage(payload="foo", headers=[("h", "v".encode())], timestamp=123, schema="s"),
        committable={
            ("t", 1): 42,
        },
    )
    ret = list(delegate.poll())
    assert len(ret) == 1
    ret_msg, _ = ret[0]

    expected = PyMessage(
        "foo_t1_t2", headers=[("h", "v".encode())], timestamp=123, schema="s"
    ).inner

    assert ret_msg.payload == expected.payload
    assert ret_msg.headers == expected.headers
    assert ret_msg.timestamp == expected.timestamp
    assert ret_msg.schema == expected.schema
