from sentry_streams.pipeline.pipeline import (
    Branch,
    KafkaSink,
    KafkaSource,
    Map,
    Pipeline,
    Router,
)


def no_op_map(msg: str) -> str:
    return msg


# pipeline: special name
pipeline = Pipeline()

source = KafkaSource(
    name="myinput",
    ctx=pipeline,
    logical_topic="logical-events",
)

map = Map(
    name="mymap",
    ctx=pipeline,
    inputs=[source],
    function=no_op_map,
)

router = Router(
    name="myrouter",
    ctx=pipeline,
    inputs=[map],
    routing_table={
        "branch_1": Branch(name="branch_1", ctx=pipeline),
        "branch_2": Branch(name="branch_2", ctx=pipeline),
    },
    routing_function=lambda x: "branch_1" if int(x) % 2 == 0 else "branch_2",
)

map_1 = Map(
    name="map1",
    ctx=pipeline,
    inputs=[router.routing_table["branch_1"]],
    function=no_op_map,
)


map_2 = Map(
    name="map2",
    ctx=pipeline,
    inputs=[router.routing_table["branch_2"]],
    function=no_op_map,
)

sink_1 = KafkaSink(
    name="kafkasink1",
    ctx=pipeline,
    inputs=[map_1],
    logical_topic="transformed-events",
)

sink_2 = KafkaSink(
    name="kafkasink2",
    ctx=pipeline,
    inputs=[map_2],
    logical_topic="transformed-events-2",
)
