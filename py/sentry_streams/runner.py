import os
import sys
from typing import Any, cast

from pyflink.datastream import StreamExecutionEnvironment
from sentry_streams.adapters.flink_adapter import FlinkAdapter
from sentry_streams.adapters.stream_adapter import StreamAdapter
from sentry_streams.pipeline import (
    Pipeline,
    PipelineGraph,
    RuntimeTranslator,
    WithInput,
)


def iterate_edges(step_streams: dict[str, Any], p_graph: PipelineGraph) -> Any:
    output_stream = None
    while step_streams:
        for input_name in list(step_streams):
            output_steps = p_graph.outgoing_edges[input_name]
            input_stream = step_streams.pop(input_name)

            if not output_steps:
                continue

            # check if the inputs are fanning out
            if len(output_steps) > 1:
                pass

            else:
                output_step_name = output_steps.pop()

                # check if the inputs are fanning in
                if len(p_graph.incoming_edges[output_step_name]) > 1:
                    pass

                # 1:1 between input and output stream
                else:
                    next_step: WithInput = cast(WithInput, p_graph.steps[output_step_name])
                    print(f"Apply step: {next_step.name}")
                    next_step_stream = next_step.apply_edge(input_stream)
                    step_streams[next_step.name] = next_step_stream
                    output_stream = next_step_stream

    return output_stream


def main() -> None:
    pipeline_globals: dict[str, Any] = {}

    with open(sys.argv[1]) as f:
        exec(f.read(), pipeline_globals)

    pipeline: Pipeline = pipeline_globals["pipeline"]
    p_graph = pipeline.graph

    libs_path = os.environ.get("FLINK_LIBS")
    assert libs_path is not None, "FLINK_LIBS environment variable is not set"

    jar_file = os.path.join(os.path.abspath(libs_path), "flink-connector-kafka-3.4.0-1.20.jar")
    kafka_jar_file = os.path.join(os.path.abspath(libs_path), "kafka-clients-3.4.0.jar")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        },
        "broker": "localhost:9092",
    }

    # This will not be harcdoded in the future
    runtime_config: StreamAdapter = FlinkAdapter(environment_config, env)
    translator = RuntimeTranslator(runtime_config)

    pipeline.set_translator(translator)

    step_streams = {}

    for source in p_graph.sources:
        print(f"Apply source: {source.name}")
        env_source = source.apply_source()
        step_streams[source.name] = env_source
        iterate_edges(step_streams, p_graph)

    # submit for execution
    env.execute()


if __name__ == "__main__":
    main()
