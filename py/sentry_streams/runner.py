import os
import sys
from typing import Any, cast

from pyflink.datastream import StreamExecutionEnvironment
from sentry_streams.sinks import Pipeline, WithInput


def main() -> None:
    pipeline_globals: dict[str, Any] = {}

    with open(sys.argv[1]) as f:
        exec(f.read(), pipeline_globals)

    p: Pipeline = pipeline_globals["pipeline"]

    libs_path = os.environ.get("FLINK_LIBS")
    assert libs_path is not None, "FLINK_LIBS environment variable is not set"

    jar_file = os.path.join(
        os.path.abspath(libs_path), "flink-connector-kafka-3.4.0-1.20.jar"
    )
    kafka_jar_file = os.path.join(os.path.abspath(libs_path), "kafka-clients-3.4.0.jar")

    print(kafka_jar_file)
    print(jar_file)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{jar_file}", f"file://{kafka_jar_file}")

    # TODO: read from yaml file
    environment_config = {
        "topics": {
            "logical-events": "events",
            "transformed-events": "transformed-events",
        }
    }

    def recurse_edge(input_name: str, stream: Any) -> None:
        for next_step_name in p.edges.get(input_name, ()):
            print(f"Apply step: {next_step_name}")
            next_step: WithInput = cast(WithInput, p.steps[next_step_name])
            recurse_edge(
                next_step_name, next_step.apply_edge(stream, environment_config)
            )

    for source in p.sources:
        print(f"Apply source: {source.name}")
        env_source = source.apply_source(env, environment_config)
        recurse_edge(source.name, env_source)

    # submit for execution
    env.execute()


if __name__ == "__main__":
    main()
