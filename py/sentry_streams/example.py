import logging
import os
import sys

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer  # type: ignore

INPUT_TOPIC = "events"
KAFKA_BROKER = "localhost:9092"

# Just a simple flink example


def run_stream() -> None:
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

    kafka_consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "python-flink-consumer",
        },
    )
    # define the source
    kafka_consumer.set_start_from_earliest()
    env.add_source(kafka_consumer).print()  # .name("Kafka Source")

    # define the sink
    print("Printing result to stdout. Use --output to specify output path.")

    # submit for execution
    env.execute()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    run_stream()
