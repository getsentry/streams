import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaException, Producer

TEST_PRODUCER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "broker.address.family": "v4",
}
TEST_CONSUMER_CONFIG = {
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "pipeline-test-consumer",
    "auto.offset.reset": "earliest",
}


@dataclass
class PipelineRun:
    name: str
    config_file: str
    application_file: str
    source_topic: str
    sink_topics: list[str]
    input_messages: list[str]
    num_expected_messages: dict[str, int]


def create_ingest_message(**kwargs: Any) -> str:
    message = {
        "org_id": 420,
        "project_id": 420,
        "name": "s:sessions/user@none",
        "tags": {
            "sdk": "raven-node/2.6.3",
            "environment": "production",
            "release": "sentry-test@1.0.0",
        },
        "timestamp": 1846062325,
        "type": "c",
        "retention_days": 90,
        "value": [1617781333],
    }
    message.update(kwargs)
    return json.dumps(message)


def create_topic(topic_name: str, num_partitions: int) -> None:
    print(f"Creating topic: {topic_name}, with {num_partitions} partitions")
    create_topic_cmd = [
        "docker",
        "exec",
        "kafka-kafka-1",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        topic_name,
        "--partitions",
        str(num_partitions),
    ]
    res = subprocess.run(create_topic_cmd, capture_output=True, text=True)
    if res.returncode != 0:
        if "already exists" in res.stderr:
            return

        print(f"Got return code: {res.returncode}, when creating topic")
        print(f"Stdout: {res.stdout}")
        print(f"Stderr: {res.stderr}")
        raise Exception(f"Failed to create topic: {topic_name}")


def run_pipeline_cmd(test: PipelineRun) -> subprocess.Popen[str]:
    """
    Run the pipeline using the command line interface.
    """
    process = subprocess.Popen[str](
        [
            "python",
            "-m",
            "sentry_streams.runner",
            "--adapter",
            "rust_arroyo",
            "--config",
            test.config_file,
            "--segment-id",
            "0",
            test.application_file,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process


def send_messages_to_topic(topic_name: str, messages: list[str]) -> None:
    """
    Send messages to kafka topic.
    """
    try:
        producer = Producer(TEST_PRODUCER_CONFIG)

        for message in messages:
            producer.produce(topic_name, message)

        producer.flush()
        print(f"Sent {len(messages)} messages to kafka topic {topic_name}")
    except Exception as e:
        raise Exception(f"Failed to send messages to kafka: {e}")


def get_topic_size(topic_name: str) -> int:
    """
    Creates a consumer and polls the topic starting at the earliest offset
    attempts are exhausted.
    """
    attempts = 30
    size = 0
    consumer = Consumer(TEST_CONSUMER_CONFIG)
    consumer.subscribe([topic_name])
    while attempts > 0:
        event = consumer.poll(1.0)
        if event is None:
            attempts -= 1
            continue
        if event.error():
            raise KafkaException(event.error())
        else:
            size += 1

    return size


def run_example_test(test: PipelineRun) -> None:
    print(f"{test.name}: Creating topics")
    create_topic(test.source_topic, 1)
    for sink_topic in test.sink_topics:
        create_topic(sink_topic, 1)

    print(f"{test.name}: Running pipeline")
    process = run_pipeline_cmd(test)

    # Give the pipeline a chance to start up
    time.sleep(30)

    print(f"{test.name}: Sending messages")
    send_messages_to_topic(test.source_topic, test.input_messages)

    print(f"{test.name}: Waiting for messages")
    start_time = time.time()
    while time.time() - start_time < 30:
        if process.poll() is not None:  # Runner shouldn't stop
            stdout, stderr = process.communicate()
            print(f"Pipeline process exited with code {process.returncode}")
            print(f"Stdout: {stdout}")
            print(f"Stderr: {stderr}")
            raise Exception(f"Pipeline process exited with code {process.returncode}")

        received = {}
        for sink_topic in test.sink_topics:
            size = get_topic_size(sink_topic)
            received[sink_topic] = (size, size == test.num_expected_messages[sink_topic])
            print(f"{test.name}: Received {received[sink_topic]} messages from {sink_topic}")

        if all(v[1] for v in received.values()):
            break

        time.sleep(1)

    print(f"{test.name}: Waiting for process to exit")
    process.send_signal(signal.SIGKILL)
    process.wait()
    stdout, stderr = process.communicate()
    print(f"Stdout: {stdout}")
    print(f"Stderr: {stderr}")

    for sink_topic, (size, expected) in received.items():
        assert (
            expected
        ), f"Expected {test.num_expected_messages[sink_topic]} messages on {sink_topic}, got {size}"


@dataclass
class ExampleTest:
    name: str
    source_topic: str
    sink_topics: list[str]
    input_messages: list[str]
    expected_messages: dict[str, int]

    def to_list(self) -> list[Any]:
        return [
            self.name,
            self.source_topic,
            self.sink_topics,
            self.input_messages,
            self.expected_messages,
        ]


example_tests = [
    pytest.param(
        ExampleTest(
            name="simple_map_filter",
            source_topic="ingest-metrics",
            sink_topics=["transformed-events"],
            input_messages=[create_ingest_message(type="c")],
            expected_messages={"transformed-events": 1},
        ),
        id="simple_map_filter",
    )
]


@pytest.mark.parametrize("example_test", example_tests)
def test_examples(example_test: ExampleTest) -> None:
    test = PipelineRun(
        name=example_test.name,
        config_file=os.path.join(
            os.path.dirname(__file__),
            "..",
            "sentry_streams",
            "deployment_config",
            f"{example_test.name}.yaml",
        ),
        application_file=os.path.join(
            os.path.dirname(__file__),
            "..",
            "sentry_streams",
            "examples",
            f"{example_test.name}.py",
        ),
        source_topic=example_test.source_topic,
        sink_topics=example_test.sink_topics,
        input_messages=example_test.input_messages,
        num_expected_messages=example_test.expected_messages,
    )
    run_example_test(test)
