import json

from sentry_streams.pipeline.message import Message


class BroadcastFunctions:
    """
    Sample broadcast functions used in the broadcast
    example pipeline.
    This pipeline is a silly example which takes a JSON string
    in the form '{"name":"Foo"}' as input and sends a Hello message
    to one topic and a Goodbye message to another.
    """

    @staticmethod
    def no_op_map(value: Message[bytes]) -> str:
        return value.payload.decode("utf-8")

    @staticmethod
    def hello_map(value: Message[str]) -> str:
        name = json.loads(value.payload)["name"]
        return f"Hello, {name}!"

    @staticmethod
    def goodbye_map(value: Message[str]) -> str:
        name = json.loads(value.payload)["name"]
        return f"Goodbye, {name}."
