import json


class BroadcastFunctions:
    """
    Sample broadcast functions used in the broadcast
    example pipeline.
    This pipeline is a silly example which takes a JSON string
    in the form '{"name":"Foo"}' as input and sends a Hello message
    to one topic and a Goodbye message to another.
    """

    @staticmethod
    def no_op_map(value: str) -> str:
        return value

    @staticmethod
    def hello_map(value: str) -> str:
        name = json.loads(value)["name"]
        return f"Hello, {name}!"

    @staticmethod
    def goodbye_map(value: str) -> str:
        name = json.loads(value)["name"]
        return f"Goodbye, {name}."
