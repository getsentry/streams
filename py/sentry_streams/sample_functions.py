import json


class EventsPipelineFunctions:
    """
    Sample user-defined functions to
    plug into pipeline
    """

    @staticmethod
    def simple_map(value: str) -> str:
        d = json.loads(value)
        res: str = d.get("name", "no name")

        return "hello " + res
