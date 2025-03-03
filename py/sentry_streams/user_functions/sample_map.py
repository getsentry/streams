import json


class EventsPipelineMapFunctions:
    """
    Sample user-defined map functions to
    plug into pipeline
    """

    @staticmethod
    def simple_map(value: str) -> str:
        d = json.loads(value)
        res: str = d.get("name", "no name")

        return "hello " + res

    @staticmethod
    def no_op_map(value: str) -> str:
        return value
