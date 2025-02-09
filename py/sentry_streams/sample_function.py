import json


class EventsPipelineMapFunction:
    """
    Sample user-defined functions to
    plug into pipeline
    """

    @staticmethod
    def simple_map(value: str) -> str:
        d = json.loads(value)
        res: str = d.get("a", "no_value")

        return res
