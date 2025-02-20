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

    @staticmethod
    def simple_filter(value: str) -> bool:
        d = json.loads(value)
        return True if "name" in d else False

    @staticmethod
    def wrong_type_filter(value: str) -> str:
        # TODO: move test functions into the tests/ folder somehow
        """
        Filter with wrong return type, used in tests
        """
        d = json.loads(value)
        return "True" if "name" in d else "False"
