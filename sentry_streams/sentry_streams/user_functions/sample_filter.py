import json


class EventsPipelineFilterFunctions:
    """
    Sample user-defined filter functions to
    plug into pipeline
    """

    @staticmethod
    def simple_filter(value: str) -> bool:
        d = json.loads(value)
        return True if "name" in d else False

    @staticmethod
    def wrong_type_filter(value: str) -> str:  # type: ignore[empty-body]
        # TODO: move test functions into the tests/ folder somehow
        """
        Filter function with wrong return type, used in tests
        """
        pass


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
