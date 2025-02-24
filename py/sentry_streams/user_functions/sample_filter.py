import json


class EventsPipelineFilterFunctions:
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
