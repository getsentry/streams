import json


class EventsPiplineFilterFunctions:
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
