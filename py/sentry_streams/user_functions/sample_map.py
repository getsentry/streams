import json


class EventsPipelineMapFunction:
    """
    Sample user-defined functions to
    plug into pipeline. Group together
    functions that are related (e.g.
    part of the same pipeline) into
    classes like this one.
    """

    @staticmethod
    def dumb_map(value: str) -> str:
        d = json.loads(value)
        word: str = d.get("word", "null_word")

        return "hello." + word

    @staticmethod
    def simple_map(value: str) -> tuple[str, int]:
        d = json.loads(value)
        word: str = d.get("word", "null_word")

        return (word, 1)

    @staticmethod
    def str_convert(value: tuple[str, int]) -> str:
        word, count = value

        return f"{word} {count}"
