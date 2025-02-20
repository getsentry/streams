from sentry_streams.user_functions.agg_template import Accumulator


class WordCounter(Accumulator):

    def create(self) -> tuple[str, int]:
        return "", 0

    def add(self, acc: tuple[str, int], value: tuple[str, int]) -> tuple[str, int]:
        return value[0], acc[1] + value[1]

    def get_output(self, acc: tuple[str, int]) -> str:
        return f"{acc[0]} {acc[1]}"

    def merge(self, acc1: tuple[str, int], acc2: tuple[str, int]) -> tuple[str, int]:
        return acc1[0], acc1[1] + acc2[1]
