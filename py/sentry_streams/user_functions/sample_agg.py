from sentry_streams.user_functions.function_template import Accumulator

WordCountTuple = tuple[str, int]
WordCountStr = str


class WordCounter(Accumulator[WordCountTuple, WordCountTuple, WordCountStr]):

    def create(self) -> WordCountTuple:
        return "", 0

    def add(self, acc: WordCountTuple, value: WordCountTuple) -> WordCountTuple:
        return value[0], acc[1] + value[1]

    def get_output(self, acc: WordCountTuple) -> WordCountStr:
        return f"{acc[0]} {acc[1]}"

    def merge(self, acc1: WordCountTuple, acc2: WordCountTuple) -> WordCountTuple:
        return acc1[0], acc1[1] + acc2[1]
