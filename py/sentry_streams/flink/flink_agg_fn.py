from typing import Any

from pyflink.datastream.functions import AggregateFunction, KeySelector
from sentry_streams.user_functions.function_template import Accumulator, GroupBy


class FlinkAggregate(AggregateFunction):

    def __init__(self, acc: Accumulator) -> None:
        self.acc = acc

    def create_accumulator(self) -> Any:
        print("CREATED")
        return self.acc.create()

    def add(self, value: Any, accumulator: Any) -> Any:
        print("ADDED")
        return self.acc.add(accumulator, value)

    def get_result(self, accumulator: Any) -> Any:
        print("RESULT")
        return self.acc.get_output(accumulator)

    def merge(self, acc_a: Any, acc_b: Any) -> Any:
        print("MERGE")
        return self.acc.merge(acc_a, acc_b)


class FlinkGroupBy(KeySelector):

    def __init__(self, group_by: GroupBy) -> None:
        self.group_by = group_by

    def get_key(self, value: Any) -> Any:
        return self.group_by.get_group_by_key(value)
