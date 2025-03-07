from typing import Optional, Self

from sentry_streams.pipeline.function_template import KVAccumulator

Outcome = dict[str, str]


class OutcomesBuffer(KVAccumulator[Outcome]):
    """
    An accumulator which adds outcomes data to a PendingBuffer.
    Upon the closing of a window, the Buffer is flushed to a
    sample backend (the OutcomesBackend). As of now this backend
    is not a mocked DB, it is a simple hash map.
    """

    def __init__(self, outcomes_dict: Optional[dict[str, int]] = None) -> None:
        if outcomes_dict:
            self.map: dict[str, int] = outcomes_dict

        else:
            self.map = {}

    def add(self, value: Outcome) -> Self:
        outcome_type = ""

        if "state" in value:
            outcome_type += value["state"]

        if "data_cat" in value:
            outcome_type += "-" + value["data_cat"]

        if outcome_type in self.map:
            self.map[outcome_type] += 1

        else:
            self.map[outcome_type] = 1

        return self

    def get_value(self) -> dict[str, int]:
        return self.map

    def merge(self, other: Self) -> Self:

        first = self.map
        second = other.map

        for outcome_key in second:
            if outcome_key in first:
                first[outcome_key] += second[outcome_key]

            else:
                first[outcome_key] = second[outcome_key]

        self.map = first

        return self
