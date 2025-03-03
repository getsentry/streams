from typing import Optional

Outcome = dict[str, str]


class PendingBuffer:

    def __init__(self, outcomes_dict: Optional[dict[str, int]] = None):
        if outcomes_dict:
            self.map: dict[str, int] = outcomes_dict

        else:
            self.map = {"state": 0, "data_cat": 0}

    def add(self, outcome: Outcome) -> None:

        if "state" in outcome:
            self.map["state"] += 1

        elif "data_cat" in outcome:
            self.map["data_cat"] += 1

        else:
            self.map["null"] += 1

    def get_key(self, key: str) -> int:
        return self.map[key]
