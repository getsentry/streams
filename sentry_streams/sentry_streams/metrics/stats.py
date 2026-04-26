from arroyo.utils.metrics import Metrics, get_metrics


class PipielineStats:

    def __init__(self, metrics: Metrics) -> None:
        self._metrics = metrics

    def step_exec(self, step: str) -> None:
        pass

    def step_error(self, step: str) -> None:
        pass

    def step_timing(self, step: str, value: float) -> None:
        pass


_stats: PipielineStats | None = None


def get_stats() -> PipielineStats:
    global _stats
    if _stats is None:
        _stats = PipielineStats(get_metrics())
    return _stats
