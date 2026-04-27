import time
from collections import defaultdict

from sentry_streams.metrics import Metrics
from sentry_streams.metrics.metrics import Metric, get_raw_metrics

FLUSH_TIME = 10


class PipelineStats:

    def __init__(self, metrics: Metrics) -> None:
        self._metrics = metrics

        self._exec_buffer: dict[str, int] = defaultdict(int)
        self._error_buffer: dict[str, int] = defaultdict(int)
        self._timing_buffer: dict[str, float] = defaultdict(float)

        self.__last_flush_time = 0.0

    def step_exec(self, step: str) -> None:
        self._exec_buffer[step] += 1
        self._maybe_flush()

    def step_error(self, step: str) -> None:
        self._error_buffer[step] += 1
        self._maybe_flush()

    def step_timing(self, step: str, value: float) -> None:
        if self._timing_buffer[step] < value:
            self._timing_buffer[step] = value
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        if time.time() - self.__last_flush_time >= FLUSH_TIME:
            self.__last_flush_time = time.time()
            for step, value in self._exec_buffer.items():
                tags = {"step": step}
                self._metrics.increment(Metric.INPUT_MESSAGES, value, tags)
            for step, value in self._error_buffer.items():
                tags = {"step": step}
                self._metrics.increment(Metric.ERRORS, value, tags)
            for step, fvalue in self._timing_buffer.items():
                tags = {"step": step}
                self._metrics.timing(Metric.DURATION, fvalue, tags)

            self._exec_buffer = defaultdict(int)
            self._error_buffer = defaultdict(int)
            self._timing_buffer = defaultdict(float)


_stats: PipelineStats | None = None


def get_stats() -> PipelineStats:
    global _stats
    if _stats is None:
        _stats = PipelineStats(get_raw_metrics())
    return _stats
