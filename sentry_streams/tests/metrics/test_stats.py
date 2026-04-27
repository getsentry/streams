from unittest.mock import MagicMock, call, patch

from sentry_streams.metrics.metrics import DummyMetricsBackend, Metric, Metrics
from sentry_streams.metrics.stats import PipelineStats


def _make_stats() -> tuple[PipelineStats, MagicMock]:
    """Build :class:`PipelineStats` with a mocked backend; return ``(stats, inner_backend)``."""
    inner = MagicMock(spec=DummyMetricsBackend)
    return PipelineStats(Metrics(inner)), inner


@patch("time.time")
def test_correct_values_are_flushed(
    _mock_time: MagicMock,
) -> None:
    """
    After buffered exec / error / timing data, a flush should emit the correct
    ``increment`` / ``timing`` calls on the raw metrics backend (string names + tags).
    """
    stats, inner = _make_stats()
    _mock_time.return_value = 100.0
    stats._maybe_flush()  # Flush to set last flush time
    stats.step_exec("in_step")
    stats.step_exec("in_step")
    stats.step_error("err_step")
    stats.step_timing("timer_step", 0.1)
    _mock_time.return_value = 120.0
    stats.step_timing("timer_step", 0.05)  # max is 0.1

    inner.increment.assert_has_calls(
        [
            call(Metric.INPUT_MESSAGES.value, 2, tags={"step": "in_step"}),
            call(Metric.ERRORS.value, 1, tags={"step": "err_step"}),
        ],
        any_order=True,
    )
    inner.timing.assert_called_once_with(Metric.DURATION.value, 0.1, tags={"step": "timer_step"})


@patch("time.time")
def test_no_flush_before_deadline(
    _mock_time: MagicMock,
) -> None:
    """``_maybe_flush`` is a no-op (no backend calls) until ``FLUSH_TIME`` has passed since last."""
    _mock_time.return_value = 100.0
    stats, inner = _make_stats()
    stats._maybe_flush()
    stats.step_exec("a")
    _mock_time.return_value = 105.0
    inner.increment.assert_not_called()
    inner.timing.assert_not_called()
    # Last flush time is only set on a successful flush
    last_flush: float = object.__getattribute__(stats, "_PipelineStats__last_flush_time")
    assert last_flush == 100.0
    exec_buf: dict[str, int] = stats._exec_buffer  # noqa: SLF001
    assert exec_buf["a"] == 1


@patch("time.time", return_value=0.0)
def test_pipieline_stats_flush_clears_buffers(mock_time: MagicMock) -> None:
    """A successful flush clears in-memory counts so a later flush only reports new data."""
    stats, inner = _make_stats()
    mock_time.return_value = 100.0
    stats.step_exec("s")
    inner.reset_mock()
    mock_time.return_value = 100.0
    inner.assert_not_called()
    mock_time.return_value = 110.0
    stats.step_exec("s")
    inner.increment.assert_called_once_with(Metric.INPUT_MESSAGES.value, 1, tags={"step": "s"})


@patch("time.time", return_value=20.0)
def test_pipieline_stats_multiple_steps_one_flush(_mock_time: MagicMock) -> None:
    """One flush can emit several backend calls, one per buffered step (each tag set)."""
    stats, inner = _make_stats()
    _mock_time.return_value = 100.0
    stats._maybe_flush()  # Flush to set last flush time
    stats.step_exec("step_1")
    stats.step_exec("step_1")
    _mock_time.return_value = 120.0
    stats.step_exec("step_2")
    assert inner.increment.call_count == 2
    inner.increment.assert_has_calls(
        [
            call(Metric.INPUT_MESSAGES.value, 2, tags={"step": "step_1"}),
            call(Metric.INPUT_MESSAGES.value, 1, tags={"step": "step_2"}),
        ],
        any_order=True,
    )
