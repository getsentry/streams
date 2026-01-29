from typing import Any, Generator, List, Optional
from unittest.mock import patch

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

from sentry_streams.pipeline.pipeline import Pipeline
from sentry_streams.runner import load_runtime


class CaptureTransport(Transport):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events: List[Any] = []
        self.envelopes: List[Any] = []

    def capture_event(self, event: Any) -> None:
        self.events.append(event)
        return None

    def capture_envelope(self, envelope: Any) -> None:
        self.envelopes.append(envelope)
        return None

    def flush(self, timeout: float, callback: Optional[Any] = None) -> None:
        """Flush is called when SDK shuts down."""
        pass


@pytest.fixture
def temp_fixture_dir(tmp_path: Any) -> Any:
    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    return fixture_dir


@pytest.fixture(autouse=True)
def reset_metrics_backend() -> Generator[None, None, None]:
    """Reset the global metrics backend between tests."""
    from sentry_streams import metrics

    try:
        from arroyo.utils import metrics as arroyo_metrics

        has_arroyo = True
    except ImportError:
        has_arroyo = False

    # Reset before each test
    metrics.metrics._metrics_backend = None
    if has_arroyo:
        arroyo_metrics._metrics_backend = None

    yield

    # Reset to None after each test
    metrics.metrics._metrics_backend = None
    if has_arroyo:
        arroyo_metrics._metrics_backend = None


@pytest.fixture
def platform_transport() -> CaptureTransport:
    transport = CaptureTransport()
    # Clear any existing Sentry client
    sentry_sdk.get_client().close()
    return transport


def test_multiprocess_pipe_communication_success(
    platform_transport: CaptureTransport, temp_fixture_dir: Any
) -> None:
    sentry_sdk.init(
        dsn="https://platform@example.com/456",
        transport=platform_transport,
    )

    app_file = temp_fixture_dir / "simple_app.py"
    app_file.write_text(
        """
from sentry_streams.pipeline import streaming_source
pipeline = streaming_source(name="test", stream_name="test-stream")
"""
    )

    with (
        patch("sentry_streams.runner.load_adapter") as mock_load_adapter,
        patch("sentry_streams.runner.iterate_edges") as mock_iterate_edges,
    ):
        mock_runtime = type(
            "MockRuntime",
            (),
            {
                "run": lambda self: None,
                "source": lambda self, step: "mock_stream",
                "complex_step_override": lambda self: {},
            },
        )()
        mock_load_adapter.return_value = mock_runtime

        runtime = load_runtime(
            name="test",
            log_level="INFO",
            adapter="arroyo",
            segment_id=None,
            application=str(app_file),
            environment_config={"metrics": {"type": "dummy"}},
        )

        assert runtime is not None

        mock_iterate_edges.assert_called_once()
        pipeline_arg = mock_iterate_edges.call_args[0][0]  # First positional argument
        assert isinstance(pipeline_arg, Pipeline)


def test_subprocess_sends_error_status_with_details(
    platform_transport: CaptureTransport, temp_fixture_dir: Any
) -> None:
    """Test that detailed error messages are captured when subprocess sends status='error'."""
    sentry_sdk.init(
        dsn="https://platform@example.com/456",
        transport=platform_transport,
    )

    # Create an app file that doesn't define 'pipeline' variable
    app_file = temp_fixture_dir / "missing_pipeline.py"
    app_file.write_text(
        """
from sentry_streams.pipeline import streaming_source
# Intentionally not defining 'pipeline' variable
my_pipeline = streaming_source(name="test", stream_name="test-stream")
"""
    )

    with pytest.raises(ValueError) as exc_info:
        load_runtime(
            name="test",
            log_level="INFO",
            adapter="arroyo",
            segment_id=None,
            application=str(app_file),
            environment_config={"metrics": {"type": "dummy"}},
        )

    assert "Application file must define a 'pipeline' variable" in str(exc_info.value)
