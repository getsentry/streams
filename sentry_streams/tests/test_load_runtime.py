from typing import Any, Generator, List, Optional
from unittest.mock import patch

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

from sentry_streams.pipeline.pipeline import Pipeline
from sentry_streams.runner import load_runtime, run_with_config_file


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
from sentry_streams.pipeline import streaming_source, StreamSink
pipeline = streaming_source(name="test", stream_name="test-stream").sink(StreamSink("test-sink", stream_name="test-output"))
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

    app_file = temp_fixture_dir / "missing_pipeline.py"
    app_file.write_text(
        """
import sentry_sdk

# Initialize customer's Sentry SDK in the subprocess
sentry_sdk.init(dsn="https://customer@example.com/123")

from sentry_streams.pipeline import streaming_source
# Intentionally not defining 'pipeline' variable
my_pipeline = streaming_source(name="test", stream_name="test-stream")
"""
    )

    config_file = temp_fixture_dir / "config.yaml"
    config_file.write_text(
        """
sentry_sdk_config:
  dsn: "https://platform@example.com/456"
metrics:
  type: dummy
"""
    )

    # Patch sentry_sdk.init to use our custom transport
    original_init = sentry_sdk.init
    error_raised = False

    def custom_init(**kwargs: Any) -> None:
        kwargs["transport"] = platform_transport
        original_init(**kwargs)

    with patch("sentry_streams.runner.sentry_sdk.init", side_effect=custom_init):
        try:
            run_with_config_file(
                name="test",
                log_level="INFO",
                adapter="arroyo",
                config=str(config_file),
                segment_id=None,
                application=str(app_file),
            )
        except ValueError as e:
            error_raised = True
            sentry_sdk.capture_exception(e)
            sentry_sdk.flush()
            assert "Application file must define a 'pipeline' variable" in str(e)

    assert error_raised, "Expected intentiaonl ValueError to be raised"

    assert len(platform_transport.envelopes) > 0, "Error should be captured in platform_transport"

    envelope = platform_transport.envelopes[0]
    items = envelope.items
    assert len(items) > 0, "Envelope should contain at least one item"

    event_item = items[0]
    error_event = event_item.payload.json

    assert "exception" in error_event
    error_message = str(error_event["exception"]["values"][0]["value"])
    assert "Application file must define a 'pipeline' variable" in error_message
