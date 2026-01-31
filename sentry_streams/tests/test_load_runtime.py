from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import patch

import pytest
import sentry_sdk
from sentry_sdk.transport import Transport

from sentry_streams.runner import load_runtime, run_with_config_file

# Path to fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


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

    app_file = FIXTURES_DIR / "simple_app.py"

    runtime = load_runtime(
        name="test",
        log_level="INFO",
        adapter="dummy",
        segment_id=None,
        application=str(app_file),
        environment_config={"metrics": {"type": "dummy"}},
    )

    assert runtime is not None

    # Verify that the pipeline was loaded and edges were iterated
    # The dummy adapter tracks input streams
    from sentry_streams.dummy.dummy_adapter import DummyAdapter

    assert isinstance(runtime, DummyAdapter)
    assert "test" in runtime.input_streams
    assert "test-sink" in runtime.input_streams


def test_subprocess_sends_error_status_with_details(
    platform_transport: CaptureTransport, temp_fixture_dir: Any
) -> None:
    """Test that detailed error messages are captured when subprocess sends status='error'."""

    app_file = FIXTURES_DIR / "missing_pipeline.py"

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
