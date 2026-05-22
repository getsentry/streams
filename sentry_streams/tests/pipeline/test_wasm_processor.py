import pytest

from sentry_streams.pipeline.pipeline import StreamSink, WasmProcessor, streaming_source


def test_wasm_processor_validate_requires_module_path() -> None:
    step = WasmProcessor("wasm", module_path="")
    with pytest.raises(ValueError, match="module_path"):
        step.validate()


def test_wasm_processor_override_config_module_path() -> None:
    step = WasmProcessor("wasm", module_path="default.wasm")
    step.override_config({"module_path": "override.wasm"})
    assert step.module_path == "override.wasm"


def test_wasm_processor_pipeline_builds() -> None:
    pipeline = streaming_source("myinput", "events")
    (
        pipeline.apply(WasmProcessor("wasm", module_path="tests/fixtures/wasm_passthrough.wasm"))
        .sink(StreamSink("mysink", stream_name="out"))
    )
    assert "wasm" in pipeline.steps
