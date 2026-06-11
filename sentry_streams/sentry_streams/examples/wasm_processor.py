from sentry_streams.pipeline.pipeline import (
    StreamSink,
    WasmProcessor,
    streaming_source,
)

pipeline = streaming_source(name="myinput", stream_name="ingest-metrics")

(
    pipeline.apply(
        WasmProcessor(
            "wasm",
            module_path="/Users/filippopacifici/code/streams/examples/wasm_guest/dist/plugin.wasm",
        )
    ).sink(StreamSink("mysink", stream_name="processed-profiles"))
)
