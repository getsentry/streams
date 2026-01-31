from sentry_streams.pipeline import streaming_source
from sentry_streams.pipeline.pipeline import DevNullSink

pipeline = streaming_source(name="test", stream_name="test-stream").sink(DevNullSink("test-sink"))
