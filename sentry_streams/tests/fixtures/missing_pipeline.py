import sentry_sdk

# Initialize customer's Sentry SDK in the subprocess
sentry_sdk.init(dsn="https://customer@example.com/123")

from sentry_streams.pipeline import streaming_source

# Intentionally not defining 'pipeline' variable
my_pipeline = streaming_source(name="test", stream_name="test-stream")
