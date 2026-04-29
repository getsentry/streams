from typing import Any

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem

from sentry_streams.examples.transform_metrics import (
    ItemsSpanProcessor,
    count_batch,
    do_count,
    do_nothing,
    do_nothing_py,
    do_something,
)
from sentry_streams.pipeline import (
    Batch,
    BatchParser,
    HeadersFilter,
    Map,
    ParquetSerializer,
    Parser,
    streaming_source,
)
from sentry_streams.pipeline.pipeline import DevNullSink, Pipeline

SBC_TOPIC = "snuba-items"
GCS_BUCKET = "arroyo-artifacts"
GCS_SINK_FOLDER = "items-span"
gcs_processor = ItemsSpanProcessor()

pipeline: Pipeline[dict[str, Any]] = (
    streaming_source(name="kafka", stream_name=SBC_TOPIC)
    .apply(
        HeadersFilter(
            name="logs_filter",
            header_name="item_type",
            value=TraceItemType.TRACE_ITEM_TYPE_SPAN,
        )
    )
    # .apply(BatchParser[TraceItem]("batch_parser"))
    # .apply(Map(name="processed_message", function=gcs_processor.process_batch_messages))
    .apply(Parser[TraceItem]("message_parser"))
    # .apply(Map(name="do_something", function=do_nothing))
    # .apply(Map(name="processed_message", function=gcs_processor.process_stream_message))
    .apply(Batch(name="batched_messages", batch_size=100000))
    #.apply(Map(name="count_batch", function=count_batch))
    # .apply(
    #    ParquetSerializer(
    #        name="serializer", schema_fields=gcs_processor.schema_fields_sentrystreams
    #    )
    # )
    .sink(DevNullSink(name="devnull"))
)
