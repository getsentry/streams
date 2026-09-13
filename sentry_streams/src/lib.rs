use pyo3::prelude::*;
mod batch_step;
mod broadcaster;
mod callers;
mod commit_policy;
mod committable;
mod consumer;
mod dev_null_sink;
mod filter_step;
mod gcs_writer;
mod header_filter_step;
mod kafka_config;
mod messages;
mod metrics;
mod metrics_config;
mod mocks;
mod operators;
mod pipeline_stats;
mod py_record_batch;
mod python_operator;
mod routers;
mod routes;
mod sinks;
mod store_sinks;
mod time_helpers;
mod transformer;
mod utils;
mod watermark;

#[doc(hidden)]
pub mod ffi;
pub use ffi::Message;
#[cfg(feature = "cli")]
pub mod run;

#[cfg(test)]
mod fake_strategy;
#[cfg(test)]
mod testutils;

#[pymodule]
fn rust_streams(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<routes::Route>()?;
    m.add_class::<operators::RuntimeOperator>()?;
    m.add_class::<kafka_config::PyKafkaConsumerConfig>()?;
    m.add_class::<kafka_config::PyKafkaProducerConfig>()?;
    m.add_class::<kafka_config::InitialOffset>()?;
    m.add_class::<consumer::ArroyoConsumer>()?;
    m.add_class::<consumer::DlqConfig>()?;
    m.add_class::<metrics_config::PyMetricConfig>()?;
    m.add_class::<messages::PyAnyMessage>()?;
    m.add_class::<messages::RawMessage>()?;
    m.add_class::<messages::PyWatermark>()?;
    m.add_class::<py_record_batch::PyRecordBatch>()?;
    Ok(())
}

/// Phase 0 of the Arrow batch parser plan: prove the new dependencies are wired
/// correctly before any code depends on them.
///
/// In particular this pins the two assumptions the plan rests on:
///   * `sentry-kafka-schemas` with `default-features = false` still exposes the
///     topic -> schema lookup (only `validate_protobuf` sits behind
///     `type_generation`), and
///   * `sentry_protos` and `prost` agree on a `prost` version, so a generated
///     type can actually be decoded through the `prost::Message` trait we import.
///
/// See `docs/design/arrow-batch-parser.md`.
#[cfg(test)]
mod dependency_wiring_tests {
    use prost::Message;
    use sentry_kafka_schemas::{get_schema, SchemaType};
    use sentry_protos::snuba::v1::TraceItem;

    #[test]
    fn snuba_items_resolves_to_the_trace_item_resource() {
        let schema = get_schema("snuba-items", None).expect("snuba-items must have a schema");

        assert_eq!(schema.schema_type, SchemaType::Protobuf);
        assert_eq!(
            schema.raw_schema(),
            "sentry_protos.snuba.v1.trace_item_pb2.TraceItem"
        );
    }

    #[test]
    fn sentry_protos_types_decode_through_our_prost() {
        let item = TraceItem {
            organization_id: 7,
            ..Default::default()
        };

        let encoded = item.encode_to_vec();
        let decoded = TraceItem::decode(encoded.as_slice()).expect("round trip");

        assert_eq!(decoded.organization_id, 7);
    }
}
