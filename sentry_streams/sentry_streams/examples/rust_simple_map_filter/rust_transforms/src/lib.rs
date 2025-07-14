use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Import the macros from rust_streams (the actual crate name)
// In practice: use sentry_streams::{rust_map_function, rust_filter_function, Message}; (when published)
use rust_streams::{rust_filter_function, rust_map_function, Message};

/// IngestMetric structure matching the schema from simple_map_filter.py
/// This would normally be imported from sentry_kafka_schemas in a real implementation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestMetric {
    #[serde(rename = "type")]
    pub metric_type: String,
    pub name: String,
    pub value: f64,
    pub tags: std::collections::HashMap<String, String>,
    pub timestamp: u64,
}

// Rust equivalent of filter_events() from simple_map_filter.py
rust_filter_function!(RustFilterEvents, IngestMetric, |msg: Message<
    IngestMetric,
>|
 -> bool {
    // Direct access to strongly typed payload - no JSON conversion needed!
    msg.payload.metric_type == "c"
});

// Enhanced IngestMetric with transform flag for output
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransformedIngestMetric {
    #[serde(rename = "type")]
    pub metric_type: String,
    pub name: String,
    pub value: f64,
    pub tags: std::collections::HashMap<String, String>,
    pub timestamp: u64,
    pub transformed: bool,
}

// Rust equivalent of transform_msg() from simple_map_filter.py
rust_map_function!(
    RustTransformMsg,
    IngestMetric,
    TransformedIngestMetric,
    |msg: Message<IngestMetric>| -> Message<TransformedIngestMetric> {
        // Direct access to strongly typed payload - no JSON conversion needed!
        let transformed_payload = TransformedIngestMetric {
            metric_type: msg.payload.metric_type,
            name: msg.payload.name,
            value: msg.payload.value,
            tags: msg.payload.tags,
            timestamp: msg.payload.timestamp,
            transformed: true,
        };

        Message::new(transformed_payload, msg.headers, msg.timestamp, msg.schema)
    }
);

// this makes the Rust functions available to Python
#[pymodule]
fn metrics_rust_transforms(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustFilterEvents>()?;
    m.add_class::<RustTransformMsg>()?;
    Ok(())
}
