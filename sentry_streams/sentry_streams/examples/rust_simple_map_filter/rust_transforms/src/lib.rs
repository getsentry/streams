use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Import the macros from rust_streams (the actual crate name)
// In practice: use sentry_streams::{rust_map_function, rust_filter_function, Message}; (when published)
use rust_streams::{convert_via_json, rust_function, Message};

/// IngestMetric structure matching the schema from simple_map_filter.py
/// This would normally be imported from sentry_kafka_schemas in a real implementation
///
/// Types are converted from/to Rust using JSON serialization. The input type must be
/// JSON-serializable and be able to deserialize into this type.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IngestMetric {
    #[serde(rename = "type")]
    pub metric_type: String,
    pub name: String,
    pub value: f64,
    pub tags: std::collections::HashMap<String, String>,
    pub timestamp: u64,
}

// Implement the FromPythonPayload and IntoPythonPayload traits. This decides how IngestMetric is
// going to be converted from the previous step's Python value.
//
// Currently, all values passed between steps are still Python objects, even between two Rust
// steps.
//
// This macro implements these traits by roundtripping values via JSON.
convert_via_json!(IngestMetric);

// Rust equivalent of filter_events() from simple_map_filter.py
rust_function!(RustFilterEvents, IngestMetric, bool, |msg: Message<
    IngestMetric,
>|
 -> bool {
    let (payload, _) = msg.take();
    payload.metric_type == "c"
});

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

convert_via_json!(TransformedIngestMetric);

// Rust equivalent of transform_msg() from simple_map_filter.py
rust_function!(
    RustTransformMsg,
    IngestMetric,
    TransformedIngestMetric,
    |msg: Message<IngestMetric>| -> TransformedIngestMetric {
        let (payload, _) = msg.take();
        TransformedIngestMetric {
            metric_type: payload.metric_type,
            name: payload.name,
            value: payload.value,
            tags: payload.tags,
            timestamp: payload.timestamp,
            transformed: true,
        }
    }
);

// this makes the Rust functions available to Python
#[pymodule]
fn metrics_rust_transforms(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustFilterEvents>()?;
    m.add_class::<RustTransformMsg>()?;
    Ok(())
}
