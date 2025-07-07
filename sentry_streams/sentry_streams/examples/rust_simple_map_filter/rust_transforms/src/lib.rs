use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Import the macros from rust_streams (the actual crate name)
// In practice: use sentry_streams::{rust_map_function, rust_filter_function}; (when published)
use rust_streams::ffi::Message;
use rust_streams::rust_filter_function;
use rust_streams::rust_map_function;

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
rust_filter_function!(RustFilterEvents, serde_json::Value, |msg: Message<
    serde_json::Value,
>|
 -> bool {
    // Deserialize JSON payload to IngestMetric struct
    let payload: IngestMetric = serde_json::from_value(msg.payload).unwrap();

    // TODO: Fix segfault in Debug formatting - this should not crash even with binary data
    // println!("Seen in filter: {:?}", msg);
    // Same logic as Python version: return bool(msg.payload["type"] == "c")
    payload.metric_type == "c"
});

// Rust equivalent of transform_msg() from simple_map_filter.py
rust_map_function!(
    RustTransformMsg,
    serde_json::Value,
    serde_json::Value, // Output as a flexible JSON value like the Python version
    |msg: Message<serde_json::Value>| -> Message<serde_json::Value> {
        // Deserialize JSON payload to IngestMetric struct
        let payload: IngestMetric = serde_json::from_value(msg.payload).unwrap();

        // TODO: Fix segfault in Debug formatting - this should not crash even with binary data
        // println!("Seen in map: {:?}", msg);
        // Convert IngestMetric to JSON, then add "transformed": true
        // This matches the Python logic: {**msg.payload, "transformed": True}
        let mut result = serde_json::to_value(&payload).unwrap();

        if let Some(obj) = result.as_object_mut() {
            obj.insert("transformed".to_string(), serde_json::Value::Bool(true));
        }

        Message::new(result, msg.headers, msg.timestamp, msg.schema)
    }
);

// this makes the Rust functions available to Python
#[pymodule]
fn metrics_rust_transforms(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustFilterEvents>()?;
    m.add_class::<RustTransformMsg>()?;
    Ok(())
}
