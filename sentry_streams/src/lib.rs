use pyo3::prelude::*;
mod broadcaster;
mod callers;
mod committable;
mod consumer;
mod filter_step;
mod gcs_writer;
mod kafka_config;
pub mod macros;
pub mod message;
mod messages;
mod operators;
mod python_operator;
mod routers;
mod routes;
mod sinks;
mod store_sinks;
pub mod transformer;
mod utils;
mod watermark;

// Re-export types so they can be used by external crates
pub use message::Message;
// Macros are automatically exported at crate root due to #[macro_export]

/// Convert a Python streaming message to a typed Rust Message format
/// This function handles the conversion for any type that implements serde::Deserialize
pub fn convert_py_message_to_rust<T>(
    py: pyo3::Python,
    py_msg: &pyo3::Py<pyo3::PyAny>,
) -> pyo3::PyResult<crate::message::Message<T>>
where
    T: serde::de::DeserializeOwned,
{
    // Extract payload and serialize to JSON for safe deserialization
    let payload_obj = py_msg.bind(py).getattr("payload")?;
    let payload_json = py
        .import("json")?
        .getattr("dumps")?
        .call1((payload_obj,))?
        .extract::<String>()?;

    // Parse JSON into the desired type
    let payload_value: T = serde_json::from_str(&payload_json).map_err(|e| {
        pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Failed to parse JSON: {}",
            e
        ))
    })?;

    // Extract headers
    let headers_py: Vec<(String, Vec<u8>)> = py_msg.bind(py).getattr("headers")?.extract()?;

    // Extract timestamp
    let timestamp: f64 = py_msg.bind(py).getattr("timestamp")?.extract()?;

    // Extract schema
    let schema: Option<String> = py_msg.bind(py).getattr("schema")?.extract()?;

    Ok(crate::message::Message::new(
        payload_value,
        headers_py,
        timestamp,
        schema,
    ))
}

/// Convert a typed Rust Message back to a Python streaming message
/// This function handles the conversion for any type that implements serde::Serialize
pub fn convert_rust_message_to_py<T>(
    py: pyo3::Python,
    rust_msg: crate::message::Message<T>,
) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>
where
    T: serde::Serialize,
{
    // Convert payload to JSON string, then to Python object
    let payload_json = serde_json::to_string(&rust_msg.payload).map_err(|e| {
        pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Failed to serialize JSON: {}",
            e
        ))
    })?;
    let payload_obj = py
        .import("json")?
        .getattr("loads")?
        .call1((payload_json,))?;

    // Create a new Python message with the transformed payload
    let py_msg_class = py
        .import("sentry_streams.rust_streams")?
        .getattr("PyAnyMessage")?;

    let result = py_msg_class
        .call1((
            payload_obj,
            rust_msg.headers,
            rust_msg.timestamp,
            rust_msg.schema,
        ))?
        .unbind();

    Ok(result)
}

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
    m.add_class::<messages::PyAnyMessage>()?;
    m.add_class::<messages::RawMessage>()?;
    m.add_class::<messages::PyWatermark>()?;
    Ok(())
}
