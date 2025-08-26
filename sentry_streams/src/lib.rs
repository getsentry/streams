use pyo3::prelude::*;
mod broadcaster;
mod callers;
mod commit_policy;
mod committable;
mod consumer;
mod filter_step;
mod gcs_writer;
mod kafka_config;
mod messages;
mod metrics;
mod metrics_config;
mod mocks;
mod operators;
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
    m.add_class::<metrics_config::PyMetricConfig>()?;
    m.add_class::<messages::PyAnyMessage>()?;
    m.add_class::<messages::RawMessage>()?;
    m.add_class::<messages::PyWatermark>()?;
    Ok(())
}
