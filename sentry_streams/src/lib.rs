use pyo3::prelude::*;
mod callers;
mod consumer;
mod filter_step;
mod gcs_writer;
mod kafka_config;
mod messages;
mod operators;
mod python_operator;
mod routers;
mod routes;
mod sinks;
mod store_sinks;
mod transformer;

#[cfg(test)]
mod fake_strategy;
#[cfg(test)]
mod test_operators;

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
    Ok(())
}
