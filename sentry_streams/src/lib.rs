use pyo3::prelude::*;
mod callers;
mod consumer;
mod filter_step;
mod kafka_config;
mod operators;
mod routers;
mod routes;
mod sinks;
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
    Ok(())
}
