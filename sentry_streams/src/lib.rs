use pyo3::prelude::*;
mod consumer;
mod kafka_config;
mod routes;
mod steps;
mod strategies;

#[pymodule]
fn rust_streams(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<routes::Route>()?;
    m.add_class::<steps::ArroyoStep>()?;
    m.add_class::<consumer::ArroyoConsumer>()?;
    Ok(())
}
