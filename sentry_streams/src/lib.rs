use pyo3::prelude::*;
mod consumer;
mod kafka_config;
mod routes;
mod steps;

#[pymodule]
fn rust_streams(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<routes::Route>()?;
    m.add_class::<steps::MapStep>()?;
    m.add_class::<steps::FilterStep>()?;
    m.add_class::<steps::RouterStep>()?;
    m.add_class::<steps::StreamSinkStep>()?;
    m.add_class::<consumer::ArroyoConsumer>()?;
    Ok(())
}
