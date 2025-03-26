use pyo3::prelude::*;
mod routes;
mod steps;

#[pymodule]
fn rust_streams(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<routes::Route>()?;
    m.add_class::<routes::RoutedValue>()?;

    let sub = PyModule::new(py, "steps")?;
    steps::rust_steps(&sub)?;
    m.add_submodule(&sub)?;
    Ok(())
}
