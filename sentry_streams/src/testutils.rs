use pyo3::prelude::*;

pub fn initialize_python() {
    let python_executable = std::env::var("STREAMS_TEST_PYTHONEXECUTABLE").unwrap();
    let python_path = std::env::var("STREAMS_TEST_PYTHONPATH").unwrap();
    let python_path: Vec<_> = python_path.split(':').map(String::from).collect();

    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", python_executable)?;
        PyModule::import(py, "sys")?.setattr("path", python_path)?;
        Ok(())
    })
    .unwrap();
}
