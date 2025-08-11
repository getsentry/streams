use std::ffi::OsString;

use pyo3::{prelude::*, types::PyList};

use crate::consumer::add_fn;

pub struct PyConfig {
    pub exec_path: OsString,
    pub module_paths: Vec<OsString>,
}

pub struct PhysicalPlanConfig {
    pub adapter_name: String,
    pub config_file: OsString,
    pub segment_id: Option<String>,
    pub application_name: String,
}

fn load_physical_plan(py_config: PyConfig, plan_config: PhysicalPlanConfig) -> PyResult<()> {
    add_fn("abc".into(), 1);

    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", py_config.exec_path)?;
        PyModule::import(py, "sys")?.setattr("path", py_config.module_paths)?;
        Ok(())
    })?;

    Python::with_gil(|py| {
        py.import("sentry_streams.runner")?
            .getattr("load_physical_plan")?
            .call1((
                plan_config.adapter_name,
                plan_config.config_file,
                plan_config.segment_id,
                plan_config.application_name,
            ))?
            .downcast::<PyList>()?
            .get_item(0)?
            .getattr("read_fn")?
            .call1(())?;
        Ok(())
    })
}

pub fn run(
    py_config: PyConfig,
    plan_config: PhysicalPlanConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    pyo3::prepare_freethreaded_python();
    load_physical_plan(py_config, plan_config)?;
    Ok(())
}
