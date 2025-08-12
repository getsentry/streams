use std::{collections::BTreeMap, ffi::OsString};

use pyo3::{prelude::*, types::PyList};

use crate::consumer::{Callable, RustNativeFns};

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

struct MyCallable {}

impl Callable for MyCallable {
    fn call(&self) {
        println!("Hello world!");
    }
}

fn load_physical_plan(py_config: PyConfig, plan_config: PhysicalPlanConfig) -> PyResult<()> {
    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", py_config.exec_path)?;
        PyModule::import(py, "sys")?.setattr("path", py_config.module_paths)?;
        Ok(())
    })?;

    let fns = RustNativeFns {
        fns: BTreeMap::<String, Box<dyn Callable>>::from([(
            "fn".to_string(),
            Box::new(MyCallable {}) as Box<dyn Callable>,
        )]),
    };

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
            .call_method1("add_rust_fns", (fns,))?;
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
