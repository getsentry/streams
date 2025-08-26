use std::ffi::OsString;

use clap::Parser;
use pyo3::prelude::*;

use crate::utils::traced_with_gil;

#[derive(Parser, Debug)]
pub struct Args {
    #[command(flatten)]
    pub py_config: PyConfig,

    #[command(flatten)]
    pub runtime_config: RuntimeConfig,
}

impl Args {
    pub fn parse() -> Self {
        Parser::parse()
    }
}

#[derive(Parser, Debug)]
pub struct PyConfig {
    /// Path of the python interpreter executable
    #[arg(short, long)]
    pub exec_path: Option<OsString>,

    #[arg(short, long)]
    /// Paths to the relevant python modules
    pub module_paths: Option<Vec<OsString>>,
}

#[derive(Parser, Debug)]
pub struct RuntimeConfig {
    /// The name of the Sentry Streams application
    #[arg(short, long)]
    pub name: String,

    /// The name of the Sentry Streams application
    #[arg(short, long)]
    pub log_level: String,

    /// The name of the adapter
    #[arg(short, long)]
    pub adapter_name: String,

    /// The deployment config file path. Each config file currently corresponds to a specific pipeline.
    #[arg(short, long)]
    pub config_file: OsString,

    /// The segment id to run the pipeline for
    #[arg(short, long)]
    pub segment_id: Option<String>,

    /// The name of the application
    pub application_name: String,
}

pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        py_config,
        runtime_config,
    } = args;

    traced_with_gil!(|py| -> PyResult<()> {
        if let Some(exec_path) = py_config.exec_path {
            PyModule::import(py, "sys")?.setattr("executable", exec_path)?;
        }
        if let Some(module_paths) = py_config.module_paths {
            PyModule::import(py, "sys")?.setattr("path", module_paths)?;
        }
        Ok(())
    })?;

    let runtime: Py<PyAny> = traced_with_gil!(|py| {
        let runtime = py
            .import("sentry_streams.runner")?
            .getattr("load_runtime")?
            .call1((
                runtime_config.name,
                runtime_config.log_level,
                runtime_config.adapter_name,
                runtime_config.config_file,
                runtime_config.segment_id,
                runtime_config.application_name,
            ))?
            .unbind();
        PyResult::Ok(runtime)
    })?;

    traced_with_gil!(|py| {
        runtime
            .bind(py)
            .call_method0("run")
            .expect("Unable to start runtime");
    });

    Ok(())
}
