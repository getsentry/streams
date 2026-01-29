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

    /// The logging level
    #[arg(short, long)]
    pub log_level: String,

    /// The stream adapter to use (e.g., "arroyo", "rust_arroyo")
    #[arg(short, long)]
    pub adapter: String,

    /// The deployment config file path. Each config file currently corresponds to a specific pipeline.
    #[arg(short, long)]
    pub config: OsString,

    /// The segment id to run the pipeline for
    #[arg(short, long)]
    pub segment_id: Option<String>,

    /// The application file path
    pub application: String,
}

pub fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        py_config,
        runtime_config,
    } = args;

    // Set PYTHONPATH environment variable if module_paths are provided
    // This ensures subprocesses spawned via multiprocessing inherit the paths
    if let Some(ref module_paths) = py_config.module_paths {
        let separator = if cfg!(windows) { ";" } else { ":" };
        let path_string = module_paths
            .iter()
            .filter_map(|p| p.to_str())
            .collect::<Vec<_>>()
            .join(separator);

        std::env::set_var("PYTHONPATH", path_string);
    }

    traced_with_gil!(|py| -> PyResult<()> {
        if let Some(exec_path) = py_config.exec_path {
            PyModule::import(py, "sys")?.setattr("executable", exec_path)?;
        }
        if let Some(module_paths) = py_config.module_paths {
            PyModule::import(py, "sys")?.setattr("path", module_paths)?;
        }
        py.import("sentry_streams.runner")?
            .getattr("run_with_config_file")?
            .call1((
                runtime_config.name,
                runtime_config.log_level,
                runtime_config.adapter,
                runtime_config.config.to_str().ok_or_else(|| {
                    pyo3::exceptions::PyValueError::new_err("Invalid config file path")
                })?,
                runtime_config.segment_id,
                runtime_config.application,
            ))?;

        Ok(())
    })?;

    Ok(())
}
