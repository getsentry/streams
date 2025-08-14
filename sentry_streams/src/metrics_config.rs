//! This module provides an implementation of the structs to
//! configure metrics backends.
//!
//! Arroyo rust provides similar structures, but those are not pyclass
//! so we need an alternative implementation.

use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyMetricConfig {
    host: String,
    port: u16,
    tags: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyMetricConfig {
    #[new]
    fn new(host: String, port: u16, tags: Option<HashMap<String, String>>) -> Self {
        PyMetricConfig { host, port, tags }
    }

    #[getter]
    pub fn host(&self) -> &str {
        &self.host
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[getter]
    pub fn tags(&self) -> Option<HashMap<String, String>> {
        self.tags.clone()
    }
}
