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
    queue_size: Option<usize>,
    buffer_size: Option<usize>,
}

#[pymethods]
impl PyMetricConfig {
    #[new]
    #[pyo3(signature = (host, port, tags=None, queue_size=None, buffer_size=None))]
    fn new(
        host: String,
        port: u16,
        tags: Option<HashMap<String, String>>,
        queue_size: Option<usize>,
        buffer_size: Option<usize>,
    ) -> Self {
        PyMetricConfig {
            host,
            port,
            tags,
            queue_size,
            buffer_size,
        }
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

    #[getter]
    pub fn queue_size(&self) -> Option<usize> {
        self.queue_size
    }

    #[getter]
    pub fn buffer_size(&self) -> Option<usize> {
        self.buffer_size
    }
}
