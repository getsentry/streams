use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
#[derive(Debug, Clone, Copy, Default)]
pub enum InitialOffset {
    #[default]
    #[pyo3(name = "earliest")]
    Earliest,
    #[pyo3(name = "latest")]
    Latest,
    #[pyo3(name = "error")]
    Error,
}

#[pymethods]
impl InitialOffset {
    #[staticmethod]
    pub fn earliest() -> Self {
        InitialOffset::Earliest
    }

    #[staticmethod]
    pub fn latest() -> Self {
        InitialOffset::Latest
    }

    #[staticmethod]
    pub fn error() -> Self {
        InitialOffset::Error
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct OffsetResetConfig {
    #[pyo3(get, set)]
    pub auto_offset_reset: InitialOffset,
    #[pyo3(get, set)]
    pub strict_offset_reset: bool,
}

#[pymethods]
impl OffsetResetConfig {
    #[new]
    fn new(auto_offset_reset: InitialOffset, strict_offset_reset: bool) -> Self {
        OffsetResetConfig {
            auto_offset_reset,
            strict_offset_reset,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyKafkaConfig {
    #[pyo3(get, set)]
    config_map: HashMap<String, String>,
    #[pyo3(get, set)]
    offset_reset_config: Option<OffsetResetConfig>,
}

#[pymethods]
impl PyKafkaConfig {
    #[new]
    fn new(
        config_map: HashMap<String, String>,
        offset_reset_config: Option<OffsetResetConfig>,
    ) -> Self {
        PyKafkaConfig {
            config_map,
            offset_reset_config,
        }
    }
}
