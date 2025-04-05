use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::config::KafkaConfig;
use sentry_arroyo::backends::kafka::InitialOffset as KafkaInitialOffset;
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

impl Into<KafkaInitialOffset> for InitialOffset {
    fn into(self) -> KafkaInitialOffset {
        match self {
            InitialOffset::Earliest => KafkaInitialOffset::Earliest,
            InitialOffset::Latest => KafkaInitialOffset::Latest,
            InitialOffset::Error => KafkaInitialOffset::Error,
        }
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
pub struct PyKafkaConsumerConfig {
    bootstrap_servers: Vec<String>,
    group_id: String,
    auto_offset_reset: InitialOffset,
    strict_offset_reset: bool,
    max_poll_interval_ms: usize,
    override_params: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyKafkaConsumerConfig {
    #[new]
    fn new(
        bootstrap_servers: Vec<String>,
        group_id: String,
        auto_offset_reset: InitialOffset,
        strict_offset_reset: bool,
        max_poll_interval_ms: usize,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        PyKafkaConsumerConfig {
            bootstrap_servers,
            group_id,
            auto_offset_reset,
            strict_offset_reset,
            max_poll_interval_ms,
            override_params,
        }
    }
}

impl From<PyKafkaConsumerConfig> for KafkaConfig {
    fn from(py_config: PyKafkaConsumerConfig) -> Self {
        KafkaConfig::new_consumer_config(
            py_config.bootstrap_servers,
            py_config.group_id,
            py_config.auto_offset_reset.into(),
            py_config.strict_offset_reset,
            py_config.max_poll_interval_ms,
            py_config.override_params,
        )
    }
}
