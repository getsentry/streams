//! This module provides an implementation of the structs to
//! configure Kafka producers and consumers that can be used in Python
//! code.
//!
//! Arroyo rust provides similar structures, but those are not pyclass
//! so we need an alternative implementation.

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

// Turns the python exposed InitialOffset into the Arroyo
// InitialOffset.
impl From<InitialOffset> for KafkaInitialOffset {
    fn from(offset: InitialOffset) -> Self {
        match offset {
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

// Python version of the Kafka consumer configuration
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

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyKafkaProducerConfig {
    bootstrap_servers: Vec<String>,
    override_params: Option<HashMap<String, String>>,
}

#[pymethods]
impl PyKafkaProducerConfig {
    #[new]
    fn new(
        bootstrap_servers: Vec<String>,
        override_params: Option<HashMap<String, String>>,
    ) -> Self {
        PyKafkaProducerConfig {
            bootstrap_servers,
            override_params,
        }
    }
}

impl From<PyKafkaProducerConfig> for KafkaConfig {
    fn from(py_config: PyKafkaProducerConfig) -> Self {
        KafkaConfig::new_producer_config(py_config.bootstrap_servers, py_config.override_params)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::config::ClientConfig as RdKafkaConfig;

    #[test]
    fn test_pykafka_consumer_config_creation() {
        let bootstrap_servers = vec!["localhost:9092".to_string()];
        let group_id = "test_group".to_string();
        let auto_offset_reset = InitialOffset::Earliest;
        let strict_offset_reset = false;
        let max_poll_interval_ms = 300000;
        let override_params = Some(HashMap::from([("key".to_string(), "value".to_string())]));

        let config = PyKafkaConsumerConfig::new(
            bootstrap_servers.clone(),
            group_id.clone(),
            auto_offset_reset,
            strict_offset_reset,
            max_poll_interval_ms,
            override_params.clone(),
        );

        let kafka_config: KafkaConfig = config.into();
        let rd_config: RdKafkaConfig = kafka_config.into();
        assert_eq!(rd_config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(rd_config.get("group.id"), Some("test_group"));
        assert_eq!(rd_config.get("auto.offset.reset"), Some("earliest"));
        assert_eq!(rd_config.get("max.poll.interval.ms"), Some("300000"));
    }

    #[test]
    fn test_pykafka_producer_config_creation() {
        let bootstrap_servers = vec!["localhost:9092".to_string()];
        let override_params = Some(HashMap::from([("key".to_string(), "value".to_string())]));

        let config = PyKafkaProducerConfig::new(bootstrap_servers.clone(), override_params.clone());

        let kafka_config: KafkaConfig = config.into();
        let rd_config: RdKafkaConfig = kafka_config.into();
        assert_eq!(rd_config.get("bootstrap.servers"), Some("localhost:9092"));
    }
}
