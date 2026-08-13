use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::Stream;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::types::{Headers, KafkaPayload};
use sentry_arroyo::processing::stream::source::KafkaSource;
use sentry_arroyo::processing::stream::{
    MessageMetadata, OffsetCommitter, PipelineEnvelope, PullSource, StageResult,
};
use sentry_arroyo::types::{Partition, Topic};

/// A test message with payload bytes and optional headers.
#[pyclass]
#[derive(Clone)]
pub struct PyTestMessage {
    #[pyo3(get)]
    pub payload: Vec<u8>,
    #[pyo3(get)]
    pub headers: HashMap<String, Vec<u8>>,
}

#[pymethods]
impl PyTestMessage {
    #[new]
    fn new(payload: Vec<u8>, headers: Option<HashMap<String, Vec<u8>>>) -> Self {
        Self {
            payload,
            headers: headers.unwrap_or_default(),
        }
    }
}

/// Source config enum — Python constructs this, Rust builds the source.
#[pyclass]
pub enum PullSourceConfig {
    #[pyo3(constructor = (config, topic))]
    Kafka {
        config: crate::kafka_config::PyKafkaConsumerConfig,
        topic: String,
    },

    #[pyo3(constructor = (messages))]
    Test { messages: Vec<PyTestMessage> },
}

impl PullSourceConfig {
    pub fn build(&self, py: Python<'_>) -> Box<dyn PullSource> {
        match self {
            PullSourceConfig::Kafka { config, topic } => {
                let kafka_config = config.clone().into();
                Box::new(KafkaSource::new(kafka_config, &[Topic::new(topic)]))
            }
            PullSourceConfig::Test { messages } => {
                Box::new(VecSource::from_test_messages(messages.clone()))
            }
        }
    }
}

/// In-memory source for testing.
pub struct VecSource {
    messages: Mutex<Vec<StageResult<KafkaPayload>>>,
    committer: NoOpCommitter,
}

impl VecSource {
    pub fn new(messages: Vec<StageResult<KafkaPayload>>) -> Self {
        Self {
            messages: Mutex::new(messages),
            committer: NoOpCommitter,
        }
    }

    pub fn from_test_messages(test_messages: Vec<PyTestMessage>) -> Self {
        let messages = test_messages
            .into_iter()
            .enumerate()
            .map(|(i, msg)| {
                let headers = if msg.headers.is_empty() {
                    None
                } else {
                    let mut h = Headers::new();
                    for (key, value) in &msg.headers {
                        h = h.insert(key, Some(value.clone()));
                    }
                    Some(h)
                };

                let kp = KafkaPayload::new(None, headers, Some(msg.payload));
                let md = MessageMetadata {
                    partition: Partition::new(Topic::new("test"), 0),
                    offset: i as u64,
                    timestamp: chrono::Utc::now(),
                };
                StageResult::Emit(PipelineEnvelope::new(kp.clone(), md, Arc::new(kp)))
            })
            .collect();

        Self::new(messages)
    }
}

impl PullSource for VecSource {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = StageResult<KafkaPayload>> + '_>> {
        let messages: Vec<_> = self.messages.lock().unwrap().drain(..).collect();
        Box::pin(futures::stream::iter(messages))
    }

    fn committer(&self) -> &dyn OffsetCommitter {
        &self.committer
    }

    fn shutdown(&self) {
        // No-op for test source
    }
}

/// No-op committer for testing.
struct NoOpCommitter;

impl OffsetCommitter for NoOpCommitter {
    fn commit_offsets(
        &self,
        _positions: &HashMap<Partition, u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        Ok(())
    }
}
