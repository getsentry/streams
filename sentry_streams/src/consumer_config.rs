//! Configuration of the source an [`crate::consumer::ArroyoConsumer`] reads from.
//!
//! A consumer either reads from Kafka or, when profiling a pipeline, from the
//! synthetic [`FakeConsumer`]. Both cases are described by [`ConsumerConfig`],
//! so the rest of the runtime does not have to care which one is in use.
//!
//! [`build_consumer`] turns a [`ConsumerConfig`] into the pair of objects the
//! Arroyo `StreamProcessor` needs: the `Consumer` itself and the `ConsumerState`
//! holding the processing strategy.

use crate::fake_consumer::{FakeConsumer, PyFakeConsumerConfig};
use crate::kafka_config::PyKafkaConsumerConfig;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::KafkaConsumer;
use sentry_arroyo::backends::{AssignmentCallbacks, Consumer};
use sentry_arroyo::processing::dlq::DlqPolicy;
use sentry_arroyo::processing::strategies::ProcessingStrategyFactory;
use sentry_arroyo::processing::{Callbacks, ConsumerState};
use sentry_arroyo::types::{Partition, Topic};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// How a consumer gets its messages.
///
/// This is exposed to Python so the adapter picks a variant when building an
/// `ArroyoConsumer`, rather than passing several mutually exclusive configs.
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub enum ConsumerConfig {
    /// Read from a Kafka topic.
    #[pyo3(name = "Kafka")]
    Kafka { config: PyKafkaConsumerConfig },

    /// Generate synthetic messages locally, for profiling a pipeline without Kafka.
    #[pyo3(name = "Fake")]
    Fake { config: PyFakeConsumerConfig },
}

impl ConsumerConfig {
    /// The consumer group the watermark commit step reports on.
    ///
    /// In fake mode there is no consumer group, so the caller's fallback (the
    /// source name) is used instead. Commits hit the no-op FakeConsumer anyway.
    pub fn consumer_group_or(&self, default: &str) -> String {
        match self {
            ConsumerConfig::Kafka { config } => config.group_id().to_string(),
            ConsumerConfig::Fake { .. } => default.to_string(),
        }
    }
}

/// The Arroyo objects needed to build a `StreamProcessor`, as produced by
/// [`build_consumer`].
pub struct BuiltConsumer {
    pub consumer: Box<dyn Consumer<KafkaPayload, Callbacks<KafkaPayload>>>,

    pub consumer_state: ConsumerState<KafkaPayload>,

    /// Set by the fake consumer once it has produced all of its messages.
    /// `None` in Kafka mode, which never stops on its own. The caller watches
    /// this flag to shut the processor down.
    pub done: Option<Arc<AtomicBool>>,
}

/// Builds the consumer and its state from a [`ConsumerConfig`].
///
/// The `dlq_policy` only applies to Kafka: the fake consumer produces
/// well-formed messages and has no topic to forward invalid ones to.
pub fn build_consumer(
    config: &ConsumerConfig,
    topic: &str,
    factory: Box<dyn ProcessingStrategyFactory<KafkaPayload>>,
    dlq_policy: Option<DlqPolicy<KafkaPayload>>,
) -> BuiltConsumer {
    match config {
        ConsumerConfig::Kafka { config } => {
            let consumer_state = ConsumerState::new(factory, dlq_policy);
            let callbacks = Callbacks(consumer_state.clone());
            let consumer =
                KafkaConsumer::new(config.clone().into(), &[Topic::new(topic)], callbacks)
                    .expect("Failed to create Kafka consumer");

            BuiltConsumer {
                consumer: Box::new(consumer),
                consumer_state,
                done: None,
            }
        }
        ConsumerConfig::Fake { config } => {
            let done = Arc::new(AtomicBool::new(false));
            let consumer_state = ConsumerState::new(factory, None);

            // The processing strategy is normally created by the Kafka consumer
            // on partition assignment. The fake consumer performs no assignment,
            // so bootstrap the strategy explicitly for its single partition.
            Callbacks(consumer_state.clone())
                .on_assign(HashMap::from([(Partition::new(Topic::new(topic), 0), 0)]));

            let consumer = FakeConsumer::new(topic, config.clone(), done.clone());

            BuiltConsumer {
                consumer: Box::new(consumer),
                consumer_state,
                done: Some(done),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka_config::InitialOffset;

    fn kafka_config() -> ConsumerConfig {
        ConsumerConfig::Kafka {
            config: PyKafkaConsumerConfig::new(
                vec!["localhost:9092".to_string()],
                "my-group".to_string(),
                InitialOffset::Earliest,
                false,
                1000,
                None,
            ),
        }
    }

    fn fake_config() -> ConsumerConfig {
        ConsumerConfig::Fake {
            config: PyFakeConsumerConfig::new(128, 100.0, 10),
        }
    }

    #[test]
    fn test_consumer_group_from_kafka_config() {
        assert_eq!(kafka_config().consumer_group_or("source"), "my-group");
    }

    #[test]
    fn test_consumer_group_falls_back_in_fake_mode() {
        assert_eq!(fake_config().consumer_group_or("source"), "source");
    }
}
