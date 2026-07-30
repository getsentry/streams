//! A fake Arroyo [`Consumer`] used to profile streaming pipelines without Kafka.
//!
//! Instead of reading from a Kafka topic, [`FakeConsumer`] synthesises messages
//! containing random bytes at a fixed rate and terminates after a fixed number
//! of messages. It is wired into [`crate::consumer::ArroyoConsumer`] through
//! [`PyFakeConsumerConfig`] and used in place of the real `KafkaConsumer`.
//!
//! The consumer deliberately implements *only* `poll`. It stores no assignment
//! callbacks and performs no revocation handling: the processing strategy is
//! bootstrapped explicitly by the caller (see `ArroyoConsumer::run`).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use pyo3::prelude::*;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::{Consumer, ConsumerError};
use sentry_arroyo::types::{BrokerMessage, Partition, Topic};

/// Configuration of the fake consumer, exposed to Python.
///
/// `messages_per_second` paces the emission of messages, `num_messages` is the
/// total number of messages to emit before the consumer signals that it is done.
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct PyFakeConsumerConfig {
    #[pyo3(get)]
    pub message_size_bytes: usize,
    #[pyo3(get)]
    pub messages_per_second: f64,
    #[pyo3(get)]
    pub num_messages: usize,
}

#[pymethods]
impl PyFakeConsumerConfig {
    #[new]
    pub fn new(message_size_bytes: usize, messages_per_second: f64, num_messages: usize) -> Self {
        PyFakeConsumerConfig {
            message_size_bytes,
            messages_per_second,
            num_messages,
        }
    }
}

/// A [`Consumer`] that produces messages of random bytes at a fixed rate.
///
/// It emits exactly `num_messages` messages on a single partition (index 0 of
/// `topic`), then sets the shared `done` flag and returns `Ok(None)` forever.
/// The `done` flag is watched by `ArroyoConsumer::run` which signals the
/// `StreamProcessor` to shut down.
pub struct FakeConsumer {
    partition: Partition,
    message_size_bytes: usize,
    num_messages: usize,
    period: Duration,
    produced: u64,
    next_send: Instant,
    done: Arc<AtomicBool>,
    rng: StdRng,
}

impl FakeConsumer {
    pub fn new(topic: &str, config: PyFakeConsumerConfig, done: Arc<AtomicBool>) -> Self {
        let period = if config.messages_per_second > 0.0 {
            Duration::from_secs_f64(1.0 / config.messages_per_second)
        } else {
            Duration::ZERO
        };
        FakeConsumer {
            partition: Partition::new(Topic::new(topic), 0),
            message_size_bytes: config.message_size_bytes,
            num_messages: config.num_messages,
            period,
            produced: 0,
            // Emit the first message immediately on the first poll.
            next_send: Instant::now(),
            done,
            rng: StdRng::from_entropy(),
        }
    }

    fn random_payload(&mut self) -> KafkaPayload {
        let mut buf = vec![0u8; self.message_size_bytes];
        self.rng.fill_bytes(&mut buf);
        KafkaPayload::new(None, None, Some(buf))
    }
}

impl<C> Consumer<KafkaPayload, C> for FakeConsumer {
    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<BrokerMessage<KafkaPayload>>, ConsumerError> {
        if self.produced as usize >= self.num_messages {
            self.done.store(true, Ordering::Relaxed);
            return Ok(None);
        }

        // Rate limiting: wait until the next message is due, bounded by the
        // poll timeout so the processor stays responsive to shutdown.
        let now = Instant::now();
        if now < self.next_send {
            let wait = self.next_send - now;
            let sleep_dur = match timeout {
                Some(t) => wait.min(t),
                None => wait,
            };
            std::thread::sleep(sleep_dur);
            if Instant::now() < self.next_send {
                return Ok(None);
            }
        }

        let payload = self.random_payload();
        let message = BrokerMessage::new(payload, self.partition, self.produced, Utc::now());
        self.produced += 1;
        self.next_send = Instant::now() + self.period;
        Ok(Some(message))
    }

    fn pause(&mut self, _partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        Ok(())
    }

    fn resume(&mut self, _partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        Ok(HashSet::new())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        Ok(HashMap::from([(self.partition, self.produced)]))
    }

    fn seek(&self, _offsets: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        Ok(())
    }

    fn commit_offsets(&mut self, _positions: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentry_arroyo::processing::Callbacks;

    fn make_consumer(num_messages: usize, message_size_bytes: usize) -> FakeConsumer {
        FakeConsumer::new(
            "fake",
            PyFakeConsumerConfig {
                message_size_bytes,
                // High rate so the test does not spend real time sleeping.
                messages_per_second: 1_000_000.0,
                num_messages,
            },
            Arc::new(AtomicBool::new(false)),
        )
    }

    // `Callbacks<KafkaPayload>` is the concrete `C` used in production; pin the
    // generic to it so the calls resolve.
    fn poll(
        consumer: &mut FakeConsumer,
    ) -> Result<Option<BrokerMessage<KafkaPayload>>, ConsumerError> {
        <FakeConsumer as Consumer<KafkaPayload, Callbacks<KafkaPayload>>>::poll(
            consumer,
            Some(Duration::from_millis(10)),
        )
    }

    #[test]
    fn test_emits_n_messages_then_none() {
        let done = Arc::new(AtomicBool::new(false));
        let mut consumer = FakeConsumer::new(
            "fake",
            PyFakeConsumerConfig {
                message_size_bytes: 16,
                messages_per_second: 1_000_000.0,
                num_messages: 3,
            },
            done.clone(),
        );

        for expected_offset in 0..3u64 {
            let msg = poll(&mut consumer)
                .unwrap()
                .expect("expected a message before reaching num_messages");
            assert_eq!(msg.offset, expected_offset);
            assert_eq!(msg.partition.index, 0);
            assert_eq!(msg.payload.payload().unwrap().len(), 16);
        }

        // After num_messages, poll returns None and flags completion.
        assert!(poll(&mut consumer).unwrap().is_none());
        assert!(done.load(Ordering::Relaxed));
    }

    #[test]
    fn test_does_not_exceed_num_messages() {
        let mut consumer = make_consumer(5, 8);
        let mut count = 0;
        for _ in 0..50 {
            if poll(&mut consumer).unwrap().is_some() {
                count += 1;
            }
        }
        assert_eq!(count, 5);
    }
}
