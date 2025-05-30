//! This module provides an implementation of the StreamSink pipeline
//! step that produces messages on a Kafka topic.
//!
//! As all the strategies in the Arroyo streaming pipeline adapter,
//! This checks whether a message should be processed or forwarded
//! via the `Route` attribute.
use crate::messages::PyStreamingMessage;
use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::strategies::MessageRejected;
use sentry_arroyo::types::{Message, Topic};

use core::panic;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::processing::strategies::{merge_commit_request, CommitRequest, StrategyError};
use std::time::Duration;

/// Turns a `Message<RoutedValue` passed among streaming primitives into
/// a `Message<KafkaPayload>` to be produced on a Kafka topic.
/// This is needed because what we need to produce on a Kafka topic is
/// only the payload in bytes taken from the `Message<RoutedValue>`.
/// Also this has to be converted from the Python PyAny payload into
/// a Vector of bytes.
fn to_kafka_payload(message: Message<RoutedValue>) -> Message<KafkaPayload> {
    // Convert the RoutedValue to KafkaPayload
    // This is a placeholder implementation
    let payload = Python::with_gil(|py| {
        let payload = &message.payload().payload;
        match payload {
            PyStreamingMessage::PyAnyMessage { .. } => {
                panic!("PyAnyMessage is not supported in KafkaPayload conversion");
            }
            PyStreamingMessage::RawMessage { content } => {
                let payload_content = content.bind(py).getattr("payload").unwrap();
                let py_bytes: &Bound<PyBytes> = payload_content.downcast().unwrap();
                let raw_bytes = py_bytes.as_bytes();
                KafkaPayload::new(None, None, Some(raw_bytes.to_vec()))
            }
        }
    });
    message.replace(payload)
}

/// Implements the StreamSink logic.
///
/// This is an Arroyo strategy that contains two next steps. One
/// is the Kafka producer that turns messages into KafkaPayload and
/// send them to Kafka. The second is the following step in the
/// pipeline that receives all messages that have a `Route` that does
/// not correspond to the one this producer is meant to process.
///
/// The producer is an Arroyo `Produce` strategy, which contains a
/// `Producer` struct and forwards messages to a `CommitOffsets` step
/// TODO: This strategy does not guarantee at least once delivery as
/// it does not wait for the following step to return an offset before
/// committing. It commits everything the Producer produces successfully.
pub struct StreamSink {
    /// The route this strategy processes. Every message not for this
    /// strategy is sent to the `next` strategy without being processed.
    route: Route,
    produce_strategy: Produce<Box<dyn ProcessingStrategy<KafkaPayload>>>,
    next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,

    /// Keeps track of the last message this strategy did not manage to
    /// forward. This is a hack like in `RunTask` to be able to return
    /// the failed message to the previous strategy after it has been
    /// passed to the transformer function and been mutated.
    message_carried_over: Option<Message<KafkaPayload>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl StreamSink {
    pub fn new(
        route: Route,
        producer: impl Producer<KafkaPayload> + 'static,
        concurrency_config: &ConcurrencyConfig,
        topic: &str,
        next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
        terminator_strategy: Box<dyn ProcessingStrategy<KafkaPayload>>,
    ) -> Self {
        let produce_strategy = Produce::new(
            terminator_strategy,
            producer,
            concurrency_config,
            Topic::new(topic).into(),
        );

        StreamSink {
            route,
            produce_strategy,
            next_strategy,
            message_carried_over: None,
            commit_request_carried_over: None,
        }
    }
}

impl ProcessingStrategy<RoutedValue> for StreamSink {
    /// Polls on both the strategies downstream.
    /// It simply merges the commit requests coming back from them and
    /// returns the result.
    /// TODO: In order to achieve at least once delivery we should
    /// always only return the lowest committed offset.
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let next_commit_request = self.next_strategy.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), next_commit_request);

        let produce_commit_request = self.produce_strategy.poll()?;
        self.commit_request_carried_over = merge_commit_request(
            self.commit_request_carried_over.take(),
            produce_commit_request,
        );

        if let Some(message) = self.message_carried_over.take() {
            match self.produce_strategy.submit(message) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(_) => {}
            }
        }

        Ok(self.commit_request_carried_over.take())
    }

    /// Submit the message to the producer if the route in the message
    /// corresponds to the route in the message correspond to the Route
    /// attribute in this strategy.
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.message_carried_over.is_some() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        if self.route != message.payload().route {
            self.next_strategy.submit(message)
        } else {
            match self.produce_strategy.submit(to_kafka_payload(message)) {
                Err(SubmitError::MessageRejected(MessageRejected {
                    message: transformed_message,
                })) => {
                    println!("Message rejected: {:?}", transformed_message);
                    self.message_carried_over = Some(transformed_message);
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(SubmitError::InvalidMessage(invalid_message));
                }
                Ok(_) => {}
            }
            Ok(())
        }
    }

    fn terminate(&mut self) {
        self.produce_strategy.terminate();
        self.next_strategy.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let next_commit = self.next_strategy.join(timeout)?;
        let next_commit_produce = self.produce_strategy.join(timeout)?;
        Ok(merge_commit_request(
            merge_commit_request(self.commit_request_carried_over.take(), next_commit),
            next_commit_produce,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::routes::Route;
    use crate::test_operators::make_raw_routed_msg;
    use parking_lot::Mutex;
    use sentry_arroyo::backends::local::broker::LocalBroker;

    use sentry_arroyo::backends::local::LocalProducer;

    use sentry_arroyo::backends::storages::memory::MemoryMessageStorage;

    use sentry_arroyo::processing::strategies::noop::Noop;
    use sentry_arroyo::types::Topic;
    use sentry_arroyo::utils::clock::TestingClock;

    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::Mutex as RawMutex;
    use std::time::SystemTime;

    #[test]
    fn test_kafka_payload() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let message = make_raw_routed_msg(
                py,
                "test_message".as_bytes().to_vec(),
                "source",
                vec!["waypoint1".to_string()],
            );
            let kafka_payload = to_kafka_payload(message);
            let py_payload = kafka_payload.payload();

            let kafka_payload = py_payload.payload();
            assert!(kafka_payload.is_some());
            assert_eq!(kafka_payload.unwrap(), b"test_message");
        });
    }

    #[test]
    fn test_route() {
        pyo3::prepare_freethreaded_python();
        let result_topic = Topic::new("result-topic");
        let mut broker = LocalBroker::new(
            Box::new(MemoryMessageStorage::default()),
            Box::new(TestingClock::new(SystemTime::now())),
        );
        broker.create_topic(result_topic, 1).unwrap();

        let broker = Arc::new(Mutex::new(broker));
        let producer = LocalProducer::new(broker.clone());

        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_messages_clone = submitted_messages.clone();
        let next_step = FakeStrategy::new(submitted_messages, false);
        let terminator = Noop {};

        let concurrency_config = ConcurrencyConfig::new(1);
        let mut sink = StreamSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            producer,
            &concurrency_config,
            "result-topic",
            Box::new(next_step),
            Box::new(terminator),
        );

        Python::with_gil(|py| {
            let value = b"test_message";
            let message = make_raw_routed_msg(py, value.to_vec(), "source", vec![]);
            sink.submit(message).unwrap();
            sink.join(None).unwrap();

            {
                let expected_messages = vec![PyBytes::new(py, b"test_message").into_any().unbind()];
                let actual_messages = submitted_messages_clone.lock().unwrap();
                assert_messages_match(py, expected_messages, actual_messages.deref());
            } // Unlock the MutexGuard around `actual_messages`

            // Try to send to the producer
            // No new message on the next strategy.
            let message =
                make_raw_routed_msg(py, value.to_vec(), "source", vec!["wp1".to_string()]);
            sink.submit(message).unwrap();
            sink.poll().unwrap();
            let expected_messages = vec![PyBytes::new(py, b"test_message").into_any().unbind()];
            let actual_messages = submitted_messages_clone.lock().unwrap();
            assert_messages_match(py, expected_messages, actual_messages.deref());
        });
    }
}
