//! This module provides an implementation of the StreamSink pipeline
//! step that produces messages on a Kafka topic.
//!
//! As all the strategies in the Arroyo streaming pipeline adapter,
//! This checks whether a message should be processed or forwarded
//! via the `Route` attribute.
use crate::messages::{PyStreamingMessage, RoutedValuePayload, Watermark, WatermarkMessage};
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use sentry_arroyo::backends::kafka::types::{Headers, KafkaPayload};
use sentry_arroyo::backends::Producer;
use sentry_arroyo::processing::strategies::MessageRejected;
use sentry_arroyo::types::{Message, Partition, Topic};

use core::panic;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::processing::strategies::{merge_commit_request, CommitRequest, StrategyError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

/// Represents a Watermark in a form that can be serialized, which allows watermarks
/// to be passed to our Produce step.
/// Since this struct exists to be serialized into JSON, and JSON keys must be strings
/// (and not tuples), we repackage the committable to be in the format:
/// ```json
/// {
///     topic_name:
///         [
///             {"index": index1, "offset": offset1},
///             {"index": index2, "offset": offset2},
///         ]
/// }
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct SerializableWatermark {
    committable: HashMap<String, Vec<HashMap<String, u64>>>,
    timestamp: u64,
}

impl From<Watermark> for SerializableWatermark {
    fn from(value: Watermark) -> Self {
        let mut committable: HashMap<String, Vec<HashMap<String, u64>>> = Default::default();
        for (partition, offset) in value.committable {
            let topic_name = partition.topic.to_string();
            let index = partition.index;
            let new_entry = HashMap::from([
                ("index".to_string(), index as u64),
                ("offset".to_string(), offset),
            ]);
            match committable.get_mut(&topic_name) {
                Some(vec) => {
                    vec.push(new_entry);
                }
                None => {
                    committable.insert(topic_name, vec![new_entry]);
                }
            }
        }
        SerializableWatermark {
            committable,
            timestamp: value.timestamp,
        }
    }
}

impl From<SerializableWatermark> for Watermark {
    fn from(value: SerializableWatermark) -> Self {
        let mut committable = BTreeMap::new();
        for (topic, topic_info) in value.committable {
            let topic = Topic::new(&topic);
            for partition_info in topic_info {
                let index = partition_info.get("index").unwrap().to_owned() as u16;
                let offset = partition_info.get("offset").unwrap().to_owned();
                committable.insert(Partition { topic, index }, offset);
            }
        }
        Watermark {
            committable,
            timestamp: value.timestamp,
        }
    }
}

/// Turns a `Message<RoutedValue` passed among streaming primitives into
/// a `Message<KafkaPayload>` to be produced on a Kafka topic.
/// This is needed because what we need to produce on a Kafka topic is
/// only the payload in bytes taken from the `Message<RoutedValue>`.
/// Also this has to be converted from the Python PyAny payload into
/// a Vector of bytes.
fn to_kafka_payload(message: Message<RoutedValue>) -> Message<KafkaPayload> {
    // Convert the RoutedValue to KafkaPayload
    // This is a placeholder implementation
    let payload = match message.payload().payload {
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::PyAnyMessage { .. }) => {
            panic!("PyAnyMessage is not supported in KafkaPayload conversion");
        }
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage { ref content }) => {
            traced_with_gil!(|py| {
                let payload_content = content.bind(py).getattr("payload").unwrap();
                let py_bytes: &Bound<PyBytes> = payload_content.downcast().unwrap();
                let raw_bytes = py_bytes.as_bytes();
                let mut headers = Headers::new();
                headers = headers.insert("is_watermark", Some(vec![0]));
                KafkaPayload::new(None, Some(headers), Some(raw_bytes.to_vec()))
            })
        }
        // TODO: currently sink step just passes watermarks on to next_step, once a wrapper is built around the produce
        // step to handle watermarks we'll be serializing watermarks as well
        RoutedValuePayload::WatermarkMessage(WatermarkMessage::PyWatermark(..)) => {
            panic!("PyWatermark is not supported in KafkaPayload conversion");
        }
        RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(ref watermark)) => {
            let serializable_watermark: SerializableWatermark = watermark.clone().into();
            let json_payload = serde_json::to_vec(&serializable_watermark).unwrap();
            let mut headers = Headers::new();
            headers = headers.insert("is_watermark", Some(vec![1]));
            KafkaPayload::new(None, Some(headers), Some(json_payload))
        }
    };
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

        // TODO: pass watermark messages on to produce step once produce step is async and we have a wrapper
        // around it to handle watermarks
        if self.route != message.payload().route || message.payload().payload.is_watermark_msg() {
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
    use crate::messages::{RoutedValuePayload, Watermark};
    use crate::routes::Route;
    use crate::testutils::make_raw_routed_msg;
    use crate::utils::traced_with_gil;
    use sentry_arroyo::backends::local::broker::LocalBroker;

    use sentry_arroyo::backends::local::LocalProducer;

    use sentry_arroyo::backends::storages::memory::MemoryMessageStorage;

    use sentry_arroyo::processing::strategies::noop::Noop;
    use sentry_arroyo::types::Topic;
    use sentry_arroyo::utils::clock::TestingClock;

    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::Mutex as RawMutex;
    use std::time::SystemTime;

    #[test]
    fn test_serializable_watermark_conversion() {
        let watermark = Watermark::new(
            BTreeMap::from([
                (Partition::new(Topic::new("Topic1"), 0), 0),
                (Partition::new(Topic::new("Topic1"), 1), 1),
                (Partition::new(Topic::new("Topic3"), 0), 0),
            ]),
            0,
        );

        let serializable_watermark: SerializableWatermark = watermark.clone().into();
        let converted_watermark: Watermark = serializable_watermark.into();
        assert_eq!(watermark, converted_watermark)
    }

    #[test]
    fn test_kafka_payload() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
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

        let watermark_payload = Watermark::new(
            BTreeMap::from([
                (Partition::new(Topic::new("Topic1"), 0), 0),
                (Partition::new(Topic::new("Topic1"), 1), 1),
                (Partition::new(Topic::new("Topic3"), 0), 0),
            ]),
            0,
        );
        let watermark = Message::new_any_message(
            RoutedValue {
                route: Route {
                    source: "source1".to_string(),
                    waypoints: vec![],
                },
                payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(
                    watermark_payload.clone(),
                )),
            },
            BTreeMap::new(),
        );
        let kafka_payload = to_kafka_payload(watermark);
        let bytes_payload = kafka_payload.payload().payload().unwrap().to_owned();
        let deserialized: SerializableWatermark = serde_json::from_slice(&bytes_payload).unwrap();
        let converted_watermark: Watermark = deserialized.into();
        assert_eq!(watermark_payload, converted_watermark)
    }

    #[test]
    fn test_watermark_message() {
        let result_topic = Topic::new("result-topic");
        let mut broker = LocalBroker::new(
            Box::new(MemoryMessageStorage::default()),
            Box::new(TestingClock::new(SystemTime::now())),
        );
        broker.create_topic(result_topic, 1).unwrap();
        let broker = Arc::new(Mutex::new(broker));
        let producer = LocalProducer::new(broker.clone());

        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks_clone = submitted_watermarks.clone();
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

        let mut sink = StreamSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            producer,
            &ConcurrencyConfig::new(1),
            "result-topic",
            Box::new(next_step),
            Box::new(Noop {}),
        );

        let watermark_val = RoutedValue {
            route: Route::new(String::from("source"), vec![]),
            payload: RoutedValuePayload::make_watermark_payload(BTreeMap::new(), 0),
        };
        let watermark_msg = Message::new_any_message(watermark_val, BTreeMap::new());
        let watermark_res = sink.submit(watermark_msg);
        assert!(watermark_res.is_ok());
        let watermark_messages = submitted_watermarks_clone.lock().unwrap();
        assert_eq!(watermark_messages[0], Watermark::new(BTreeMap::new(), 0));
    }

    #[test]
    fn test_route() {
        crate::testutils::initialize_python();
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
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);
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

        traced_with_gil!(|py| {
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
