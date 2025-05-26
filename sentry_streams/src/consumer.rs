//! This module contains the implementation of a single Arroyo consumer
//! that runs a streaming pipeline.
//!
//! Each arroyo consumer represents a source in the streaming pipeline
//! and all the steps following that source.
//! The pipeline is built by adding RuntimeOperators to the consumer.

use crate::kafka_config::PyKafkaConsumerConfig;
use crate::messages::{into_pyraw, PyStreamingMessage, RawMessage};
use crate::operators::build;
use crate::operators::RuntimeOperator;
use crate::routes::Route;
use crate::routes::RoutedValue;
use pyo3::prelude::*;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::noop::Noop;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::ProcessingStrategyFactory;
use sentry_arroyo::processing::ProcessorHandle;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Message, Topic};
use std::sync::Arc;

use pyo3::Python;
use std::time::Duration;

/// The class that represent the consumer.
/// This class is exposed to python and it is the main entry point
/// used by the Python adapter to build a pipeline and run it.
///
/// The Consumer class needs the Kafka configuration to be instantiated
/// then RuntimeOperator are added one by one in the order a message
/// would be processed. It is needed for the consumer to be provided
/// the whole pipeline before an Arroyo consumer can be built because
/// arroyo primitives have to be instantiated from the end to the
/// beginning.
///
#[pyclass]
pub struct ArroyoConsumer {
    consumer_config: PyKafkaConsumerConfig,

    topic: String,

    source: String,

    schema: Option<String>,

    steps: Vec<Py<RuntimeOperator>>,

    /// The ProcessorHandle allows the main thread to stop the StreamingProcessor
    /// from a different thread.
    handle: Option<ProcessorHandle>,

    // this variable must live for the lifetime of the entire consumer.
    // This is a requirement of Arroyo Rust.
    concurrency_config: Arc<ConcurrencyConfig>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(
        source: String,
        kafka_config: PyKafkaConsumerConfig,
        topic: String,
        schema: Option<String>,
    ) -> Self {
        ArroyoConsumer {
            consumer_config: kafka_config,
            topic,
            source,
            schema,
            steps: Vec::new(),
            handle: None,
            concurrency_config: Arc::new(ConcurrencyConfig::new(1)),
        }
    }

    /// Add a step to the Consumer pipeline at the end of it.
    /// This class is supposed to be instantiated by the Python adapter
    /// so it takes the steps descriptor as a Py<RuntimeOperator>.
    fn add_step(&mut self, step: Py<RuntimeOperator>) {
        self.steps.push(step);
    }

    /// Runs the consumer.
    /// This method is blocking and will run until the consumer
    /// is stopped via SIGTERM or SIGINT.
    fn run(&mut self) {
        tracing_subscriber::fmt::init();
        println!("Running Arroyo Consumer...");

        let factory = ArroyoStreamingFactory::new(
            self.source.clone(),
            &self.steps,
            self.concurrency_config.clone(),
            self.schema.clone(),
        );
        let config = self.consumer_config.clone().into();
        let processor = StreamProcessor::with_kafka(config, factory, Topic::new(&self.topic), None);
        self.handle = Some(processor.get_handle());

        let mut handle = processor.get_handle();
        ctrlc::set_handler(move || {
            println!("\nCtrl+C pressed!");
            handle.signal_shutdown();
        })
        .expect("Error setting Ctrl+C handler");

        let _ = processor.run();
    }

    fn shutdown(&mut self) {
        match self.handle.take() {
            Some(mut handle) => handle.signal_shutdown(),
            None => println!("No handle to shut down."),
        }
    }
}

/// Converts a Message<KafkaPayload> to a Message<RoutedValue>.
///
/// The messages we send around between steps in the pipeline contain
/// the `Route` object that represent the path the message took when
/// going through branches.
/// The message coming from Kafka is a Message<KafkaPayload>, so we need
/// to turn the content into PyBytes for python to manage the content
/// and we need to wrap the message into a RoutedValue object.
fn to_routed_value(
    source: &str,
    message: Message<KafkaPayload>,
    schema: &Option<String>,
) -> Message<RoutedValue> {
    let raw_payload = message.payload().payload();
    let raw_payload = match raw_payload {
        Some(payload) => payload,
        None => &vec![],
    };
    let headers = message.payload().headers();

    let transformed_headers = match headers {
        Some(h) => {
            let rd_headers: OwnedHeaders = h.clone().into();
            let transformed = rd_headers
                .iter()
                .map(|Header { key, value }| {
                    // Convert Header to Python tuple (key, value)
                    match value {
                        Some(v) => (key.to_string(), v.to_vec()),
                        None => (key.to_string(), vec![]),
                    }
                })
                .collect();
            transformed
        }

        None => vec![],
    };
    // Convert message.timestamp() (Option<i64>) to UTC timestamp as float (seconds since epoch)
    let timestamp = match message.timestamp() {
        Some(ts) => ts.timestamp_millis() as f64 / 1_000_000_000.0,
        None => 0.0, // Default to 0 if no timestamp is available
    };
    let raw_message = RawMessage {
        payload: raw_payload.to_vec(),
        headers: transformed_headers,
        timestamp,
        schema: schema.clone(),
    };
    let py_msg = Python::with_gil(|py| PyStreamingMessage::RawMessage {
        content: into_pyraw(py, raw_message).unwrap(),
    });

    let route = Route::new(source.to_string(), vec![]);
    message.replace(RoutedValue {
        route,
        payload: py_msg,
    })
}

/// Builds the Arroyo StreamProcessor for this consumer.
///
/// It wires up all the operators added to the consumer object,
/// it prefix the chain with a step that converts the Message<KafkaPayload>
/// to a Message<RoutedValue> and it adds a termination step provided
/// by the caller. This is generally a CommitOffsets step but it can
/// be customized.
fn build_chain(
    source: &str,
    steps: &[Py<RuntimeOperator>],
    ending_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    concurrency_config: &ConcurrencyConfig,
    schema: &Option<String>,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let mut next = ending_strategy;
    for step in steps.iter().rev() {
        next = build(
            step,
            next,
            Box::new(CommitOffsets::new(Duration::from_secs(5))),
            concurrency_config,
        );
    }

    let copied_source = source.to_string();
    let copied_schema = schema.clone();
    let conversion_function = move |message: Message<KafkaPayload>| {
        Ok(to_routed_value(&copied_source, message, &copied_schema))
    };

    let converter = RunTask::new(conversion_function, next);

    Box::new(converter)
}

struct ArroyoStreamingFactory {
    source: String,
    steps: Vec<Py<RuntimeOperator>>,
    concurrency_config: Arc<ConcurrencyConfig>,
    schema: Option<String>,
}

impl ArroyoStreamingFactory {
    /// Creates a new instance of ArroyoStreamingFactory.
    fn new(
        source: String,
        steps: &[Py<RuntimeOperator>],
        concurrency_config: Arc<ConcurrencyConfig>,
        schema: Option<String>,
    ) -> Self {
        let steps_copy = Python::with_gil(|py| {
            steps
                .iter()
                .map(|step| step.clone_ref(py))
                .collect::<Vec<_>>()
        });

        ArroyoStreamingFactory {
            source,
            steps: steps_copy,
            concurrency_config,
            schema,
        }
    }
}

impl ProcessingStrategyFactory<KafkaPayload> for ArroyoStreamingFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        build_chain(
            &self.source,
            &self.steps,
            Box::new(Noop {}),
            &self.concurrency_config,
            &self.schema,
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::operators::RuntimeOperator;
    use crate::routes::Route;
    use crate::test_operators::make_lambda;
    use crate::test_operators::make_msg;
    use pyo3::ffi::c_str;
    use pyo3::types::PyBytes;
    use pyo3::IntoPyObjectExt;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_to_routed_value() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let payload_data = b"test_payload";
            let message = make_msg(Some(payload_data.to_vec()));

            let python_message = to_routed_value("source", message, &Some("schema".to_string()));

            let py_payload = python_message.payload();

            if let PyStreamingMessage::RawMessage { ref content } = py_payload.payload {
                let payload = content.getattr(py, "payload").unwrap();
                let down: &Bound<PyBytes> = payload.bind(py).downcast().unwrap();
                let payload_bytes: &[u8] = down.as_bytes();
                assert_eq!(payload_bytes, payload_data);
            } else {
                panic!("Expected RawMessage, got PyAnyMessage");
            }

            assert_eq!(py_payload.route.source, "source");
            assert_eq!(py_payload.route.waypoints.len(), 0);
        });
    }

    #[test]
    fn test_to_none_python() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let message = make_msg(None);
            let python_message = to_routed_value("source", message, &Some("schema".to_string()));
            let py_payload = &python_message.payload().payload;

            if let PyStreamingMessage::RawMessage { content } = py_payload {
                let bytes = content
                    .getattr(py, "payload")
                    .unwrap()
                    .bind(py)
                    .downcast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                assert_eq!(bytes, Vec::<u8>::new());
            } else {
                panic!("Expected RawMessage, got PyAnyMessage");
            }
        });
    }

    #[test]
    fn test_build_chain() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let callable = make_lambda(
                py,
                c_str!("lambda x: x.replace_payload((x.payload.decode('utf-8') + '_transformed').encode())"),
            );

            let mut steps: Vec<Py<RuntimeOperator>> = vec![];

            let r = Py::new(
                py,
                RuntimeOperator::Map {
                    route: Route::new("source".to_string(), vec![]),
                    function: callable,
                },
            )
            .unwrap();
            steps.push(r);

            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy::new(submitted_messages, false);

            let concurrency_config = ConcurrencyConfig::new(1);
            let mut chain = build_chain(
                "source",
                &steps,
                Box::new(next_step),
                &concurrency_config,
                &Some("schema".to_string()),
            );
            let message = make_msg(Some(b"test_payload".to_vec()));

            chain.submit(message).unwrap();

            let value = "test_payload_transformed"
                .to_string()
                .into_bytes()
                .into_py_any(py)
                .unwrap();
            let expected_messages = vec![value];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());
        })
    }
}
