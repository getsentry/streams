//! This module contains the implementation of a single Arroyo consumer
//! that runs a streaming pipeline.
//!
//! Each arroyo consumer represents a source in the streaming pipeline
//! and all the steps following that source.
//! The pipeline is built by adding RuntimeOperators to the consumer.

use crate::kafka_config::PyKafkaConsumerConfig;
use crate::messages::{into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload};
use crate::operators::build;
use crate::operators::RuntimeOperator;
use crate::routes::Route;
use crate::routes::RoutedValue;
use crate::utils::traced_with_gil;
use crate::watermark::WatermarkEmitter;
use pyo3::prelude::*;
use pyo3::types::PyList;
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
use std::time::Duration;

/// Enum to represent different consumer backend modes
#[derive(Debug, Clone)]
pub enum ConsumerMode {
    /// Production Kafka consumer mode
    Kafka,
    /// In-memory testing mode with pre-populated data
    InMemory {
        /// Raw message data to be processed
        test_data: Vec<Vec<u8>>,
    },
}

/// Configuration for in-memory testing
#[pyclass]
#[derive(Debug, Clone)]
pub struct InMemoryConfig {
    /// Test data as list of byte strings
    pub test_data: Vec<Vec<u8>>,
}

#[pymethods]
impl InMemoryConfig {
    #[new]
    fn new() -> Self {
        InMemoryConfig {
            test_data: Vec::new(),
        }
    }

    /// Add test data from Python bytes or strings
    fn add_test_data(&mut self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Ok(py_list) = data.downcast::<PyList>() {
            for item in py_list.iter() {
                if let Ok(bytes) = item.extract::<Vec<u8>>() {
                    self.test_data.push(bytes);
                } else if let Ok(string) = item.extract::<String>() {
                    self.test_data.push(string.into_bytes());
                }
            }
        } else if let Ok(bytes) = data.extract::<Vec<u8>>() {
            self.test_data.push(bytes);
        } else if let Ok(string) = data.extract::<String>() {
            self.test_data.push(string.into_bytes());
        }
        Ok(())
    }
}

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

    /// Consumer mode - either Kafka or InMemory for testing
    mode: ConsumerMode,
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
            mode: ConsumerMode::Kafka,
        }
    }

    /// Add a step to the Consumer pipeline at the end of it.
    /// This class is supposed to be instantiated by the Python adapter
    /// so it takes the steps descriptor as a Py<RuntimeOperator>.
    fn add_step(&mut self, step: Py<RuntimeOperator>) {
        self.steps.push(step);
    }

    /// Configure the consumer for in-memory testing mode with test data
    fn set_test_mode(&mut self, config: InMemoryConfig) {
        self.mode = ConsumerMode::InMemory {
            test_data: config.test_data,
        };
    }

    /// Check if the consumer is in test mode
    fn is_test_mode(&self) -> bool {
        matches!(self.mode, ConsumerMode::InMemory { .. })
    }

    /// Runs the consumer.
    /// This method is blocking and will run until the consumer
    /// is stopped via SIGTERM or SIGINT.
    fn run(&mut self) {
        tracing_subscriber::fmt::init();

        match &self.mode {
            ConsumerMode::Kafka => {
                println!("Running Arroyo Consumer in Kafka mode...");
                self.run_kafka_mode();
            }
            ConsumerMode::InMemory { test_data } => {
                println!(
                    "Running Arroyo Consumer in test mode with {} messages...",
                    test_data.len()
                );
                self.run_test_mode(test_data.clone());
            }
        }
    }

    /// Run the consumer in Kafka mode (original implementation)
    fn run_kafka_mode(&mut self) {
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

    /// Run the consumer in test mode with in-memory data
    fn run_test_mode(&mut self, test_data: Vec<Vec<u8>>) {
        println!("Processing {} test messages...", test_data.len());

        let factory = ArroyoStreamingFactory::new(
            self.source.clone(),
            &self.steps,
            self.concurrency_config.clone(),
            self.schema.clone(),
        );

        // Create a processing strategy directly
        let mut strategy = factory.create();

        // Convert test data to KafkaPayload messages and process them
        for (i, data) in test_data.iter().enumerate() {
            let kafka_payload = KafkaPayload::new(None, None, Some(data.clone()));
            let message =
                Message::new_any_message(kafka_payload, std::collections::BTreeMap::new());

            match strategy.submit(message) {
                Ok(()) => {
                    println!("Processed test message {}", i + 1);
                }
                Err(e) => {
                    println!("Error processing test message {}: {:?}", i + 1, e);
                }
            }

            // Poll the strategy to process any pending work
            match strategy.poll() {
                Ok(_) => {}
                Err(e) => {
                    println!("Error during poll: {:?}", e);
                }
            }
        }

        // Join the strategy to ensure all work is completed
        match strategy.join(Some(Duration::from_secs(5))) {
            Ok(_) => println!("All test messages processed successfully"),
            Err(e) => println!("Error during join: {:?}", e),
        }
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
    let py_msg = traced_with_gil!(|py| PyStreamingMessage::RawMessage {
        content: into_pyraw(py, raw_message).unwrap(),
    });

    let route = Route::new(source.to_string(), vec![]);
    message.replace(RoutedValue {
        route,
        payload: RoutedValuePayload::PyStreamingMessage(py_msg),
    })
}

/// Builds the Arroyo StreamProcessor for this consumer.
///
/// It wires up all the operators added to the consumer object,
/// it prefix the chain with a step that converts the Message<KafkaPayload>
/// to a Message<RoutedValue> and it adds a termination step provided
/// by the caller. This is generally a CommitOffsets step but it can
/// be customized.
/// It also adds a Watermark step which periodically sends watermark messages downstream.
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
    let watermark_step = Box::new(WatermarkEmitter::new(
        next,
        Route {
            source: source.to_string(),
            waypoints: vec![],
        },
        10,
    ));

    let copied_source = source.to_string();
    let copied_schema = schema.clone();
    let conversion_function = move |message: Message<KafkaPayload>| {
        Ok(to_routed_value(&copied_source, message, &copied_schema))
    };

    let converter = RunTask::new(conversion_function, watermark_step);

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
        let steps_copy = traced_with_gil!(|py| {
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
        traced_with_gil!(|py| {
            let payload_data = b"test_payload";
            let message = make_msg(Some(payload_data.to_vec()));

            let python_message = to_routed_value("source", message, &Some("schema".to_string()));

            let msg_payload = python_message.payload();
            let py_payload = msg_payload.payload.unwrap_payload();

            if let PyStreamingMessage::RawMessage { ref content } = py_payload {
                let payload = content.getattr(py, "payload").unwrap();
                let down: &Bound<PyBytes> = payload.bind(py).downcast().unwrap();
                let payload_bytes: &[u8] = down.as_bytes();
                assert_eq!(payload_bytes, payload_data);
            } else {
                panic!("Expected RawMessage, got PyAnyMessage");
            }

            assert_eq!(msg_payload.route.source, "source");
            assert_eq!(msg_payload.route.waypoints.len(), 0);
        });
    }

    #[test]
    fn test_to_none_python() {
        pyo3::prepare_freethreaded_python();
        traced_with_gil!(|py| {
            let message = make_msg(None);
            let python_message = to_routed_value("source", message, &Some("schema".to_string()));
            let msg_payload = &python_message.payload();
            let py_payload = msg_payload.payload.unwrap_payload();

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
        traced_with_gil!(|py| {
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
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

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

    #[test]
    fn test_in_memory_consumer_mode() {
        use crate::kafka_config::{InitialOffset, PyKafkaConsumerConfig};

        pyo3::prepare_freethreaded_python();

        // Create consumer in Kafka mode by default
        let kafka_config = PyKafkaConsumerConfig::new(
            vec!["localhost:9092".to_string()],
            "test_group".to_string(),
            InitialOffset::Earliest,
            false,
            300000,
            None,
        );

        let consumer = ArroyoConsumer::new(
            "test_source".to_string(),
            kafka_config,
            "test_topic".to_string(),
            Some("test_schema".to_string()),
        );

        assert!(!consumer.is_test_mode());

        // Test switching to in-memory mode
        let mut consumer = consumer;
        let mut config = InMemoryConfig::new();
        config.test_data.push(b"test_message_1".to_vec());
        config.test_data.push(b"test_message_2".to_vec());

        consumer.set_test_mode(config);
        assert!(consumer.is_test_mode());

        // Verify test data is stored correctly
        if let ConsumerMode::InMemory { test_data } = &consumer.mode {
            assert_eq!(test_data.len(), 2);
            assert_eq!(test_data[0], b"test_message_1".to_vec());
            assert_eq!(test_data[1], b"test_message_2".to_vec());
        } else {
            panic!("Consumer should be in InMemory mode");
        }
    }
}
