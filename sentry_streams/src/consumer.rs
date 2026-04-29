//! This module contains the implementation of a single Arroyo consumer
//! that runs a streaming pipeline.
//!
//! Each arroyo consumer represents a source in the streaming pipeline
//! and all the steps following that source.
//! The pipeline is built by adding RuntimeOperators to the consumer.

use crate::commit_policy::WatermarkCommitOffsets;
use crate::kafka_config::{PyKafkaConsumerConfig, PyKafkaProducerConfig};
use crate::messages::{into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload};
use crate::metrics::configure_metrics;
use crate::metrics_config::PyMetricConfig;
use crate::operators::build;
use crate::operators::RuntimeOperator;
use crate::routes::Route;
use crate::routes::RoutedValue;
use crate::utils::traced_with_gil;
use crate::watermark::WatermarkEmitter;
use pyo3::prelude::*;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::dlq::{DlqLimit, DlqPolicy, KafkaDlqProducer};
use sentry_arroyo::processing::strategies::healthcheck::HealthCheck;
use sentry_arroyo::processing::strategies::noop::Noop;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::ProcessingStrategyFactory;
use sentry_arroyo::processing::ProcessorHandle;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Message, Topic};
use std::sync::Arc;

/// Default path for the healthcheck file touched when write_healthcheck is enabled.
/// Matches Arroyo docs for Kubernetes liveness probes.
const HEALTHCHECK_PATH: &str = "/tmp/health.txt";

/// Configuration for Dead Letter Queue (DLQ).
/// When provided, invalid messages will be sent to the DLQ topic.
#[pyclass]
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// The Kafka topic name to send invalid messages to
    #[pyo3(get)]
    pub topic: String,

    /// The Kafka producer configuration for the DLQ
    #[pyo3(get)]
    pub producer_config: PyKafkaProducerConfig,
}

#[pymethods]
impl DlqConfig {
    #[new]
    fn new(topic: String, producer_config: PyKafkaProducerConfig) -> Self {
        DlqConfig {
            topic,
            producer_config,
        }
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

    metric_config: Option<PyMetricConfig>,

    /// When true, wrap the strategy chain with HealthCheck to touch a file on poll for liveness.
    write_healthcheck: bool,

    /// DLQ (Dead Letter Queue) configuration.
    /// If provided, invalid messages will be sent to the DLQ topic.
    /// Otherwise, invalid messages will cause the consumer to stop processing.
    #[pyo3(get)]
    dlq_config: Option<DlqConfig>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    #[pyo3(signature = (source, kafka_config, topic, schema, metric_config=None, write_healthcheck=false, dlq_config=None))]
    fn new(
        source: String,
        kafka_config: PyKafkaConsumerConfig,
        topic: String,
        schema: Option<String>,
        metric_config: Option<PyMetricConfig>,
        write_healthcheck: bool,
        dlq_config: Option<DlqConfig>,
    ) -> Self {
        ArroyoConsumer {
            consumer_config: kafka_config,
            topic,
            source,
            schema,
            steps: Vec::new(),
            handle: None,
            concurrency_config: Arc::new(ConcurrencyConfig::new(1)),
            metric_config,
            write_healthcheck,
            dlq_config,
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

        configure_metrics(self.metric_config.clone());

        let factory = ArroyoStreamingFactory::new(
            self.source.clone(),
            &self.steps,
            self.concurrency_config.clone(),
            self.schema.clone(),
            self.write_healthcheck,
        );
        let config = self.consumer_config.clone().into();

        // Build DLQ policy if configured
        let dlq_policy = build_dlq_policy(&self.dlq_config, self.concurrency_config.handle());

        let processor =
            StreamProcessor::with_kafka(config, factory, Topic::new(&self.topic), dlq_policy);
        self.handle = Some(processor.get_handle());

        let mut handle = processor.get_handle();
        ctrlc::set_handler(move || {
            println!("\nCtrl+C pressed!");
            handle.signal_shutdown();
        })
        .expect("Error setting Ctrl+C handler");

        if let Err(e) = processor.run() {
            tracing::error!("StreamProcessor error: {:?}", e);
        }
    }

    fn shutdown(&mut self) {
        match self.handle.take() {
            Some(mut handle) => handle.signal_shutdown(),
            None => println!("No handle to shut down."),
        }
    }
}

/// Builds the DLQ policy if dlq_config is provided.
/// Returns None if DLQ is not configured.
pub fn build_dlq_policy(
    dlq_config: &Option<DlqConfig>,
    handle: tokio::runtime::Handle,
) -> Option<DlqPolicy<KafkaPayload>> {
    match dlq_config {
        Some(dlq_config) => {
            tracing::info!("Configuring DLQ with topic: {}", dlq_config.topic);

            // Create Kafka producer for DLQ
            let producer_config = dlq_config.producer_config.clone().into();
            let kafka_producer = KafkaProducer::new(producer_config);
            let dlq_producer = KafkaDlqProducer::new(kafka_producer, Topic::new(&dlq_config.topic));

            // Use default DLQ limits (no limits) and no max buffered messages
            // These can be made configurable in a future PR if needed
            let dlq_limit = DlqLimit::default();
            let max_buffered_messages = None;

            Some(DlqPolicy::new(
                handle,
                Box::new(dlq_producer),
                dlq_limit,
                max_buffered_messages,
            ))
        }
        None => {
            tracing::info!("DLQ not configured, invalid messages will cause processing to stop");
            None
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
pub fn build_chain(
    source: &str,
    steps: &[Py<RuntimeOperator>],
    ending_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    concurrency_config: &ConcurrencyConfig,
    schema: &Option<String>,
    write_healthcheck: bool,
) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let mut next = ending_strategy;
    for step in steps.iter().rev() {
        next = build(step, next, Box::new(Noop {}), concurrency_config);
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

    let chain: Box<dyn ProcessingStrategy<KafkaPayload>> = Box::new(converter);
    if write_healthcheck {
        Box::new(HealthCheck::new(chain, HEALTHCHECK_PATH))
    } else {
        chain
    }
}

struct ArroyoStreamingFactory {
    source: String,
    steps: Vec<Py<RuntimeOperator>>,
    concurrency_config: Arc<ConcurrencyConfig>,
    schema: Option<String>,
    write_healthcheck: bool,
}

impl ArroyoStreamingFactory {
    /// Creates a new instance of ArroyoStreamingFactory.
    fn new(
        source: String,
        steps: &[Py<RuntimeOperator>],
        concurrency_config: Arc<ConcurrencyConfig>,
        schema: Option<String>,
        write_healthcheck: bool,
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
            write_healthcheck,
        }
    }
}

impl ProcessingStrategyFactory<KafkaPayload> for ArroyoStreamingFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        build_chain(
            &self.source,
            &self.steps,
            // TODO: once Broadcast/Router work properly, count how many total downstream
            // branches a pipeline has and pass that value to the Watermark
            Box::new(WatermarkCommitOffsets::new(1)),
            &self.concurrency_config,
            &self.schema,
            self.write_healthcheck,
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
    use crate::testutils::make_lambda;
    use crate::testutils::make_msg;
    use pyo3::ffi::c_str;
    use pyo3::types::PyBytes;
    use pyo3::IntoPyObjectExt;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_to_routed_value() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let payload_data = b"test_payload";
            let message = make_msg(Some(payload_data.to_vec()), BTreeMap::new());

            let python_message = to_routed_value("source", message, &Some("schema".to_string()));

            let msg_payload = python_message.payload();
            let py_payload = msg_payload.payload.unwrap_payload();

            if let PyStreamingMessage::RawMessage { ref content } = py_payload {
                let payload = content.getattr(py, "payload").unwrap();
                let down: &Bound<PyBytes> = payload.bind(py).cast().unwrap();
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
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let message = make_msg(None, BTreeMap::new());
            let python_message = to_routed_value("source", message, &Some("schema".to_string()));
            let msg_payload = &python_message.payload();
            let py_payload = msg_payload.payload.unwrap_payload();

            if let PyStreamingMessage::RawMessage { content } = py_payload {
                let bytes = content
                    .getattr(py, "payload")
                    .unwrap()
                    .bind(py)
                    .cast::<PyBytes>()
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
        crate::testutils::initialize_python();
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
                true,
            );
            let message = make_msg(Some(b"test_payload".to_vec()), BTreeMap::new());

            chain.submit(message).unwrap();

            let value = "test_payload_transformed"
                .to_string()
                .into_bytes()
                .into_py_any(py)
                .unwrap();
            let expected_messages = vec![value];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());

            // Validate that the HealthCheck strategy writes the healthcheck file when poll() is called.
            // HealthCheck touches the file at most once per second, so we poll, wait past the interval, then poll again.
            let _ = chain.poll();
            thread::sleep(Duration::from_secs(2));
            let _ = chain.poll();
            let healthcheck_path = Path::new(super::HEALTHCHECK_PATH);
            assert!(
                healthcheck_path.exists(),
                "healthcheck file should exist at {} after poll() with write_healthcheck=true",
                super::HEALTHCHECK_PATH
            );
            let _ = std::fs::remove_file(healthcheck_path);
        })
    }

    #[test]
    fn test_build_dlq_policy_with_various_configs() {
        // Define test cases: (test_name, dlq_bootstrap_servers, expected_some)
        let test_cases = vec![
            ("without_dlq_config", None, false),
            (
                "with_dlq_config_single_broker",
                Some(vec!["localhost:9092".to_string()]),
                true,
            ),
            (
                "with_dlq_config_multiple_brokers",
                Some(vec![
                    "broker1:9092".to_string(),
                    "broker2:9092".to_string(),
                    "broker3:9092".to_string(),
                ]),
                true,
            ),
        ];

        // Create a tokio runtime to get a handle for testing
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        for (test_name, dlq_bootstrap_servers, expected_some) in test_cases {
            // Create DLQ config if bootstrap servers are provided
            let dlq_config = dlq_bootstrap_servers.map(|servers| {
                let producer_config = PyKafkaProducerConfig::new(servers, None);
                DlqConfig::new("test-dlq".to_string(), producer_config)
            });

            // Build DLQ policy and assert
            let dlq_policy: Option<DlqPolicy<KafkaPayload>> =
                build_dlq_policy(&dlq_config, handle.clone());
            assert_eq!(
                dlq_policy.is_some(),
                expected_some,
                "Test case '{}' failed: expected is_some() to be {}",
                test_name,
                expected_some
            );
        }
    }

    // Note: Asserting on inside properties of dlq_policy is tested through Python integration tests
    // in tests/test_dlq.py, as the dlq_policy is an external crate and inner properties are private.
}
