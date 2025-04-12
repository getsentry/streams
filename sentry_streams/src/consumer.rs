//! This module contains the implementation of a single Arroyo consumer
//! that runs a streaming pipeline.
//!
//! Each arroyo consumer represents a source in the streaming pipeline
//! and all the steps following that source.
//! The pipeline is built by adding RuntimeOperators to the consumer.

use crate::kafka_config::PyKafkaConsumerConfig;
use crate::operators::build;
use crate::operators::RuntimeOperator;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::ProcessingStrategyFactory;
use sentry_arroyo::processing::ProcessorHandle;
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Message, Topic};

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

    steps: Vec<Py<RuntimeOperator>>,

    /// The ProcessorHandle allows the main thread to stop the StreamingProcessor
    /// from a different thread.
    handle: Option<ProcessorHandle>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(kafka_config: PyKafkaConsumerConfig, topic: String) -> Self {
        ArroyoConsumer {
            consumer_config: kafka_config,
            topic,
            steps: Vec::new(),
            handle: None,
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
        let steps_copy = Python::with_gil(|py| {
            self.steps
                .iter()
                .map(|step| step.clone_ref(py))
                .collect::<Vec<_>>()
        });
        let factory = ArroyoStreamingFactory { steps: steps_copy };
        let config = self.consumer_config.clone().into();
        let processor = StreamProcessor::with_kafka(config, factory, Topic::new(&self.topic), None);
        self.handle = Some(processor.get_handle());

        let mut handle = processor.get_handle();
        ctrlc::set_handler(move || {
            println!("\nCtrl+C pressed!");
            handle.signal_shutdown();
        })
        .expect("Error setting Ctrl+C handler");

        processor.run();
    }

    fn shutdown(&mut self) {
        match self.handle.take() {
            Some(mut handle) => handle.signal_shutdown(),
            None => println!("No handle to shut down."),
        }
    }
}

/// Converts a Message<KafkaPayload> to a Message<Py<PyAny>>.
/// It takes the Kafka payload as bytes and turns it into a
/// Python bytes object.
fn to_python(message: Message<KafkaPayload>) -> Message<Py<PyAny>> {
    let payload = Python::with_gil(|py| {
        let payload = message.payload().payload().unwrap();
        let py_bytes = PyBytes::new(py, payload);
        py_bytes.into_any().unbind()
    });
    message.replace(payload)
}

/// Builds the Arroyo StreamProcessor for this consumer.
/// It plugs a Commit policy at the end and a translator at the beginning
/// that takes the payload of the Kafka message and turns it into a Py<PyAny>
fn build_chain(steps: &[Py<RuntimeOperator>]) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let mut next: Box<dyn ProcessingStrategy<Py<PyAny>>> =
        Box::new(CommitOffsets::new(Duration::from_secs(5)));
    for step in steps.iter().rev() {
        next = build(step, next);
    }

    let converter = RunTask::new(
        |message: Message<KafkaPayload>| Ok(to_python(message)),
        next,
    );

    Box::new(converter)
}

struct ArroyoStreamingFactory {
    steps: Vec<Py<RuntimeOperator>>,
}

impl ProcessingStrategyFactory<KafkaPayload> for ArroyoStreamingFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        build_chain(&self.steps)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::RuntimeOperator;
    use crate::routes::Route;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use std::collections::BTreeMap;

    #[test]
    fn test_to_python() {
        Python::with_gil(|py| {
            let payload_data = b"test_payload";
            let message = Message::new_any_message(
                KafkaPayload::new(None, None, Some(payload_data.to_vec())),
                BTreeMap::new(),
            );

            let python_message = to_python(message);

            let py_payload = python_message.payload();

            let down: &Bound<PyBytes> = py_payload.bind(py).downcast().unwrap();
            let payload_bytes: &[u8] = down.as_bytes();
            assert_eq!(payload_bytes, payload_data);
        });
    }

    #[test]
    fn test_build_chain() {
        Python::with_gil(|py| {
            let callable = py
                .eval(
                    c_str!("lambda x: x.decode('utf-8') + '_transformed'"),
                    None,
                    None,
                )
                .unwrap()
                .into_py_any(py)
                .unwrap();

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

            let mut chain = build_chain(&steps);
            let message = Message::new_any_message(
                KafkaPayload::new(None, None, Some(b"test_payload".to_vec())),
                BTreeMap::new(),
            );
            chain.submit(message).unwrap();
        })
    }
}
