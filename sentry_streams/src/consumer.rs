use crate::kafka_config::PyKafkaConsumerConfig;
use crate::steps::ArroyoStep;
use crate::strategies::build;
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

use std::time::Duration;

#[pyclass]
pub struct ArroyoConsumer {
    source: String,

    consumer_config: PyKafkaConsumerConfig,

    topic: String,

    steps: Vec<Py<ArroyoStep>>,

    handle: Option<ProcessorHandle>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(source: String, kafka_config: PyKafkaConsumerConfig, topic: String) -> Self {
        ArroyoConsumer {
            source,
            consumer_config: kafka_config,
            topic,
            steps: Vec::new(),
            handle: None,
        }
    }

    fn add_step(&mut self, step: Py<ArroyoStep>) {
        self.steps.push(step);
    }

    fn dump(&self) {
        println!("Arroyo Consumer:");
        println!("Source: {}", self.source);
        for step in &self.steps {
            println!("Step: {:?}", step);
        }
    }

    fn run(&mut self, py: Python<'_>) {
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

fn to_python(message: Message<KafkaPayload>) -> Message<Py<PyAny>> {
    let payload = Python::with_gil(|py| {
        let payload = message.payload().payload().unwrap();
        let py_bytes = PyBytes::new(py, payload);
        py_bytes.into_any().unbind()
    });
    message.replace(payload)
}

fn build_chain(steps: &[Py<ArroyoStep>]) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
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
    steps: Vec<Py<ArroyoStep>>,
}

impl ProcessingStrategyFactory<KafkaPayload> for ArroyoStreamingFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        build_chain(&self.steps)
    }
}
