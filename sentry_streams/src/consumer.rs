use crate::steps::ArroyoStep;
use crate::strategies::build;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::ProcessingStrategyFactory;
use sentry_arroyo::types::{Message, Partition, Topic};
use std::collections::BTreeMap;

use std::time::Duration;

#[pyclass]
pub struct ArroyoConsumer {
    source: String,

    steps: Vec<Py<ArroyoStep>>,

    factory: Option<ArroyoStreamingFactory>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(source: String) -> Self {
        ArroyoConsumer {
            source,
            steps: Vec::new(),
            factory: None,
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

    fn run(&mut self) {
        let steps_copy = Python::with_gil(|py| {
            self.steps
                .iter()
                .map(|step| step.clone_ref(py))
                .collect::<Vec<_>>()
        });
        let factory = ArroyoStreamingFactory { steps: steps_copy };
        self.factory = Some(factory);
        let mut chain = self.factory.as_mut().expect("Missing factory").create();
        let message1 = Message::new_any_message(
            KafkaPayload::new(Some(b"key1".to_vec()), None, Some(b"payload2".to_vec())),
            BTreeMap::from([(Partition::new(Topic::new("test"), 0), 0)]),
        );

        let message2 = Message::new_any_message(
            KafkaPayload::new(Some(b"key1".to_vec()), None, Some(b"payload3".to_vec())),
            BTreeMap::from([(Partition::new(Topic::new("test"), 0), 0)]),
        );

        chain.submit(message1).unwrap();
        chain.submit(message2).unwrap();
    }

    fn shutdown(&mut self) {}
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
