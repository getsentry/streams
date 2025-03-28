use std::ops::Deref;

use crate::steps::ArroyoStep;
use crate::strategies::build;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::config::KafkaConfig;

use pyo3::types::PyBytes;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::backends::kafka::InitialOffset;
use sentry_arroyo::processing::strategies::commit_offsets::CommitOffsets;
use sentry_arroyo::processing::strategies::noop::Noop;
use sentry_arroyo::processing::strategies::produce::Produce;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
use sentry_arroyo::processing::strategies::{
    ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
};
use sentry_arroyo::processing::StreamProcessor;
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};

use std::time::Duration;

#[pyclass]
pub struct ArroyoConsumer {
    source: String,

    steps: Vec<Py<ArroyoStep>>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(source: String) -> Self {
        ArroyoConsumer {
            source,
            steps: Vec::new(),
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

    fn run(&mut self, msg: Py<PyAny>) {}

    fn shutdown(&mut self) {}
}

fn pythonify(message: &Message<KafkaPayload>) -> Py<PyAny> {
    Python::with_gil(|py| {
        let payload = message.payload().payload().unwrap();
        let py_bytes = PyBytes::new(py, payload);
        Ok(py_bytes.into())
    })
    .unwrap()
}

fn build_chain(steps: Vec<Py<ArroyoStep>>) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
    let mut next = CommitOffsets::new(Duration::from_secs(5));
    for step in steps.iter().rev() {
        let next = build(step, next);
    }

    Box::new(next)
}

//fn build_strategy_factory() -> ProcessingStrategyFactory<KafkaPayload> {
//    struct ArroyoStreamingFactory {
//        steps: Vec<Py<ArroyoStep>>,
//    }
//
//    impl ProcessingStrategyFactory<KafkaPayload> for ArroyoStreamingFactory {
//        fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {}
//    }
//}
