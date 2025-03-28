use crate::steps::ArroyoStep;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::{
    ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
};
use sentry_arroyo::types::{Message, Topic, TopicOrPartition};

pub fn build(
    step: &Py<ArroyoStep>,
    next: Box<dyn ProcessingStrategy<Py<PyAny>>>,
) -> Box<dyn ProcessingStrategy<Py<PyAny>>> {
    match step.get() {
        ArroyoStep::Map { function, .. } => {
            let func_ref = Python::with_gil(|py| function.clone_ref(py));
            build_map(func_ref, next)
        }
        ArroyoStep::Filter { function, .. } => {
            let func_ref = Python::with_gil(|py| function.clone_ref(py));
            unimplemented!()
            //Strategy::new(get_function(func_ref))
        }
        ArroyoStep::Router {
            routing_function, ..
        } => {
            let func_ref = Python::with_gil(|py| routing_function.clone_ref(py));
            unimplemented!()
            //Strategy::new(get_function(func_ref))
        }
        ArroyoStep::StreamSink { .. } => {
            // Handle StreamSink step
            unimplemented!()
        }
    }
}

fn build_map(
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<Py<PyAny>>>,
) -> Box<dyn ProcessingStrategy<Py<PyAny>>> {
    fn call_python_function(callable: &Py<PyAny>, message: &Message<Py<PyAny>>) -> Py<PyAny> {
        Python::with_gil(|py| {
            let result = callable.call1(py, (message.payload(),)).unwrap();
            result
        })
    }

    let mapper = move |message| {
        let transformed = call_python_function(&callable, &message);
        Ok(message.replace(transformed))
    };
    Box::new(RunTask::new(mapper, next))
}
