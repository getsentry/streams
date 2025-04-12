use crate::kafka_config::PyKafkaConsumerConfig;
use crate::routes::Route;
use crate::transformer::build_map;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::ProcessingStrategy;

/// RuntimeOperator represent a translated step in the streaming pipeline the
/// Arroyo Rust runtime know how to run.
///
/// This enum is exported to Python as this is the data structure the adapter
/// code builds to instruct the Rust runtime on how to add logic to the
/// pipeline.
///
/// RuntimeOperators do not necessarily map 1:1 to the steps in the python Pipeline
/// data model. This are the operations the Rust runtime can run. Multiple
/// Python pipeline steps may be managed by the same Rust runtime step.
#[pyclass]
#[derive(Debug)]
pub enum RuntimeOperator {
    /// Represents a Map transformation in the streaming pipeline.
    /// This translates to a RunTask step in arroyo where a function
    /// is provided to transform the message payload into a different
    /// one.
    #[pyo3(name = "Map")]
    Map { route: Route, function: Py<PyAny> },

    /// Represents a Kafka Producer as a Sink in the pipeline.
    /// It is translated to an Arroyo Kafka producer.
    #[pyo3(name = "StreamSink")]
    StreamSink {
        route: Route,
        topic_name: String,
        kafka_config: PyKafkaConsumerConfig,
    },
}

/*
#[pymethods]
impl RuntimeOperator {
    #[staticmethod]
    pub fn new_map(route: Route, function: Py<PyAny>) -> Self {
        RuntimeOperator::Map { route, function }
    }

    #[staticmethod]
    pub fn new_filter(route: Route, function: Py<PyAny>) -> Self {
        RuntimeOperator::Filter { route, function }
    }

    #[staticmethod]
    pub fn new_router(
        route: Route,
        routing_function: Py<PyAny>,
        branch_names: std::collections::HashSet<String>,
    ) -> Self {
        RuntimeOperator::Router {
            route,
            routing_function,
            branch_names,
        }
    }

    #[staticmethod]
    pub fn new_stream_sink(
        route: Route,
        topic_name: String,
        kafka_config: PyKafkaConsumerConfig,
    ) -> Self {
        RuntimeOperator::StreamSink {
            route,
            topic_name,
            kafka_config,
        }
    }
}
*/

pub fn build(
    step: &Py<RuntimeOperator>,
    next: Box<dyn ProcessingStrategy<Py<PyAny>>>,
) -> Box<dyn ProcessingStrategy<Py<PyAny>>> {
    match step.get() {
        RuntimeOperator::Map { function, .. } => {
            let func_ref = Python::with_gil(|py| function.clone_ref(py));
            build_map(func_ref, next)
        }
        RuntimeOperator::StreamSink { .. } => {
            // Handle StreamSink step
            unimplemented!()
        }
    }
}
