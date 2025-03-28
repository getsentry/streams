use crate::kafka_config::PyKafkaConfig;
use crate::routes::Route;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use std::sync::Arc;

pub trait ArroyoStep {
    fn build(
        &self,
        next: Arc<dyn ProcessingStrategy<KafkaPayload>>,
    ) -> Arc<dyn ProcessingStrategy<KafkaPayload>>;
}

#[pyclass]
pub struct MapStep {
    // Represents a Map transformation in the streaming pipeline.
    // This translates to a RunTask step in arroyo where a function
    // is provided to transform the message payload into a different
    // one.
    #[pyo3(get, set)]
    route: Route,
    #[pyo3(get, set)]
    function: Py<PyAny>,
}

#[pymethods]
impl MapStep {
    #[new]
    fn new(route: Route, function: Py<PyAny>) -> Self {
        MapStep { route, function }
    }
}

#[pyclass]
pub struct FilterStep {
    // Represents a Filter transformation in the streaming pipeline.
    // This translates to a RunTask step in arroyo where a message is filtered
    // based on the result of a filter function.
    #[pyo3(get, set)]
    route: Route,
    #[pyo3(get, set)]
    function: Py<PyAny>,
}

#[pymethods]
impl FilterStep {
    #[new]
    fn new(route: Route, function: Py<PyAny>) -> Self {
        FilterStep { route, function }
    }
}

#[pyclass]
pub struct RouterStep {
    #[pyo3(get, set)]
    route: Route,
    #[pyo3(get, set)]
    routing_function: Py<PyAny>,
    #[pyo3(get, set)]
    branch_names: std::collections::HashSet<String>,
}

#[pymethods]
impl RouterStep {
    // Represents a Router which can direct a message to one of multiple
    // downstream branches based on the output of a routing function.
    //
    // Since Arroyo has no concept of 'branches', this updates the `waypoints` list within
    // a message's `Route` object based on the result of the routing function.

    #[new]
    fn new(
        route: Route,
        routing_function: Py<PyAny>,
        branch_names: std::collections::HashSet<String>,
    ) -> Self {
        RouterStep {
            route,
            routing_function,
            branch_names,
        }
    }
}

#[pyclass]
pub struct StreamSinkStep {
    #[pyo3(get, set)]
    route: Route,
    #[pyo3(get, set)]
    topic_name: String,
    #[pyo3(get, set)]
    kafka_config: PyKafkaConfig,
}

#[pymethods]
impl StreamSinkStep {
    // StreamSinkStep is backed by the Forwarder custom strategy, which either produces
    // messages to a topic via an arroyo Producer or forwards messages to the next downstream
    // step.
    // This allows the use of multiple sinks, each at the end of a different branch of a Router step.

    #[new]
    fn new(route: Route, topic_name: String, kafka_config: PyKafkaConfig) -> Self {
        StreamSinkStep {
            route,
            topic_name,
            kafka_config,
        }
    }
}
