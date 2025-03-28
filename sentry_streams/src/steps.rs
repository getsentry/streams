use crate::kafka_config::PyKafkaConfig;
use crate::routes::Route;
use pyo3::prelude::*;

#[pyclass]
#[derive(Debug)]
pub enum ArroyoStep {
    // Represents a Map transformation in the streaming pipeline.
    // This translates to a RunTask step in arroyo where a function
    // is provided to transform the message payload into a different
    // one.
    #[pyo3(name = "Map")]
    Map { route: Route, function: Py<PyAny> },

    // Represents a Filter transformation in the streaming pipeline.
    // This translates to a RunTask step in arroyo where a message is filtered
    // based on the result of a filter function.
    #[pyo3(name = "Filter")]
    Filter { route: Route, function: Py<PyAny> },

    // Represents a Router which can direct a message to one of multiple
    // downstream branches based on the output of a routing function.
    //
    // Since Arroyo has no concept of 'branches', this updates the `waypoints` list within
    // a message's `Route` object based on the result of the routing function.
    #[pyo3(name = "Router")]
    Router {
        route: Route,
        routing_function: Py<PyAny>,
        branch_names: std::collections::HashSet<String>,
    },

    // StreamSinkStep is backed by the Forwarder custom strategy, which either produces
    // messages to a topic via an arroyo Producer or forwards messages to the next downstream
    // step.
    // This allows the use of multiple sinks, each at the end of a different branch of a Router step.
    #[pyo3(name = "StreamSink")]
    StreamSink {
        route: Route,
        topic_name: String,
        kafka_config: PyKafkaConfig,
    },
}

#[pymethods]
impl ArroyoStep {
    #[staticmethod]
    pub fn new_map(route: Route, function: Py<PyAny>) -> Self {
        ArroyoStep::Map { route, function }
    }

    #[staticmethod]
    pub fn new_filter(route: Route, function: Py<PyAny>) -> Self {
        ArroyoStep::Filter { route, function }
    }

    #[staticmethod]
    pub fn new_router(
        route: Route,
        routing_function: Py<PyAny>,
        branch_names: std::collections::HashSet<String>,
    ) -> Self {
        ArroyoStep::Router {
            route,
            routing_function,
            branch_names,
        }
    }

    #[staticmethod]
    pub fn new_stream_sink(route: Route, topic_name: String, kafka_config: PyKafkaConfig) -> Self {
        ArroyoStep::StreamSink {
            route,
            topic_name,
            kafka_config,
        }
    }
}
