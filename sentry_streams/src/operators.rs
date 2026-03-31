use crate::backpressure_metrics::BackpressureNext;
use crate::broadcaster::Broadcaster;
use crate::kafka_config::PyKafkaProducerConfig;
use crate::python_operator::PythonAdapter;
use crate::routers::build_router;
use crate::routes::{Route, RoutedValue};
use crate::sinks::StreamSink;
use crate::store_sinks::GCSSink;
use crate::transformer::{build_filter, build_map};
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::producer::KafkaProducer;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
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
    Map {
        step_name: String,
        route: Route,
        function: Py<PyAny>,
    },

    /// Represents a Filter step in the streaming pipeline.
    /// This translates to a custom Arroyo strategy (Filter step) where a function
    /// is provided to transform the message payload into a bool.
    #[pyo3(name = "Filter")]
    Filter {
        step_name: String,
        route: Route,
        function: Py<PyAny>,
    },

    /// Represents a Kafka Producer as a Sink in the pipeline.
    /// It is translated to an Arroyo Kafka producer.
    #[pyo3(name = "StreamSink")]
    StreamSink {
        step_name: String,
        route: Route,
        topic_name: String,
        kafka_config: PyKafkaProducerConfig,
    },

    #[pyo3(name = "GCSSink")]
    GCSSink {
        step_name: String,
        route: Route,
        bucket: String,
        object_generator: Py<PyAny>,
        thread_count: usize,
    },
    /// Represents a DevNullSink that discards all messages.
    /// Useful for testing and benchmarking pipelines.
    /// Simulates batching with configurable delays.
    #[pyo3(name = "DevNullSink")]
    DevNullSink {
        step_name: String,
        route: Route,
        batch_size: Option<usize>,
        batch_time_ms: Option<f64>,
        average_sleep_time_ms: Option<f64>,
        max_sleep_time_ms: Option<f64>,
    },
    /// Represents a Broadcast step in the pipeline that takes a single
    /// message and submits a copy of that message to each downstream route.
    #[pyo3(name = "Broadcast")]
    Broadcast {
        step_name: String,
        route: Route,
        downstream_routes: Py<PyAny>,
    },
    /// Represents a router step in the pipeline that can send messages
    /// to one of the downstream routes.
    #[pyo3(name = "Router")]
    Router {
        step_name: String,
        route: Route,
        routing_function: Py<PyAny>,
        downstream_routes: Py<PyAny>,
    },
    /// Delegates messages processing to a Python operator that provides
    /// the same kind of interface as an Arroyo strategy. This is meant
    /// to simplify the porting of python strategies to Rust.
    #[pyo3(name = "PythonAdapter")]
    PythonAdapter {
        step_name: String,
        route: Route,
        delegate_factory: Py<PyAny>,
    },
}

impl RuntimeOperator {
    /// Pipeline DSL `Step.name` for backpressure metric labels.
    pub fn pipeline_step_name(&self) -> &str {
        match self {
            RuntimeOperator::Map { step_name, .. }
            | RuntimeOperator::Filter { step_name, .. }
            | RuntimeOperator::StreamSink { step_name, .. }
            | RuntimeOperator::GCSSink { step_name, .. }
            | RuntimeOperator::DevNullSink { step_name, .. }
            | RuntimeOperator::Broadcast { step_name, .. }
            | RuntimeOperator::Router { step_name, .. }
            | RuntimeOperator::PythonAdapter { step_name, .. } => step_name.as_str(),
        }
    }
}

#[pymethods]
impl RuntimeOperator {
    /// Pipeline DSL step name (`Step.name`).
    #[getter]
    fn step_name(&self) -> String {
        self.pipeline_step_name().to_string()
    }
}

/// Wires `next` with backpressure instrumentation using [`RuntimeOperator::pipeline_step_name`].
pub fn build(
    step: &Py<RuntimeOperator>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
    terminator_strategy: Box<dyn ProcessingStrategy<KafkaPayload>>,
    concurrency_config: &ConcurrencyConfig,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let op = step.get();
    let label = op.pipeline_step_name().to_string();
    let next = Box::new(BackpressureNext::new(next, label.clone()));
    match op {
        RuntimeOperator::Map {
            step_name: _,
            function,
            route,
        } => {
            let func_ref = traced_with_gil!(|py| function.clone_ref(py));
            build_map(route, func_ref, next)
        }
        RuntimeOperator::Filter {
            step_name: _,
            function,
            route,
        } => {
            let func_ref = traced_with_gil!(|py| function.clone_ref(py));
            build_filter(route, func_ref, next)
        }
        RuntimeOperator::StreamSink {
            step_name: _,
            route,
            topic_name,
            kafka_config,
        } => {
            let producer = KafkaProducer::new(kafka_config.clone().into());
            Box::new(StreamSink::new(
                route.clone(),
                producer,
                concurrency_config,
                topic_name,
                next,
                terminator_strategy,
                label,
            ))
        }
        RuntimeOperator::GCSSink {
            step_name: _,
            route,
            bucket,
            object_generator,
            thread_count: _,
        } => {
            let func_ref = traced_with_gil!(|py| { object_generator.clone_ref(py) });

            Box::new(GCSSink::new(
                route.clone(),
                next,
                concurrency_config,
                bucket,
                func_ref,
            ))
        }
        RuntimeOperator::DevNullSink {
            step_name: _,
            route,
            batch_size,
            batch_time_ms,
            average_sleep_time_ms,
            max_sleep_time_ms,
        } => {
            use crate::dev_null_sink::DevNullSink;
            Box::new(DevNullSink::new(
                route.clone(),
                next,
                *batch_size,
                *batch_time_ms,
                *average_sleep_time_ms,
                *max_sleep_time_ms,
            ))
        }
        RuntimeOperator::Router {
            step_name: _,
            route,
            routing_function,
            #[allow(unused_variables)]
            downstream_routes,
        } => {
            let func_ref = traced_with_gil!(|py| { routing_function.clone_ref(py) });

            build_router(route, func_ref, next)
        }
        RuntimeOperator::PythonAdapter {
            step_name: _,
            route,
            delegate_factory,
        } => {
            let factory = traced_with_gil!(|py| { delegate_factory.clone_ref(py) });
            Box::new(PythonAdapter::new(route.clone(), factory, next, label))
        }
        RuntimeOperator::Broadcast {
            step_name: _,
            route,
            downstream_routes,
        } => {
            let rust_branches =
                traced_with_gil!(|py| { downstream_routes.extract::<Vec<String>>(py).unwrap() });
            Box::new(Broadcaster::new(next, route.clone(), rust_branches))
        }
    }
}
