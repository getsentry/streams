use pyo3::prelude::*;

use super::gcs_client::GcsClient;
use super::gcs_sink_handler::GcsSinkHandler;
use super::pipeline_sink::{MockSinkHandler, PipelineSink};
use super::pipeline_stage::PipelineStage;
use super::stages::batch::BatchAccumulatorStage;
use super::stages::header_filter::HeaderFilterStage;
use super::stages::py_callable::PyCallableStage;

/// Operator enum passed from Python to describe a pipeline step.
#[pyclass]
pub enum PullOperator {
    #[pyo3(constructor = (header_name, expected_value))]
    HeaderFilter {
        header_name: String,
        expected_value: i64,
    },

    #[pyo3(constructor = (max_batch_size))]
    Batch { max_batch_size: usize },

    #[pyo3(constructor = (callable, name, schema))]
    PyCallable {
        callable: Py<PyAny>,
        name: String,
        schema: String,
    },

    #[pyo3(constructor = (bucket, object_generator))]
    GcsSink {
        bucket: String,
        object_generator: Py<PyAny>,
    },

    #[pyo3(constructor = ())]
    MockSink {},
}

impl PullOperator {
    pub fn build_stage(&self, py: Python<'_>) -> PipelineStage {
        match self {
            PullOperator::HeaderFilter {
                header_name,
                expected_value,
            } => PipelineStage::HeaderFilter(HeaderFilterStage::new(
                header_name.clone(),
                *expected_value,
            )),
            PullOperator::Batch { max_batch_size } => {
                PipelineStage::Batch(BatchAccumulatorStage::new(*max_batch_size))
            }
            PullOperator::PyCallable {
                callable,
                name,
                schema,
            } => PipelineStage::PyCallable(PyCallableStage::new(
                callable.clone_ref(py),
                name.clone(),
                schema.clone(),
            )),
            PullOperator::GcsSink { .. } | PullOperator::MockSink { .. } => {
                panic!("Sink operators are not stages — use build_sink()")
            }
        }
    }

    pub fn build_sink(&self, py: Python<'_>) -> PipelineSink {
        match self {
            PullOperator::GcsSink {
                bucket,
                object_generator,
            } => {
                let client = GcsClient::with_defaults(bucket.clone());
                PipelineSink::Gcs(GcsSinkHandler::new(client, object_generator.clone_ref(py)))
            }
            PullOperator::MockSink {} => PipelineSink::Mock(MockSinkHandler::new()),
            _ => panic!("build_sink() called on non-sink operator"),
        }
    }
}
