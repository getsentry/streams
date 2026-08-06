use std::pin::Pin;
use std::time::Duration;

use futures::stream::Stream;
use futures::StreamExt;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::offset_tracker::OffsetTracker;
use sentry_arroyo::processing::stream::{LogHandler, PipelineExt, StageResult};

use super::pipeline_sink::PipelineSink;
use super::pipeline_stage::PipelineStage;
use super::pipeline_value::PipelineValue;
use super::pull_operator::PullOperator;
use super::pull_source::{PullSource, PullSourceConfig};

/// Pull-based pipeline consumer. Fully configured at construction time.
/// `run()` is parameterless.
#[pyclass]
pub struct PullConsumer {
    source: Box<dyn PullSource>,
    stages: Vec<PipelineStage>,
    pub sink: Option<PipelineSink>,
}

#[pymethods]
impl PullConsumer {
    #[new]
    fn new(
        py: Python<'_>,
        source: Py<PullSourceConfig>,
        steps: Vec<Py<PullOperator>>,
        sink: Option<Py<PullOperator>>,
    ) -> Self {
        let source = source.get().build(py);
        let stages = steps.iter().map(|op| op.get().build_stage(py)).collect();
        let sink = sink.map(|s| s.get().build_sink(py));

        Self {
            source,
            stages,
            sink,
        }
    }

    /// Run the pipeline. Blocks until completion or fatal error.
    fn run(&self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create tokio runtime: {e}"
            ))
        })?;

        rt.block_on(self.run_pipeline())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Pipeline failed: {e}")))
    }

    /// Get results captured by a MockSink. Returns a list of byte arrays.
    /// Raises RuntimeError if the sink is not a MockSink.
    fn get_mock_sink_results(&self) -> PyResult<Vec<Vec<u8>>> {
        match &self.sink {
            Some(PipelineSink::Mock(handler)) => Ok(handler.get_results()),
            _ => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Sink is not a MockSink",
            )),
        }
    }
}

impl PullConsumer {
    /// Construct from Rust with an injected source (for testing).
    pub fn with_source(
        source: impl PullSource + 'static,
        stages: Vec<PipelineStage>,
        sink: Option<PipelineSink>,
    ) -> Self {
        Self {
            source: Box::new(source),
            stages,
            sink,
        }
    }

    pub async fn run_pipeline(&self) -> Result<(), Box<dyn std::error::Error + Send>> {
        let committer = self.source.committer();
        let error_handler = LogHandler;
        let commit_interval = Duration::from_secs(5);
        let mut tracker = OffsetTracker::new(commit_interval, committer);

        let source_stream = self.source.stream().map(|result| match result {
            StageResult::Emit(envelope) => {
                StageResult::Emit(envelope.map_payload(PipelineValue::Raw))
            }
            StageResult::Drop { metadata } => StageResult::Drop { metadata },
            StageResult::Skip => StageResult::Skip,
            StageResult::Reject {
                metadata,
                raw,
                reason,
            } => StageResult::Reject {
                metadata,
                raw,
                reason,
            },
            StageResult::Fail(e) => StageResult::Fail(e),
        });

        let mut stream: Pin<Box<dyn Stream<Item = StageResult<PipelineValue>> + '_>> =
            Box::pin(source_stream);

        for stage in &self.stages {
            stream = Box::pin(stream.apply(stage));
        }

        if let Some(sink) = &self.sink {
            stream
                .on_next(sink)
                .on_reject(&error_handler)
                .commit(&mut tracker)
                .await
        } else {
            stream.on_reject(&error_handler).commit(&mut tracker).await
        }
    }
}
