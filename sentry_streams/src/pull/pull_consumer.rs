use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::Stream;
use futures::StreamExt;
use pyo3::prelude::*;
use sentry_arroyo::processing::stream::{
    LogHandler, OffsetTracker, PipelineExit, PipelineExt, PullSource, StageResult,
};

use super::pipeline_sink::PipelineSink;
use super::pipeline_stage::PipelineStage;
use super::pipeline_value::PipelineValue;
use super::pull_operator::PullOperator;
use super::pull_source::PullSourceConfig;

/// Pull-based pipeline consumer. Stores operator descriptions and
/// rebuilds fresh stages on each partition assignment (rebalance).
/// `run()` is parameterless — handles rebalance restart and signal shutdown.
#[pyclass]
pub struct PullConsumer {
    source: Arc<dyn PullSource>,
    operators: Vec<Py<PullOperator>>,
    sink_operator: Option<Py<PullOperator>>,
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
        let source: Arc<dyn PullSource> = Arc::from(source.get().build(py));
        Self {
            source,
            operators: steps,
            sink_operator: sink,
        }
    }

    /// Run the pipeline. Blocks until completion, fatal error, or signal.
    /// Rebuilds stages fresh on each rebalance.
    fn run(&self) -> PyResult<()> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to create tokio runtime: {e}"
            ))
        })?;

        rt.block_on(async {
            self.install_signal_handlers();
            loop {
                // Build fresh stages and sink for this assignment
                let (stages, sink) = Python::attach(|py| {
                    let stages: Vec<PipelineStage> = self
                        .operators
                        .iter()
                        .map(|op| op.get().build_stage(py))
                        .collect();
                    let sink = self.sink_operator.as_ref().map(|s| s.get().build_sink(py));
                    (stages, sink)
                });

                let exit = Self::run_pipeline(&self.source, &stages, sink.as_ref()).await;

                // Always signal drain complete — unblocks the rebalance callback
                // if one is waiting. No-op if no rebalance is in progress.
                self.source.signal_drain_complete();

                let exit = exit.map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("Pipeline failed: {e}"))
                })?;

                match exit {
                    PipelineExit::Rebalance => {
                        tracing::info!("Rebalance detected, restarting pipeline");
                        continue;
                    }
                    PipelineExit::Shutdown | PipelineExit::Complete => {
                        return Ok(());
                    }
                }
            }
        })
    }
}

impl PullConsumer {
    /// Run a single pipeline iteration.
    pub async fn run_pipeline(
        source: &Arc<dyn PullSource>,
        stages: &[PipelineStage],
        sink: Option<&PipelineSink>,
    ) -> Result<PipelineExit, Box<dyn std::error::Error + Send>> {
        let committer = source.committer();
        let error_handler = LogHandler;
        let commit_interval = Duration::from_secs(5);
        let mut tracker = OffsetTracker::new(commit_interval, committer);

        let source_stream = source.stream().map(|result| match result {
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
            StageResult::Exit(reason) => StageResult::Exit(reason),
        });

        let mut stream: Pin<Box<dyn Stream<Item = StageResult<PipelineValue>> + '_>> =
            Box::pin(source_stream);

        for stage in stages {
            stream = Box::pin(stream.apply(stage));
        }

        if let Some(sink) = sink {
            stream
                .on_next(sink)
                .on_reject(&error_handler)
                .commit(&mut tracker)
                .await
        } else {
            stream.on_reject(&error_handler).commit(&mut tracker).await
        }
    }

    fn install_signal_handlers(&self) {
        let source = self.source.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!("Received SIGINT, shutting down...");
                source.shutdown();
            }
        });

        #[cfg(unix)]
        {
            let source = self.source.clone();
            tokio::spawn(async move {
                let mut sigterm =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("Failed to register SIGTERM handler");

                sigterm.recv().await;
                tracing::info!("Received SIGTERM, shutting down...");
                source.shutdown();
            });
        }
    }
}
