use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use super::pipeline_value::PipelineValue;
use super::stages::batch::BatchAccumulatorStage;
use super::stages::header_filter::HeaderFilterStage;
use super::stages::py_callable::PyCallableStage;

/// Enum dispatch for pipeline stages, avoiding trait object limitations.
///
/// `Stage` is not object-safe (returns `impl Future`), so we can't use
/// `Box<dyn Stage>`. Instead, this enum wraps all concrete stage types
/// and delegates `process()` via match. Zero-cost dispatch.
pub enum PipelineStage {
    HeaderFilter(HeaderFilterStage),
    Batch(BatchAccumulatorStage),
    PyCallable(PyCallableStage),
}

impl Stage for PipelineStage {
    type In = PipelineValue;
    type Out = PipelineValue;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineValue>,
    ) -> StageResult<PipelineValue> {
        match self {
            PipelineStage::HeaderFilter(s) => s.process(envelope).await,
            PipelineStage::Batch(s) => s.process(envelope).await,
            PipelineStage::PyCallable(s) => s.process(envelope).await,
        }
    }

    fn name(&self) -> &str {
        match self {
            PipelineStage::HeaderFilter(s) => s.name(),
            PipelineStage::Batch(s) => s.name(),
            PipelineStage::PyCallable(s) => s.name(),
        }
    }
}
