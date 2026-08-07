use sentry_arroyo::processing::stream::handlers::next::NextHandler;
use sentry_arroyo::processing::stream::PipelineEnvelope;

use super::gcs_sink_handler::GcsSinkHandler;
use super::pipeline_value::PipelineValue;

/// Enum dispatch for pipeline sinks.
pub enum PipelineSink {
    Gcs(GcsSinkHandler),
}

impl NextHandler<PipelineValue> for PipelineSink {
    async fn handle(
        &self,
        envelope: &PipelineEnvelope<PipelineValue>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        match self {
            PipelineSink::Gcs(h) => h.handle(envelope).await,
        }
    }
}
