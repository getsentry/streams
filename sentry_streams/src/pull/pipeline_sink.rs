use std::sync::{Arc, Mutex};

use sentry_arroyo::processing::stream::handlers::next::NextHandler;
use sentry_arroyo::processing::stream::PipelineEnvelope;

use super::gcs_sink_handler::GcsSinkHandler;
use super::pipeline_value::PipelineValue;
use super::pipeline_value_converter::PipelineValueConverter;

/// Enum dispatch for pipeline sinks. Mirrors PipelineStage pattern.
pub enum PipelineSink {
    Gcs(GcsSinkHandler),
    /// Test sink that captures received payloads.
    Mock(MockSinkHandler),
}

impl NextHandler<PipelineValue> for PipelineSink {
    async fn handle(
        &self,
        envelope: &PipelineEnvelope<PipelineValue>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        match self {
            PipelineSink::Gcs(h) => h.handle(envelope).await,
            PipelineSink::Mock(h) => h.handle(envelope).await,
        }
    }
}

/// Mock sink handler that records what it receives as extracted bytes.
pub struct MockSinkHandler {
    results: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl MockSinkHandler {
    pub fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_results(&self) -> Vec<Vec<u8>> {
        self.results.lock().unwrap().clone()
    }
}

impl NextHandler<PipelineValue> for MockSinkHandler {
    async fn handle(
        &self,
        envelope: &PipelineEnvelope<PipelineValue>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let bytes = PipelineValueConverter::extract_bytes(&envelope.payload)
            .unwrap_or_else(|_| b"<extraction-failed>".to_vec());
        self.results.lock().unwrap().push(bytes);
        Ok(())
    }
}
