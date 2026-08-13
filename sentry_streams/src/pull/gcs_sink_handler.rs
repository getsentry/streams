use pyo3::prelude::*;
use sentry_arroyo::processing::stream::handlers::next::NextHandler;
use sentry_arroyo::processing::stream::PipelineEnvelope;

use super::gcs_client::GcsClient;
use super::pipeline_value::PipelineValue;
use super::pipeline_value_converter::PipelineValueConverter;

/// Sink handler that uploads pipeline output to GCS.
///
/// Used with `.on_next(&gcs_sink)` in the pipeline. Extracts bytes
/// from the envelope payload, generates an object name via a Python
/// callable, and delegates to `GcsClient` for the actual upload.
pub struct GcsSinkHandler {
    client: GcsClient,
    object_generator: Py<PyAny>,
}

impl GcsSinkHandler {
    pub fn new(client: GcsClient, object_generator: Py<PyAny>) -> Self {
        Self {
            client,
            object_generator,
        }
    }

    fn generate_object_name(&self) -> Result<String, Box<dyn std::error::Error + Send>> {
        Python::attach(|py| {
            let result = self
                .object_generator
                .call0(py)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
            result
                .extract::<String>(py)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
        })
    }
}

impl NextHandler<PipelineValue> for GcsSinkHandler {
    async fn handle(
        &self,
        envelope: &PipelineEnvelope<PipelineValue>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let bytes = PipelineValueConverter::extract_bytes(&envelope.payload)?;
        let object_name = self.generate_object_name()?;
        self.client
            .upload(&object_name, &bytes)
            .await
            .map_err(|e| Box::new(e) as _)
    }
}
