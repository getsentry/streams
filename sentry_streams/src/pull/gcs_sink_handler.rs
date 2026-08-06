use pyo3::prelude::*;
use sentry_arroyo::processing::stream::handlers::next::NextHandler;
use sentry_arroyo::processing::stream::PipelineEnvelope;

use super::gcs_client::GcsClient;
use super::pipeline_value::PipelineValue;

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

    /// Extract bytes from the pipeline value.
    fn extract_bytes(value: &PipelineValue) -> Result<Vec<u8>, Box<dyn std::error::Error + Send>> {
        match value {
            PipelineValue::Python(obj) => Python::attach(|py| {
                obj.extract::<Vec<u8>>(py)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)
            }),
            PipelineValue::Rust(boxed) => {
                boxed.downcast_ref::<Vec<u8>>().cloned().ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "GcsSinkHandler expected Rust Vec<u8>",
                    )) as Box<dyn std::error::Error + Send>
                })
            }
            PipelineValue::Raw(kp) => Ok(kp.payload().map(|v| v.to_vec()).unwrap_or_default()),
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
        let bytes = Self::extract_bytes(&envelope.payload)?;
        let object_name = self.generate_object_name()?;
        self.client
            .upload(&object_name, &bytes)
            .await
            .map_err(|e| Box::new(e) as _)
    }
}
