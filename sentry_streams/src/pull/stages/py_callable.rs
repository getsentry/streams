use pyo3::prelude::*;
use pyo3::types::PyList;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use crate::pull::pipeline_value::PipelineValue;

/// Calls a Python callable as a pipeline stage.
///
/// Handles three input scenarios:
/// - PipelineValue::Rust(Vec<KafkaPayload>) — converts to Python list of bytes,
///   calls the callable. Used for batch_parser.
/// - PipelineValue::Python — passes the Python object directly to the callable.
///   Used for processor, serializer.
/// - PipelineValue::Raw — converts single KafkaPayload to Python bytes,
///   calls the callable. Used for single-message transforms.
///
/// Output is always PipelineValue::Python (the callable's return value).
///
/// Python exceptions become StageResult::Fail (no DLQ for now).
pub struct PyCallableStage {
    callable: Py<PyAny>,
    stage_name: String,
}

impl PyCallableStage {
    pub fn new(callable: Py<PyAny>, name: impl Into<String>) -> Self {
        Self {
            callable,
            stage_name: name.into(),
        }
    }

    /// Convert a Vec<KafkaPayload> to a Python list of bytes objects.
    fn batch_to_python<'py>(
        py: Python<'py>,
        payloads: Vec<KafkaPayload>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let items: Vec<Bound<'py, PyAny>> = payloads
            .iter()
            .map(|kp| {
                let bytes = kp.payload().map(|v| v.as_slice()).unwrap_or(&[]);
                pyo3::types::PyBytes::new(py, bytes).into_any()
            })
            .collect();
        Ok(PyList::new(py, &items)?.into_any())
    }

    /// Convert a single KafkaPayload to Python bytes.
    fn raw_to_python<'py>(
        py: Python<'py>,
        payload: &KafkaPayload,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = payload.payload().map(|v| v.as_slice()).unwrap_or(&[]);
        Ok(pyo3::types::PyBytes::new(py, bytes).into_any())
    }
}

impl Stage for PyCallableStage {
    type In = PipelineValue;
    type Out = PipelineValue;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineValue>,
    ) -> StageResult<PipelineValue> {
        let result = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let input: Bound<'_, PyAny> = match envelope.payload {
                PipelineValue::Rust(ref boxed) => {
                    // Try to downcast as Vec<KafkaPayload> (batch)
                    if let Some(payloads) = boxed.downcast_ref::<Vec<KafkaPayload>>() {
                        Self::batch_to_python(py, payloads.clone())?
                    } else {
                        return Err(pyo3::exceptions::PyTypeError::new_err(
                            "PyCallableStage received unsupported Rust type",
                        ));
                    }
                }
                PipelineValue::Python(ref obj) => obj.bind(py).clone().into_any(),
                PipelineValue::Raw(ref kp) => Self::raw_to_python(py, kp)?,
            };

            self.callable.call1(py, (input,))
        });

        match result {
            Ok(output) => StageResult::Emit(PipelineEnvelope::new(
                PipelineValue::Python(output),
                envelope.metadata,
                envelope.raw,
            )),
            Err(py_err) => {
                // For now, all Python errors are fatal (no DLQ)
                StageResult::Fail(Box::new(py_err))
            }
        }
    }

    fn name(&self) -> &'static str {
        // Leak the string to get a &'static str.
        // This is fine — stages are long-lived, created once at pipeline build time.
        Box::leak(self.stage_name.clone().into_boxed_str())
    }
}
