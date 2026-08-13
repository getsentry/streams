use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use sentry_arroyo::backends::kafka::types::KafkaPayload;

use super::pipeline_value::PipelineValue;

/// Converts PipelineValue to/from Python objects and byte arrays.
/// Keeps conversion logic out of the PipelineValue enum and the stages.
pub struct PipelineValueConverter;

impl PipelineValueConverter {
    /// Convert a PipelineValue to a Python object.
    pub fn to_python<'py>(value: &PipelineValue, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match value {
            PipelineValue::Raw(kp) => {
                let bytes = kp.payload().map(|v| v.as_slice()).unwrap_or(&[]);
                Ok(PyBytes::new(py, bytes).into_any())
            }
            PipelineValue::Rust(boxed) => {
                if let Some(payloads) = boxed.downcast_ref::<Vec<KafkaPayload>>() {
                    let items: PyResult<Vec<Bound<'py, PyAny>>> = payloads
                        .iter()
                        .map(|kp| {
                            let bytes = kp.payload().map(|v| v.as_slice()).unwrap_or(&[]);
                            Ok(PyBytes::new(py, bytes).into_any())
                        })
                        .collect();
                    Ok(PyList::new(py, &items?)?.into_any())
                } else {
                    Err(pyo3::exceptions::PyTypeError::new_err(
                        "PipelineValueConverter: unsupported Rust type for Python conversion",
                    ))
                }
            }
            PipelineValue::Python(obj) => Ok(obj.bind(py).clone().into_any()),
        }
    }

    /// Extract raw bytes from a PipelineValue.
    /// Unwraps Message wrappers (objects with .payload attribute) automatically.
    pub fn extract_bytes(
        value: &PipelineValue,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send>> {
        match value {
            PipelineValue::Raw(kp) => Ok(kp.payload().map(|v| v.to_vec()).unwrap_or_default()),
            PipelineValue::Rust(boxed) => {
                boxed.downcast_ref::<Vec<u8>>().cloned().ok_or_else(|| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "PipelineValueConverter: expected Rust Vec<u8>",
                    )) as Box<dyn std::error::Error + Send>
                })
            }
            PipelineValue::Python(obj) => Python::attach(|py| {
                let bound = obj.bind(py);

                // Unwrap Message wrapper if present (.payload attribute)
                let inner = if let Ok(payload) = bound.getattr("payload") {
                    payload
                } else {
                    bound.clone()
                };

                inner.extract::<Vec<u8>>().map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("PipelineValueConverter: failed to extract bytes: {e}"),
                    )) as Box<dyn std::error::Error + Send>
                })
            }),
        }
    }
}
