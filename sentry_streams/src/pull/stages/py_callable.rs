use pyo3::prelude::*;
use sentry_arroyo::processing::stream::{PipelineEnvelope, Stage, StageResult};

use crate::pull::message_wrapper::MessageWrapper;
use crate::pull::pipeline_value::PipelineValue;
use crate::pull::pipeline_value_converter::PipelineValueConverter;

/// Calls a Python callable as a pipeline stage.
///
/// Wraps the input in a streams `Message` object before calling,
/// and re-wraps the output. All streams pipeline callables expect
/// `Message[T]` with `.payload`, `.headers`, `.timestamp`, `.schema`.
pub struct PyCallableStage {
    callable: Py<PyAny>,
    stage_name: String,
    schema: String,
}

impl PyCallableStage {
    pub fn new(callable: Py<PyAny>, name: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            callable,
            stage_name: name.into(),
            schema: schema.into(),
        }
    }
}

impl Stage for PyCallableStage {
    type In = PipelineValue;
    type Out = PipelineValue;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineValue>,
    ) -> StageResult<PipelineValue> {
        let timestamp = envelope.metadata.timestamp.timestamp_millis() as f64 / 1000.0;
        let schema = &self.schema;

        let result = Python::attach(|py| -> PyResult<Py<PyAny>> {
            let input = PipelineValueConverter::to_python(&envelope.payload, py)?;
            let message = MessageWrapper::ensure(py, input, timestamp, schema)?;
            let result = self.callable.call1(py, (&message,))?;
            MessageWrapper::rewrap(py, result.bind(py).clone(), &message)
        });

        match result {
            Ok(output) => StageResult::Emit(PipelineEnvelope::new(
                PipelineValue::Python(output),
                envelope.metadata,
                envelope.raw,
            )),
            Err(py_err) => StageResult::Fail(Box::new(py_err)),
        }
    }

    fn name(&self) -> &str {
        &self.stage_name
    }
}
