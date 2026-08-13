use pyo3::prelude::*;
use pyo3::types::PyList;

/// Handles wrapping/unwrapping of streams `PyMessage` objects.
///
/// The streams pipeline convention is that all Python callables receive
/// a `Message[T]` wrapper with `.payload`, `.headers`, `.timestamp`,
/// `.schema` attributes. This struct encapsulates that protocol.
pub struct MessageWrapper;

impl MessageWrapper {
    /// Wrap a Python value in a PyMessage if not already wrapped.
    /// Returns the input unchanged if it's already a PyMessage instance.
    pub fn ensure<'py>(
        py: Python<'py>,
        input: Bound<'py, PyAny>,
        timestamp: f64,
        schema: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let message_cls = Self::get_message_class(py)?;

        if input.is_instance(&message_cls)? {
            Ok(input)
        } else {
            let headers = PyList::empty(py);
            let schema_py = schema.into_pyobject(py)?.into_any();
            message_cls.call1((input, headers, timestamp, schema_py))
        }
    }

    /// Re-wrap a callable's return value in a new PyMessage, preserving
    /// headers, timestamp, and schema from the original message.
    pub fn rewrap<'py>(
        py: Python<'py>,
        result: Bound<'py, PyAny>,
        original: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let message_cls = Self::get_message_class(py)?;

        let headers = original.getattr("headers")?;
        let timestamp = original.getattr("timestamp")?;
        let schema = original.getattr("schema")?;

        message_cls
            .call1((result, headers, timestamp, schema))
            .map(|r| r.unbind())
    }

    fn get_message_class<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        py.import("sentry_streams.pipeline.message")?
            .getattr("PyMessage")
    }
}
