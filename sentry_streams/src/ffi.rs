use pyo3::prelude::*;

pub const RUST_FUNCTION_VERSION: usize = 1;

/// The message type exposed to Rust functions, with typed payload.
#[derive(Debug, Clone)]
pub struct Message<T> {
    payload: T,
    headers: Vec<(String, Vec<u8>)>,
    timestamp: f64,
    schema: Option<String>,
}

impl<T> Message<T> {
    /// Split up the message into payload and metadata
    pub fn take(self) -> (T, Message<()>) {
        (
            self.payload,
            Message {
                payload: (),
                headers: self.headers,
                timestamp: self.timestamp,
                schema: self.schema,
            },
        )
    }
}

/// Convert a Python payload into a given Rust type
///
/// You can implement this trait easiest by calling `convert_via_json!(MyType)`, provided your type
/// is JSON-serializable and deserializable on both sides.
pub trait FromPythonPayload {
    fn from_python_payload(
        py: pyo3::Python<'_>,
        value: pyo3::Py<pyo3::PyAny>,
    ) -> pyo3::PyResult<Self>;
}

/// Convert a Rust type back into a Python payload
///
/// You can implement this trait easiest by calling `convert_via_json!(MyType)`, provided your type
/// is JSON-serializable and deserializable with serde.
pub trait IntoPythonPayload {
    fn into_python_payload(self, py: pyo3::Python<'_>) -> pyo3::Py<pyo3::PyAny>;
}

/// Implement type conversion from/to Python by roundtripping with `serde_json` and `json.loads`.
///
/// You need `serde_json` and `pyo3` in your crate's dependencies.
macro_rules! convert_via_json {
    ($ty:ty) => {
        impl FromPythonPayload for $ty {
            fn from_python_payload(
                py: pyo3::Python<'_>,
                value: ::pyo3::Py<pyo3::PyAny>,
            ) -> ::pyo3::PyResult<Self> {
                use pyo3::prelude::*;

                let payload_json = py
                    .import("json")?
                    .getattr("dumps")?
                    .call1((value,))?
                    .extract::<String>()?;

                let payload_value: Self = ::serde_json::from_str(&payload_json).map_err(|e| {
                    ::pyo3::PyErr::new::<::pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to parse JSON: {}",
                        e
                    ))
                })?;

                Ok(payload_value)
            }
        }

        impl IntoPythonPayload for $ty {
            fn into_python_payload(self, py: ::pyo3::Python<'_>) -> ::pyo3::Py<pyo3::PyAny> {
                use pyo3::prelude::*;

                let payload_json = ::serde_json::to_string(&rust_msg.payload).map_err(|e| {
                    ::pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to serialize JSON: {}",
                        e
                    ))
                })?;
                let payload_obj = py
                    .import("json")?
                    .getattr("loads")?
                    .call1((payload_json,))?;

                Ok(payload_obj)
            }
        }
    };
}

pub use convert_via_json;

/// Convert a Python streaming message to a typed Rust Message format
/// This function handles the conversion for any type that implements serde::Deserialize
pub fn convert_py_message_to_rust<T>(
    py: pyo3::Python,
    py_msg: &pyo3::Py<pyo3::PyAny>,
) -> pyo3::PyResult<crate::message::Message<T>>
where
    T: FromPythonPayload,
{
    let payload_obj = py_msg.bind(py).getattr("payload")?;
    let payload_value = T::from_python_payload(payload_obj);

    let headers_py: Vec<(String, Vec<u8>)> = py_msg.bind(py).getattr("headers")?.extract()?;
    let timestamp: f64 = py_msg.bind(py).getattr("timestamp")?.extract()?;
    let schema: Option<String> = py_msg.bind(py).getattr("schema")?.extract()?;

    Ok(crate::message::Message::new(
        payload_value,
        headers_py,
        timestamp,
        schema,
    ))
}

pub fn convert_rust_message_to_py<T>(
    py: pyo3::Python,
    rust_msg: crate::message::Message<T>,
) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>
where
    T: IntoPythonPayload,
{
    let payload_obj = rust_msg.payload.into_python_payload()?;

    // Create a new Python message with the transformed payload
    let py_msg_class = py
        .import("sentry_streams.rust_streams")?
        .getattr("PyAnyMessage")?;

    let result = py_msg_class
        .call1((
            payload_obj,
            rust_msg.headers,
            rust_msg.timestamp,
            rust_msg.schema,
        ))?
        .unbind();

    Ok(result)
}

/// Macro to create a Rust map function that can be called from Python
/// Usage: rust_map_function!(MyFunction, InputType, OutputType, |msg: Message<InputType>| -> OutputType { ... });
macro_rules! rust_function {
    ($name:ident, $input_type:ty, $output_type:ty, $transform_fn:expr) => {
        #[pyo3::pyclass]
        pub struct $name;

        #[pyo3::pymethods]
        impl $name {
            #[new]
            pub fn new() -> Self {
                Self
            }

            #[pyo3(name = "__call__")]
            pub fn call(
                &self,
                py: pyo3::Python<'_>,
                py_msg: pyo3::Py<pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                // If this cast fails, the user is not providing the right types
                let transform_fn: fn(
                    $crate::ffi::Message<$input_type>,
                ) -> $crate::ffi::Message<$output_type> = $transform_fn;

                // Convert Python message to typed Rust message
                let rust_msg = $crate::ffi::convert_py_message_to_rust::<$input_type>(py, &py_msg)?;

                // Release GIL and call Rust function
                let result_msg = py.allow_threads(|| transform_fn(rust_msg));

                // Convert result back to Python message
                $crate::ffi::convert_rust_message_to_py(py, result_msg)
            }

            pub fn input_type(&self) -> &'static str {
                std::any::type_name::<$input_type>()
            }

            pub fn output_type(&self) -> &'static str {
                std::any::type_name::<$output_type>()
            }

            pub fn rust_function_version(&self) -> usize {
                $crate::ffi::RUST_FUNCTION_VERSION
            }
        }
    };
}

pub use rust_function;

use crate::python_operator;
