use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

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

    /// Map the payload to a new type while preserving metadata
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Message<U> {
        Message {
            payload: f(self.payload),
            headers: self.headers,
            timestamp: self.timestamp,
            schema: self.schema,
        }
    }
}

/// Convert a Python payload into a given Rust type
///
/// You can implement this trait easiest by calling `convert_via_json!(MyType)`, provided your type
/// is JSON-serializable and deserializable on both sides.
pub trait FromPythonPayload: Sized {
    fn from_python_payload(value: pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self>;
}

/// Convert a Rust type back into a Python payload
///
/// You can implement this trait easiest by calling `convert_via_json!(MyType)`, provided your type
/// is JSON-serializable and deserializable with serde.
pub trait IntoPythonPayload {
    fn into_python_payload(self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>>;
}

/// Implement type conversion from/to Python by roundtripping with `serde_json` and `json.loads`.
///
/// You need `serde_json` and `pyo3` in your crate's dependencies.
#[macro_export]
macro_rules! convert_via_json {
    ($ty:ty) => {
        impl $crate::ffi::FromPythonPayload for $ty {
            fn from_python_payload(value: pyo3::Bound<'_, pyo3::PyAny>) -> ::pyo3::PyResult<Self> {
                use pyo3::prelude::*;

                let py = value.py();
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

        impl $crate::ffi::IntoPythonPayload for $ty {
            fn into_python_payload(
                self,
                py: ::pyo3::Python<'_>,
            ) -> ::pyo3::PyResult<::pyo3::Py<pyo3::PyAny>> {
                use pyo3::prelude::*;

                let payload_json = ::serde_json::to_string(&self).map_err(|e| {
                    ::pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to serialize JSON: {}",
                        e
                    ))
                })?;
                let payload_obj = py
                    .import("json")?
                    .getattr("loads")?
                    .call1((payload_json,))?;

                Ok(payload_obj.unbind())
            }
        }
    };
}

/// Convert a Python streaming message to a typed Rust Message format
/// This function handles the conversion for any type that implements serde::Deserialize
pub fn convert_py_message_to_rust<T>(
    py: pyo3::Python,
    py_msg: &pyo3::Py<pyo3::PyAny>,
) -> pyo3::PyResult<Message<T>>
where
    T: FromPythonPayload,
{
    let payload_obj = py_msg.bind(py).getattr("payload")?;
    let payload_value = T::from_python_payload(payload_obj);

    let headers_py: Vec<(String, Vec<u8>)> = py_msg.bind(py).getattr("headers")?.extract()?;
    let timestamp: f64 = py_msg.bind(py).getattr("timestamp")?.extract()?;
    let schema: Option<String> = py_msg.bind(py).getattr("schema")?.extract()?;

    Ok(Message {
        payload: payload_value?,
        headers: headers_py,
        timestamp,
        schema,
    })
}

/// Macro to create a Rust map function that can be called from Python
/// Usage: rust_map_function!(MyFunction, InputType, OutputType, |msg: Message<InputType>| -> OutputType { ... });
#[macro_export]
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
                let transform_fn: fn($crate::ffi::Message<$input_type>) -> $output_type =
                    $transform_fn;

                // Convert Python message to typed Rust message
                let rust_msg = $crate::ffi::convert_py_message_to_rust::<$input_type>(py, &py_msg)?;

                // Release GIL and call Rust function
                let result_msg = py.allow_threads(|| {
                    // clone metadata, but try very hard to avoid cloning the payload
                    let (payload, metadata) = rust_msg.take();
                    let metadata_clone = metadata.clone();
                    let result_payload = transform_fn(metadata.map(|()| payload));
                    metadata_clone.map(|()| result_payload)
                });

                // Return the raw payload directly (not wrapped in a message)
                let (payload, _) = result_msg.take();
                $crate::ffi::IntoPythonPayload::into_python_payload(payload, py)
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

// Built-in implementations for common types
impl IntoPythonPayload for bool {
    fn into_python_payload(self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(pyo3::types::PyBool::new(py, self).into_py_any(py)?)
    }
}

impl FromPythonPayload for bool {
    fn from_python_payload(value: pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        value.extract::<bool>()
    }
}

impl IntoPythonPayload for String {
    fn into_python_payload(self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(pyo3::types::PyString::new(py, &self).into_py_any(py)?)
    }
}

impl FromPythonPayload for String {
    fn from_python_payload(value: pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        value.extract::<String>()
    }
}

impl IntoPythonPayload for u64 {
    fn into_python_payload(self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        Ok(self.into_py_any(py)?)
    }
}

impl FromPythonPayload for u64 {
    fn from_python_payload(value: pyo3::Bound<'_, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        value.extract::<u64>()
    }
}
