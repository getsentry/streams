//! This module defines the Message primitives used to move data between
//! streaming operators.
//!
//! Messages have a a payload metadata. Metadata is the same for all types
//! of messages: headers, timestamp and optional schema.
//! The payload can be different between message types. Examples:
//! - PyAny is a Python object that is opaque to Rust code
//! - Bytes is a raw byte array.
//! - ...
//!
//! Not all operators can handle all message types. For example a Sink expects
//! a Message containing a byte array serialized by another step before.
//!
//! Message classes are pyClass if they have to be processed by Python code.
//! This means conversion is generally required when the message is created
//! in Rust and provided to Python code. Conversely, a message produced by
//! a Python operator cannot be extracted from the Gil without copying in order
//! to process the payload in Rust.
//!
//! Messages are immutable. They have to be consumed to replace the payload.
//! This cannot really be enforced in the Python code if the payload object
//! is itself mutable and python code decides to mutate it.
//!
//! There are a lot of TODOs at this stage:
//! TODO: Avoid re-declaring all the metadata fields in each message type.
//!       Create a Message class that contains the metadata and an Enum for
//!       different payload types.
//! TODO: Create tow variants of each of the message types that have to be
//!       usable by both Rust and Python. The Rust variant contains a native
//!       Rust payload. The Python variant contains a Py smart pointer to
//!       and object in Python memory.
//! TODO: Provide a standard translation layer between Rust and Python that
//!       is transparent to the operator that has to process a message. This
//!       will allow us to optimize the translation avoiding copy without
//!       impacting each operator.
use pyo3::types::{PyBytes, PyList, PyTuple};
use pyo3::Python;

use pyo3::{prelude::*, types::PySequence, IntoPyObjectExt};

pub fn headers_to_vec(py: Python<'_>, headers: Py<PySequence>) -> PyResult<Vec<(String, Vec<u8>)>> {
    // Converts the Python consumable representation of the Message headers into
    // the Rust native representation (which is a Vec<(String, Vec<u8>)>).
    // This copies data. The original python representation is still usable
    // as it is a Py smart pointer.
    headers
        .bind(py)
        .try_iter()?
        .map(|item| -> PyResult<(String, Vec<u8>)> {
            let tuple_i = item?;
            let tuple = tuple_i.downcast::<pyo3::types::PyTuple>()?;
            let key = tuple.get_item(0)?.unbind().extract(py)?;
            let value: Vec<u8> = tuple.get_item(1)?.unbind().extract(py)?;
            Ok((key, value))
        })
        .collect()
}

pub fn headers_to_sequence(
    py: Python<'_>,
    headers: &[(String, Vec<u8>)],
) -> PyResult<Py<PySequence>> {
    // Transforms the Rust native representation of the Message headers into
    // the Python consumable representation in Python memory.
    // This copies data.
    let py_tuples = headers
        .iter()
        .map(|(k, v)| {
            let py_key = k.into_py_any(py).unwrap();
            let py_value = v.into_py_any(py).unwrap();
            PyTuple::new(py, &[py_key, py_value]).unwrap()
        })
        .collect::<Vec<_>>();
    let list = PyList::new(py, py_tuples).unwrap();
    let seq = list.into_sequence();
    Ok(seq.unbind())
}

/// Represents a message with any Python object as payload. This message type
/// is meant to be processed by Python operators. Rust operators should consider
/// The payload as opaque and should not transform it.
///
/// The message can be created both by Python code or Rust code. When Rust code
/// instantiates it, it has to convert it into a Py<PyAnyMessage> before handing
/// it to Python code. This can be done with the `into_pyany` function.
#[pyclass]
#[derive(Debug)]
pub struct PyAnyMessage {
    #[pyo3(get)]
    pub payload: Py<PyAny>,

    pub headers: Vec<(String, Vec<u8>)>,

    #[pyo3(get)]
    pub timestamp: f64,

    #[pyo3(get)]
    pub schema: Option<String>,
}

pub fn into_pyany(py: Python<'_>, message: PyAnyMessage) -> PyResult<Py<PyAnyMessage>> {
    // Converts a PyAnyMessage instantiated outside of Python memory into a
    // Py<PyAnyMessage> that can be used in Python code.
    let py_obj = Py::new(py, message)?;
    Ok(py_obj)
}

#[pymethods]
impl PyAnyMessage {
    #[new]
    pub fn new(
        payload: Py<PyAny>,
        headers: Py<PySequence>,
        timestamp: f64,
        schema: Option<String>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        Ok(Self {
            payload,
            headers: headers_to_vec(py, headers)?,
            timestamp,
            schema,
        })
    }

    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Py<PySequence>> {
        headers_to_sequence(py, &self.headers)
    }

    fn replace_payload(&self, new_payload: Py<PyAny>) -> PyAnyMessage {
        PyAnyMessage {
            payload: new_payload,
            headers: self.headers.clone(),
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let payload_repr = self.payload.call_method0(py, "__repr__")?;
        Ok(format!(
            "PyAnyMessage(payload={}, headers={:?}, timestamp={}, schema={:?})",
            payload_repr, self.headers, self.timestamp, self.schema
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

/// Represent a message whose payload is a byte array. The payload is a Vec<u8>, not
/// a PyBytes. Copy is needed to convert one to the other. This is meant primarily to
/// represent the message produced by a Rust source and consumed by a Rust Sink.
///
/// TODO: With FFI there should be a way to share a byte array between Rust and Python
///       without copying.
#[pyclass]
#[derive(Debug)]
pub struct RawMessage {
    pub payload: Vec<u8>,

    pub headers: Vec<(String, Vec<u8>)>,

    #[pyo3(get, set)]
    pub timestamp: f64,

    #[pyo3(get, set)]
    pub schema: Option<String>,
}

#[pymethods]
impl RawMessage {
    #[new]
    pub fn new(
        payload: Py<PyBytes>,
        headers: Py<PySequence>,
        timestamp: f64,
        schema: Option<String>,
        py: Python,
    ) -> PyResult<Self> {
        Ok(Self {
            payload: payload.as_bytes(py).to_vec(),
            headers: headers_to_vec(py, headers)?,
            timestamp,
            schema,
        })
    }

    #[getter]
    fn headers(&self, py: Python) -> PyResult<Py<PySequence>> {
        headers_to_sequence(py, &self.headers)
    }

    #[getter]
    fn payload(&self, py: Python) -> PyResult<Py<PyBytes>> {
        Ok(PyBytes::new(py, &self.payload).unbind())
    }

    fn replace_payload(&self, new_payload: Py<PyBytes>, py: Python<'_>) -> RawMessage {
        RawMessage {
            payload: new_payload.as_bytes(py).to_vec(),
            headers: self.headers.clone(),
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        }
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "RawMessage(payload={:?}, headers={:?}, timestamp={}, schema={:?})",
            self.payload, self.headers, self.timestamp, self.schema
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        self.__repr__()
    }
}

#[allow(unused)]
pub fn replace_raw_payload(message: RawMessage, new_payload: Vec<u8>) -> RawMessage {
    // Replaces the payload of a `RawMessage` with a new byte array when the
    // message is managed by Rust and is not on Python memory.
    RawMessage {
        payload: new_payload,
        headers: message.headers,
        timestamp: message.timestamp,
        schema: message.schema,
    }
}

pub fn into_pyraw(py: Python<'_>, message: RawMessage) -> PyResult<Py<RawMessage>> {
    let py_obj = Py::new(py, message)?;
    Ok(py_obj)
}

/// Represents a generic message that is in Python memory and can be sent back and
/// forth to Python code. This is means to be passed between between operators in
/// the Rust code.
///
/// TODO: See the TODO at the module level. This is where we would put the message
///       metadata.
#[derive(Debug)]
#[pyclass]
pub enum PyStreamingMessage {
    PyAnyMessage { content: Py<PyAnyMessage> },
    RawMessage { content: Py<RawMessage> },
}

impl Into<PyStreamingMessage> for Py<PyAny> {
    fn into(self) -> PyStreamingMessage {
        Python::with_gil(|py| {
            let bound = self.clone_ref(py).into_bound(py);
            if bound.is_instance_of::<PyAnyMessage>() {
                let content = bound.downcast::<PyAnyMessage>()?;
                Ok(PyStreamingMessage::PyAnyMessage {
                    content: content.clone().unbind(),
                })
            } else if bound.is_instance_of::<RawMessage>() {
                let content = bound.downcast::<RawMessage>()?;
                Ok(PyStreamingMessage::RawMessage {
                    content: content.clone().unbind(),
                })
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "Message type is invalid: expected PyAnyMessage or RawMessage, got {}",
                    bound.get_type().name().unwrap()
                )))
            }
        })
        .unwrap()
    }
}

/// Represents a generic message that is in Rust memory and can be processed by Rust
/// code without taking the Gil.
///
/// TODO: See the TODO at the module level. This is where we would put the message
///       metadata.
#[allow(unused)]
#[derive(Debug)]
pub enum StreamingMessage {
    PyAnyMessage { content: PyAnyMessage },
    RawMessage { content: RawMessage },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_headers_to_vec_and_sequence_roundtrip() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let headers = vec![
                ("key1".to_string(), vec![1, 2, 3]),
                ("key2".to_string(), vec![4, 5, 6]),
            ];
            let py_tuples: Vec<_> = headers
                .iter()
                .map(|(k, v)| {
                    PyTuple::new(
                        py,
                        &[
                            k.into_py_any(py).unwrap(),
                            PyBytes::new(py, v).into_py_any(py).unwrap(),
                        ],
                    )
                    .unwrap()
                })
                .collect();
            let py_list = PyList::new(py, py_tuples);
            let py_seq = py_list.unwrap().into_sequence();

            let headers_vec = headers_to_vec(py, py_seq.unbind()).unwrap();
            assert_eq!(headers_vec, headers);

            let py_seq2 = headers_to_sequence(py, &headers_vec).unwrap();
            let headers_vec2 = headers_to_vec(py, py_seq2.extract(py).unwrap()).unwrap();
            assert_eq!(headers_vec2, headers);
        });
    }

    #[test]
    fn test_pyanymessage_lifecycle() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Prepare test data
            let payload = "payload".into_py_any(py).unwrap();
            let headers = vec![
                ("foo".to_string(), vec![10, 20]),
                ("bar".to_string(), vec![30, 40]),
            ];
            let py_headers = headers_to_sequence(py, &headers).unwrap();

            let timestamp = 42.5;
            let schema = Some("myschema".to_string());

            // Create PyAnyMessage
            let msg = PyAnyMessage::new(
                payload.clone_ref(py),
                py_headers.clone_ref(py),
                timestamp,
                schema.clone(),
                py,
            )
            .unwrap();

            // Extract and assert attributes
            assert_eq!(msg.timestamp, timestamp);
            assert_eq!(msg.schema, schema);

            // Check payload
            let payload_val: String = msg.payload.bind(py).extract().unwrap();
            assert_eq!(payload_val, "payload");

            // Check headers
            assert_eq!(msg.headers, headers);

            let new_msg = msg.replace_payload("new_payload".into_py_any(py).unwrap());

            // Ensure new_msg is not the same struct as msg by comparing their payloads
            let old_payload: String = msg.payload.bind(py).extract().unwrap();
            let new_payload: String = new_msg.payload.bind(py).extract().unwrap();
            assert_ne!(old_payload, new_payload);

            // test the python methods
            let pymsg = into_pyany(py, msg).unwrap();

            let repr = pymsg.call_method0(py, "__repr__").unwrap();
            let expected_repr = format!(
                "PyAnyMessage(payload='{}', headers={:?}, timestamp={}, schema={:?})",
                payload_val, headers, timestamp, schema
            );
            assert_eq!(repr.extract::<String>(py).unwrap(), expected_repr);
        });
    }

    #[test]
    fn test_rawmessage_lifecycle() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Prepare test data
            let payload_bytes = vec![100, 101, 102, 103];
            let py_payload = PyBytes::new(py, &payload_bytes);
            let headers = vec![
                ("alpha".to_string(), vec![1, 2]),
                ("beta".to_string(), vec![3, 4]),
            ];
            let py_headers = headers_to_sequence(py, &headers).unwrap();

            let timestamp = 123.45;
            let schema = Some("rawschema".to_string());

            // Create RawMessage
            let msg = RawMessage::new(
                py_payload.unbind(),
                py_headers.clone_ref(py),
                timestamp,
                schema.clone(),
                py,
            )
            .unwrap();

            // Extract and assert attributes
            assert_eq!(msg.timestamp, timestamp);
            assert_eq!(msg.schema, schema);

            // Check payload
            assert_eq!(msg.payload, payload_bytes);

            // Check headers
            assert_eq!(msg.headers, headers);

            // Test payload getter
            let py_payload_val = msg.payload(py).unwrap();
            let payload_val: &[u8] = py_payload_val.bind(py).as_bytes();
            assert_eq!(payload_val, &payload_bytes[..]);

            // Test headers getter
            let py_headers_val = msg.headers(py).unwrap();
            let headers_val = headers_to_vec(py, py_headers_val).unwrap();
            assert_eq!(headers_val, headers);

            // Replace payload via python
            let new_payload_bytes = vec![200, 201, 202];
            let new_py_payload = PyBytes::new(py, &new_payload_bytes);
            let new_msg = msg.replace_payload(new_py_payload.unbind(), py);

            let new_payload_val: Py<PyBytes> = new_msg.payload(py).unwrap();
            let new_payload_bytes2: &[u8] = new_payload_val.as_bytes(py);
            assert_eq!(new_payload_bytes2, new_payload_bytes);

            // test the python methods
            let pymsg = into_pyraw(py, msg).unwrap();

            let repr = pymsg.call_method0(py, "__repr__").unwrap();
            let expected_repr = format!(
                "RawMessage(payload={:?}, headers={:?}, timestamp={}, schema={:?})",
                payload_bytes, headers, timestamp, schema
            );
            assert_eq!(repr.extract::<String>(py).unwrap(), expected_repr);
        });
    }
}
