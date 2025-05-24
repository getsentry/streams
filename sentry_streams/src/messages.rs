use pyo3::types::{PyBytes, PyList, PyTuple};
use pyo3::Python;

use pyo3::{prelude::*, types::PySequence, IntoPyObjectExt};

pub fn headers_to_vec(py: Python<'_>, headers: Py<PySequence>) -> PyResult<Vec<(String, Vec<u8>)>> {
    Ok(headers
        .bind(py)
        .try_iter()
        .unwrap()
        .map(|item| {
            let tuple_i = item.unwrap();
            let tuple = tuple_i.downcast::<pyo3::types::PyTuple>().unwrap();
            let key = tuple.get_item(0).unwrap().unbind().extract(py).unwrap();
            let value: Vec<u8> = tuple.get_item(1).unwrap().unbind().extract(py).unwrap();
            (key, value)
        })
        .collect())
}

pub fn headers_to_sequence(
    py: Python<'_>,
    headers: &[(String, Vec<u8>)],
) -> PyResult<Py<PySequence>> {
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

    fn replace_payload(
        &self,
        new_payload: Py<PyAny>,
        py: Python<'_>,
    ) -> PyResult<Py<PyAnyMessage>> {
        let new_message = PyAnyMessage {
            payload: new_payload,
            headers: self.headers.clone(),
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        };
        into_pyany(py, new_message)
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

    fn replace_payload(
        &self,
        new_payload: Py<PyBytes>,
        py: Python<'_>,
    ) -> PyResult<Py<RawMessage>> {
        let new_message = RawMessage {
            payload: new_payload.as_bytes(py).to_vec(),
            headers: self.headers.clone(),
            timestamp: self.timestamp,
            schema: self.schema.clone(),
        };
        into_pyraw(py, new_message)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "RawMessage(payload={:?}, headers={:?}, timestamp={}, schema={:?})",
            self.payload, self.headers, self.timestamp, self.schema
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

pub fn replace_raw_payload(message: RawMessage, new_payload: Vec<u8>) -> RawMessage {
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

#[derive(Debug)]
#[pyclass]
pub enum StreamingMessage {
    PyAnyMessage { content: Py<PyAnyMessage> },
    RawMessage { content: Py<RawMessage> },
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
}
