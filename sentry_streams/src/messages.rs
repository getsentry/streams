use pyo3::types::{PyBytes, PyList, PyTuple};
use pyo3::Python;

use pyo3::{prelude::*, types::PySequence, IntoPyObjectExt};

#[pyclass]
#[derive(Debug)]
pub struct PyAnyMessage {
    #[pyo3(get, set)]
    payload: Py<PyAny>,

    headers: Vec<(String, Vec<u8>)>,

    #[pyo3(get, set)]
    timestamp: f64,

    #[pyo3(get, set)]
    schema: Option<String>,
}

fn headers_to_vec(headers: Py<PySequence>) -> PyResult<Vec<(String, Vec<u8>)>> {
    Python::with_gil(|py| {
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
    })
}

fn headers_to_sequence(headers: &[(String, Vec<u8>)], py: Python<'_>) -> PyResult<Py<PySequence>> {
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

#[pymethods]
impl PyAnyMessage {
    #[new]
    pub fn new(
        payload: Py<PyAny>,
        headers: Py<PySequence>,
        timestamp: f64,
        schema: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            payload,
            headers: headers_to_vec(headers)?,
            timestamp,
            schema,
        })
    }

    #[getter]
    fn headers(&self, py: Python<'_>) -> PyResult<Py<PySequence>> {
        headers_to_sequence(&self.headers, py)
    }
}

#[pyclass]
#[derive(Debug)]
pub struct RawMessage {
    payload: Vec<u8>,

    headers: Vec<(String, Vec<u8>)>,

    #[pyo3(get, set)]
    timestamp: f64,

    #[pyo3(get, set)]
    schema: Option<String>,
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
            headers: headers_to_vec(headers)?,
            timestamp,
            schema,
        })
    }

    #[getter]
    fn headers(&self, py: Python) -> PyResult<Py<PySequence>> {
        headers_to_sequence(&self.headers, py)
    }

    #[getter]
    fn payload(&self, py: Python) -> PyResult<Py<PyBytes>> {
        Ok(PyBytes::new(py, &self.payload).unbind())
    }
}

#[derive(Debug)]
pub enum StreamingMessage {
    PyAnyMessage(PyAnyMessage),
    RawMessage(RawMessage),
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

            let headers_vec = headers_to_vec(py_seq.unbind()).unwrap();
            assert_eq!(headers_vec, headers);

            let py_seq2 = headers_to_sequence(&headers_vec, py).unwrap();
            let headers_vec2 = headers_to_vec(py_seq2.extract(py).unwrap()).unwrap();
            assert_eq!(headers_vec2, headers);
        });
    }
}
