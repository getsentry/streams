use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::types::Message;
use std::ffi::CStr;

#[cfg(test)]
pub fn make_lambda(py: Python<'_>, py_code: &CStr) -> Py<PyAny> {
    py.eval(py_code, None, None)
        .unwrap()
        .into_py_any(py)
        .unwrap()
}

#[cfg(test)]
pub fn make_msg(payload: Option<Vec<u8>>) -> Message<KafkaPayload> {
    Message::new_any_message(
        KafkaPayload::new(None, None, payload),
        std::collections::BTreeMap::new(),
    )
}
