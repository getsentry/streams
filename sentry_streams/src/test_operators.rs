use crate::routes::Route;
use crate::routes::RoutedValue;
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

#[cfg(test)]
pub fn build_routed_value(
    _: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    let route = Route::new(source.to_string(), waypoints);
    RoutedValue {
        route,
        payload: msg_payload,
    }
}

#[cfg(test)]
pub fn make_routed_msg(
    py: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
) -> Message<RoutedValue> {
    let routed_value = build_routed_value(py, msg_payload, source, waypoints);
    Message::new_any_message(routed_value, std::collections::BTreeMap::new())
}
