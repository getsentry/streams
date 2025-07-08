use crate::messages::PyAnyMessage;
use crate::messages::{into_pyany, into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload};
use crate::routes::Route;
use crate::routes::RoutedValue;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
#[cfg(test)]
use sentry_arroyo::types::{Message, Partition, Topic};
#[cfg(test)]
use std::collections::BTreeMap;
use std::ffi::CStr;

#[cfg(test)]
pub fn import_py_dep(module: &str, attr: &str) {
    use std::ffi::CString;

    use crate::utils::traced_with_gil;

    let stmt = format!("from {} import {}", module, attr);
    traced_with_gil!(|py| {
        py.run(
            &CString::new(stmt).expect("Unable to convert import statement into Cstr"),
            None,
            None,
        )
        .expect("Unable to import");
    });
}

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
    py: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    let route = Route::new(source.to_string(), waypoints);
    let payload = PyStreamingMessage::PyAnyMessage {
        content: into_pyany(
            py,
            PyAnyMessage {
                payload: msg_payload,
                headers: vec![],
                timestamp: 0.0,
                schema: None,
            },
        )
        .unwrap(),
    };
    RoutedValue {
        route,
        payload: RoutedValuePayload::PyStreamingMessage(payload),
    }
}

#[cfg(test)]
pub fn build_raw_routed_value(
    py: Python<'_>,
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    use std::vec;

    let route = Route::new(source.to_string(), waypoints);
    let payload = PyStreamingMessage::RawMessage {
        content: into_pyraw(
            py,
            RawMessage {
                payload: msg_payload,
                headers: vec![],
                timestamp: 0.0,
                schema: None,
            },
        )
        .unwrap(),
    };
    RoutedValue {
        route,
        payload: RoutedValuePayload::PyStreamingMessage(payload),
    }
}

#[allow(unused)]
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

#[cfg(test)]
pub fn make_raw_routed_msg(
    py: Python<'_>,
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> Message<RoutedValue> {
    let routed_value = build_raw_routed_value(py, msg_payload, source, waypoints);
    Message::new_any_message(routed_value, std::collections::BTreeMap::new())
}

/// Returns a BTreeMap of {Partition: Offset}. Topic name and offset starts at `starting_offset`,
/// while `num_partitions` is the total number of entries in the BTreeMap.
#[cfg(test)]
pub fn make_committable(num_partitions: u64, starting_offset: u64) -> BTreeMap<Partition, u64> {
    let mut committable = BTreeMap::new();
    for i in 0..num_partitions {
        let val = i + starting_offset;
        committable.insert(
            Partition::new(Topic::new(format!("t{val}").as_str()), val as u16),
            val,
        );
    }
    committable
}

pub fn initialize_python() {
    let python_executable = std::env::var("STREAMS_TEST_PYTHONEXECUTABLE").unwrap();
    let python_path = std::env::var("STREAMS_TEST_PYTHONPATH").unwrap();
    let python_path: Vec<_> = python_path.split(':').map(String::from).collect();

    Python::with_gil(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", python_executable)?;
        PyModule::import(py, "sys")?.setattr("path", python_path)?;
        Ok(())
    })
    .unwrap();
}
