#[cfg(test)]
use crate::messages::PyAnyMessage;
use crate::messages::{into_pyany, into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload};
use crate::routes::Route;
use crate::routes::RoutedValue;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
#[cfg(test)]
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
#[cfg(test)]
use sentry_arroyo::types::{Message, Partition, Topic};
#[cfg(test)]
use std::collections::BTreeMap;
use std::ffi::CStr;
#[cfg(test)]
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::time::Duration;

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
pub fn make_msg(
    payload: Option<Vec<u8>>,
    committable: BTreeMap<Partition, u64>,
) -> Message<KafkaPayload> {
    Message::new_any_message(KafkaPayload::new(None, None, payload), committable)
}

#[cfg(test)]
pub fn build_routed_value(
    py: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    build_routed_value_with_timestamp(py, msg_payload, source, waypoints, 0.0)
}

#[cfg(test)]
pub fn build_routed_value_with_timestamp(
    py: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
    timestamp: f64,
) -> RoutedValue {
    let route = Route::new(source.to_string(), waypoints);
    let payload = PyStreamingMessage::PyAnyMessage {
        content: into_pyany(
            py,
            PyAnyMessage {
                payload: msg_payload,
                headers: vec![],
                timestamp,
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

/// A Rust-owned message with headers. This is the representation the Kafka source emits,
/// so it is the default for tests: the suite exercises the path production takes.
/// Use [`build_py_routed_value_with_headers`] for the Python-memory counterpart.
#[cfg(test)]
pub fn build_routed_value_with_headers(
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
    headers: Vec<(String, Vec<u8>)>,
) -> RoutedValue {
    RoutedValue {
        route: Route::new(source.to_string(), waypoints),
        payload: RoutedValuePayload::RustRawMessage(RawMessage {
            payload: msg_payload.into(),
            headers,
            timestamp: 0.0,
            schema: None,
        }),
    }
}

/// The Python-memory counterpart of [`build_routed_value_with_headers`]: a `PyAnyMessage`
/// in a `PyStreamingMessage`, as a step downstream of a Python operator would see.
#[cfg(test)]
pub fn build_py_routed_value_with_headers(
    py: Python<'_>,
    msg_payload: Py<PyAny>,
    source: &str,
    waypoints: Vec<String>,
    headers: Vec<(String, Vec<u8>)>,
) -> RoutedValue {
    let route = Route::new(source.to_string(), waypoints);
    let payload = PyStreamingMessage::PyAnyMessage {
        content: into_pyany(
            py,
            PyAnyMessage {
                payload: msg_payload,
                headers,
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

/// A Rust-owned byte message, the representation the Kafka source emits. Default for tests;
/// use [`build_py_raw_routed_value`] for the Python-memory counterpart.
#[cfg(test)]
pub fn build_raw_routed_value(
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    build_raw_routed_value_with_timestamp(msg_payload, source, waypoints, 0.0)
}

#[cfg(test)]
pub fn build_raw_routed_value_with_timestamp(
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
    timestamp: f64,
) -> RoutedValue {
    RoutedValue {
        route: Route::new(source.to_string(), waypoints),
        payload: RoutedValuePayload::RustRawMessage(RawMessage {
            payload: msg_payload.into(),
            headers: vec![],
            timestamp,
            schema: None,
        }),
    }
}

/// The Python-memory counterpart of [`build_raw_routed_value`]: a `RawMessage` that has
/// already been moved into Python memory.
#[cfg(test)]
pub fn build_py_raw_routed_value(
    py: Python<'_>,
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> RoutedValue {
    let route = Route::new(source.to_string(), waypoints);
    let payload = PyStreamingMessage::RawMessage {
        content: into_pyraw(
            py,
            RawMessage {
                payload: msg_payload.into(),
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
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> Message<RoutedValue> {
    let routed_value = build_raw_routed_value(msg_payload, source, waypoints);
    Message::new_any_message(routed_value, std::collections::BTreeMap::new())
}

/// The Python-memory counterpart of [`make_raw_routed_msg`].
#[allow(unused)]
#[cfg(test)]
pub fn make_py_raw_routed_msg(
    py: Python<'_>,
    msg_payload: Vec<u8>,
    source: &str,
    waypoints: Vec<String>,
) -> Message<RoutedValue> {
    let routed_value = build_py_raw_routed_value(py, msg_payload, source, waypoints);
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

/// Names the representation of a payload, so a test can assert which memory a message lives
/// in. The ratchet rule is only observable this way: steps that read the payload to make a
/// decision must forward the Rust form, steps that transform it must write back the Python one.
#[cfg(test)]
pub fn payload_kind(payload: &RoutedValuePayload) -> &'static str {
    match payload {
        RoutedValuePayload::RustRawMessage(..) => "rust_raw",
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage { .. }) => "py_raw",
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::PyAnyMessage { .. }) => "py_any",
        RoutedValuePayload::WatermarkMessage(..) => "watermark",
    }
}

/// A next step that records the representation of everything submitted to it.
#[cfg(test)]
pub struct RecordingStrategy {
    kinds: Arc<Mutex<Vec<&'static str>>>,
}

#[cfg(test)]
impl RecordingStrategy {
    /// Returns the strategy and a handle on what it records.
    pub fn new() -> (Self, Arc<Mutex<Vec<&'static str>>>) {
        let kinds = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                kinds: kinds.clone(),
            },
            kinds,
        )
    }
}

#[cfg(test)]
impl ProcessingStrategy<RoutedValue> for RecordingStrategy {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        self.kinds
            .lock()
            .unwrap()
            .push(payload_kind(&message.into_payload().payload));
        Ok(())
    }

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
}

pub fn initialize_python() {
    let python_executable = std::env::var("STREAMS_TEST_PYTHONEXECUTABLE").unwrap();
    let python_path = std::env::var("STREAMS_TEST_PYTHONPATH").unwrap();
    let python_path: Vec<_> = python_path.split(':').map(String::from).collect();

    Python::attach(|py| -> PyResult<()> {
        PyModule::import(py, "sys")?.setattr("executable", python_executable)?;
        PyModule::import(py, "sys")?.setattr("path", python_path)?;
        Ok(())
    })
    .unwrap();
}
