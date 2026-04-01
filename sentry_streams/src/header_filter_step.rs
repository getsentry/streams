//! Filter messages by integer equality on a named header, without calling Python.
use crate::messages::PyStreamingMessage;
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::Duration;

/// Returns true if `value` encodes `expected` as big-endian i64 (8 bytes) or as UTF-8 decimal.
fn header_value_eq_i64(value: &[u8], expected: i64) -> bool {
    if value.len() == 8 {
        let arr: [u8; 8] = match value.try_into() {
            Ok(a) => a,
            Err(_) => return false,
        };
        if i64::from_be_bytes(arr) == expected {
            return true;
        }
    }
    if let Ok(s) = std::str::from_utf8(value) {
        if let Ok(parsed) = s.trim().parse::<i64>() {
            return parsed == expected;
        }
    }
    false
}

fn headers_match_int(headers: &[(String, Vec<u8>)], name: &str, expected: i64) -> bool {
    headers
        .iter()
        .any(|(k, v)| k == name && header_value_eq_i64(v, expected))
}

fn streaming_message_headers(msg: &PyStreamingMessage) -> Vec<(String, Vec<u8>)> {
    traced_with_gil!(|py| match msg {
        PyStreamingMessage::PyAnyMessage { content } => content.bind(py).borrow().headers.clone(),
        PyStreamingMessage::RawMessage { content } => content.bind(py).borrow().headers.clone(),
    })
}

pub struct HeaderIntEqualityFilter {
    header_name: String,
    expected: i64,
    next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    route: Route,
}

impl HeaderIntEqualityFilter {
    pub fn new(
        header_name: String,
        expected: i64,
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
    ) -> Self {
        Self {
            header_name,
            expected,
            next_step,
            route,
        }
    }
}

impl ProcessingStrategy<RoutedValue> for HeaderIntEqualityFilter {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            // Watermarks and any other non-streaming payload: pass through (same as build_map in transformer.rs).
            return self.next_step.submit(message);
        };

        let headers = streaming_message_headers(py_streaming_msg);
        if headers_match_int(&headers, &self.header_name, self.expected) {
            self.next_step.submit(message)
        } else {
            Ok(())
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}

pub fn build_header_int_filter(
    route: &Route,
    header_name: String,
    expected: i64,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    Box::new(HeaderIntEqualityFilter::new(
        header_name,
        expected,
        next,
        route.clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::Watermark;
    use crate::routes::Route;
    use crate::testutils::build_routed_value_with_headers;
    use crate::utils::traced_with_gil;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_header_int_filter_utf8() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted = Arc::new(Mutex::new(Vec::new()));
            let submitted_clone = submitted.clone();
            let next_step = FakeStrategy::new(submitted_clone, Arc::default(), false);
            let mut strategy = build_header_int_filter(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                "pid".to_string(),
                42,
                Box::new(next_step),
            );

            let msg_ok = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "x".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                    vec![("pid".to_string(), b"42".to_vec())],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg_ok).is_ok());

            let msg_drop = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "y".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                    vec![("pid".to_string(), b"41".to_vec())],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg_drop).is_ok());

            let other_route = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "z".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint2".to_string()],
                    vec![("pid".to_string(), b"99".to_vec())],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(other_route).is_ok());

            let expected = vec!["x".into_py_any(py).unwrap(), "z".into_py_any(py).unwrap()];
            assert_messages_match(py, expected, submitted.lock().unwrap().deref());
        });
    }

    #[test]
    fn test_header_int_filter_be_i64() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted = Arc::new(Mutex::new(Vec::new()));
            let submitted_clone = submitted.clone();
            let next_step = FakeStrategy::new(submitted, Arc::default(), false);
            let mut strategy = build_header_int_filter(
                &Route::new("s".to_string(), vec!["w".to_string()]),
                "k".to_string(),
                -1,
                Box::new(next_step),
            );

            let msg = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "p".into_py_any(py).unwrap(),
                    "s",
                    vec!["w".to_string()],
                    vec![("k".to_string(), (-1i64).to_be_bytes().to_vec())],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg).is_ok());

            let expected = vec!["p".into_py_any(py).unwrap()];
            assert_messages_match(py, expected, submitted_clone.lock().unwrap().deref());
        });
    }

    #[test]
    fn test_header_filter_watermark_forwarded() {
        crate::testutils::initialize_python();
        let submitted_wm = Arc::new(Mutex::new(Vec::new()));
        let submitted_wm_clone = submitted_wm.clone();
        let mut strategy = build_header_int_filter(
            &Route::new("source".to_string(), vec![]),
            "h".to_string(),
            1,
            Box::new(FakeStrategy::new(Arc::default(), submitted_wm_clone, false)),
        );

        let watermark_val = RoutedValue {
            route: Route::new(String::from("source"), vec![]),
            payload: RoutedValuePayload::make_watermark_payload(BTreeMap::new(), 0),
        };
        let watermark_msg = Message::new_any_message(watermark_val, BTreeMap::new());
        assert!(strategy.submit(watermark_msg).is_ok());
        let wm = submitted_wm.lock().unwrap();
        assert_eq!(wm[0], Watermark::new(BTreeMap::new(), 0));
    }

    #[test]
    fn test_header_filter_wrong_route_forwarded() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted = Arc::new(Mutex::new(Vec::new()));
            let submitted_clone = submitted.clone();
            let mut strategy = build_header_int_filter(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                "x".to_string(),
                1,
                Box::new(FakeStrategy::new(submitted, Arc::default(), false)),
            );

            let msg = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "payload".into_py_any(py).unwrap(),
                    "source1",
                    vec!["other".to_string()],
                    vec![],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg).is_ok());
            let expected = vec!["payload".into_py_any(py).unwrap()];
            assert_messages_match(py, expected, submitted_clone.lock().unwrap().deref());
        });
    }
}
