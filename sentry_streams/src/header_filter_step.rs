//! Filter messages by integer equality on a named header, without calling Python.
use crate::messages::PyStreamingMessage;
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message};
use std::time::Duration;

/// `Ok(true)` if any header named `name` has a non-empty value that parses to `expected`.
/// `Ok(false)` if there is no match (header missing, empty value, or different integer).
/// `Err(())` if any value for `name` is non-empty but not a valid decimal integer (invalid message).
///
/// Header values are treated as UTF-8 (ASCII decimal from Relay): optional leading `+` / `-`, then digits.
fn header_int_equality_decision(
    headers: &[(String, Vec<u8>)],
    name: &str,
    expected: i64,
) -> Result<bool, ()> {
    let mut matched = false;
    for (k, v) in headers {
        if k != name {
            continue;
        }
        if v.is_empty() {
            continue;
        }
        let parsed = std::str::from_utf8(v)
            .map_err(|_| ())?
            .parse::<i64>()
            .map_err(|_| ())?;
        if parsed == expected {
            matched = true;
        }
    }
    Ok(matched)
}

fn invalid_message_submit_error(message: &Message<RoutedValue>) -> SubmitError<RoutedValue> {
    match &message.inner_message {
        InnerMessage::BrokerMessage(broker_message) => {
            SubmitError::InvalidMessage(broker_message.into())
        }
        InnerMessage::AnyMessage(..) => panic!(
            "HeaderIntEqualityFilter: invalid header on AnyMessage; Arroyo cannot surface InvalidMessage"
        ),
    }
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
        match header_int_equality_decision(&headers, &self.header_name, self.expected) {
            Ok(true) => self.next_step.submit(message),
            Ok(false) => Ok(()),
            Err(()) => Err(invalid_message_submit_error(&message)),
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
    use pyo3::types::PyAnyMethods;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn header_int_equality_decision_unit_cases() {
        crate::testutils::initialize_python();

        const NAME: &str = "pid";
        const EXPECTED: i64 = 42;

        assert_eq!(
            header_int_equality_decision(&[], NAME, EXPECTED),
            Ok(false),
            "header not present: no headers"
        );
        assert_eq!(
            header_int_equality_decision(&[("org".to_string(), b"1".to_vec())], NAME, EXPECTED,),
            Ok(false),
            "header not present: only other keys"
        );
        assert_eq!(
            header_int_equality_decision(&[(NAME.to_string(), vec![])], NAME, EXPECTED),
            Ok(false),
            "header present but empty"
        );
        assert_eq!(
            header_int_equality_decision(&[(NAME.to_string(), b"41".to_vec())], NAME, EXPECTED),
            Ok(false),
            "header has different integer"
        );
        assert_eq!(
            header_int_equality_decision(&[(NAME.to_string(), b"42".to_vec())], NAME, EXPECTED),
            Ok(true),
            "header has expected value"
        );

        traced_with_gil!(|py| {
            use pyo3::ffi::c_str;
            let header_bytes: Vec<u8> = py
                .eval(c_str!("str(42).encode('ascii')"), None, None)
                .expect("str(42).encode('ascii')")
                .extract()
                .expect("extract header bytes");
            assert_eq!(
                header_int_equality_decision(&[(NAME.to_string(), header_bytes)], NAME, EXPECTED),
                Ok(true),
                "header value from Python str().encode('ascii')"
            );
        });

        assert_eq!(
            header_int_equality_decision(&[(NAME.to_string(), vec![0xFF])], NAME, EXPECTED),
            Err(()),
            "non-UTF-8 header value"
        );
        assert_eq!(
            header_int_equality_decision(
                &[(NAME.to_string(), b"not-int".to_vec())],
                NAME,
                EXPECTED
            ),
            Err(()),
            "UTF-8 but not a decimal integer"
        );
    }

    #[test]
    fn test_header_int_filter_ascii_decimal() {
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
    fn test_header_int_filter_empty_header_dropped() {
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

            let msg = Message::new_any_message(
                build_routed_value_with_headers(
                    py,
                    "y".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                    vec![("pid".to_string(), vec![])],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg).is_ok());
            assert!(submitted.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn test_header_int_filter_invalid_header_broker_returns_invalid_message() {
        crate::testutils::initialize_python();
        use chrono::Utc;
        use sentry_arroyo::processing::strategies::InvalidMessage;
        use sentry_arroyo::types::Partition;
        use sentry_arroyo::types::Topic;

        traced_with_gil!(|py| {
            let mut strategy = build_header_int_filter(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                "pid".to_string(),
                42,
                Box::new(FakeStrategy::new(Arc::default(), Arc::default(), false)),
            );

            let message = Message::new_broker_message(
                build_routed_value_with_headers(
                    py,
                    "x".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                    vec![("pid".to_string(), b"not-an-int".to_vec())],
                ),
                Partition::new(Topic::new("topic"), 2),
                10,
                Utc::now(),
            );
            let SubmitError::InvalidMessage(InvalidMessage { partition, offset }) =
                strategy.submit(message).unwrap_err()
            else {
                panic!("Expected SubmitError::InvalidMessage")
            };
            assert_eq!(partition, Partition::new(Topic::new("topic"), 2));
            assert_eq!(offset, 10);
        });
    }

    #[test]
    fn test_header_int_filter_negative_ascii_decimal() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted = Arc::new(Mutex::new(Vec::new()));
            let submitted_clone = submitted.clone();
            let next_step = FakeStrategy::new(submitted_clone, Arc::default(), false);
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
                    vec![("k".to_string(), b"-1".to_vec())],
                ),
                BTreeMap::new(),
            );
            assert!(strategy.submit(msg).is_ok());

            let expected = vec!["p".into_py_any(py).unwrap()];
            assert_messages_match(py, expected, submitted.lock().unwrap().deref());
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
