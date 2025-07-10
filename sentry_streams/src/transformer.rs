use crate::callers::{try_apply_py, ApplyError};
use crate::filter_step::Filter;
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::{ProcessingStrategy, SubmitError};
use sentry_arroyo::types::{InnerMessage, Message};

/// Creates an Arroyo transformer strategy that uses a Python callable to
/// transform messages. The callable is expected to take a Message<RoutedValue>
/// as input and return a transformed message. The strategy is built on top of
/// the `RunTask` Arroyo strategy.
///
/// This function takes a `next`  step to wire the Arroyo strategy to.
pub fn build_map(
    route: &Route,
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();
    let mapper = move |message: Message<RoutedValue>| {
        if message.payload().route != copied_route {
            return Ok(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            return Ok(message);
        };

        let route = message.payload().route.clone();

        let res = traced_with_gil!(|py| {
            try_apply_py(py, &callable, (Into::<Py<PyAny>>::into(py_streaming_msg),))
        });

        match (res, &message.inner_message) {
            (Ok(transformed), _) => Ok(message.replace(RoutedValue {
                route,
                payload: RoutedValuePayload::PyStreamingMessage(transformed.into()),
            })),
            (Err(ApplyError::ApplyFailed), _) => panic!("Python map function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"),
            (Err(ApplyError::InvalidMessage), InnerMessage::AnyMessage(..)) => panic!("Got exception while processing AnyMessage, Arroyo cannot handle error on AnyMessage"),
            (Err(ApplyError::InvalidMessage),  InnerMessage::BrokerMessage(broker_message)) => Err(SubmitError::InvalidMessage(broker_message.into()))
        }
    };
    Box::new(RunTask::new(mapper, next))
}

/// Creates an Arroyo-based filter step strategy that uses a Python callable to
/// filter out messages. The callable is expected to take a Message<RoutedValue>
/// as input and return a bool. The strategy is a custom Processing Strategy,
/// defined in sentry_streams/src.
///
/// This function takes a `next` step to wire the Arroyo strategy to.
pub fn build_filter(
    route: &Route,
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();
    Box::new(Filter::new(callable, next, copied_route))
}

/// Creates a Rust-native map transformation strategy that calls a Rust function directly
/// without Python GIL overhead. The function pointer should point to a function with
/// signature: extern "C" fn(*const Message<T>) -> *const Message<U>
pub fn build_rust_map(
    route: &Route,
    function_ptr: usize,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();

    let mapper = move |message: Message<RoutedValue>| {
        if message.payload().route != copied_route {
            return Ok(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            return Ok(message);
        };

        let route = message.payload().route.clone();

        // Convert Python message to our FFI Message format
        let rust_msg = traced_with_gil!(|py| {
            let py_any: Py<PyAny> = py_streaming_msg.into();
            convert_py_message_to_rust(py, &py_any)
        });

        let input_ptr = crate::ffi::message_to_ptr(rust_msg);

        // Cast function pointer and call it
        let rust_fn: extern "C" fn(
            *const crate::ffi::Message<serde_json::Value>,
        ) -> *const crate::ffi::Message<serde_json::Value> =
            unsafe { std::mem::transmute(function_ptr) };

        let output_ptr = rust_fn(input_ptr);
        let output_msg = unsafe { crate::ffi::ptr_to_message(output_ptr) };

        // Convert result back to Python message
        let py_result = traced_with_gil!(|py| { convert_rust_message_to_py(py, output_msg) });

        Ok(message.replace(RoutedValue {
            route,
            payload: RoutedValuePayload::PyStreamingMessage(py_result.into()),
        }))
    };

    Box::new(RunTask::new(mapper, next))
}

/// Creates a Rust-native filter strategy that calls a Rust function directly
/// without Python GIL overhead. The function pointer should point to a function with
/// signature: extern "C" fn(*const Message<T>) -> bool
pub fn build_rust_filter(
    route: &Route,
    function_ptr: usize,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();

    // Create a custom Rust filter that follows the same pattern as the Python one
    Box::new(RustFilter::new(function_ptr, next, copied_route))
}

/// A custom filter strategy that uses Rust functions directly
pub struct RustFilter {
    pub function_ptr: usize,
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
}

impl RustFilter {
    pub fn new(
        function_ptr: usize,
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
    ) -> Self {
        Self {
            function_ptr,
            next_step,
            route,
        }
    }
}

impl ProcessingStrategy<RoutedValue> for RustFilter {
    fn poll(
        &mut self,
    ) -> Result<
        Option<sentry_arroyo::processing::strategies::CommitRequest>,
        sentry_arroyo::processing::strategies::StrategyError,
    > {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        // Handle messages for different routes and watermarks like the Python filter
        if self.route != message.payload().route || message.payload().payload.is_watermark_msg() {
            return self.next_step.submit(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            unreachable!("Watermark message trying to be passed to rust filter function.")
        };

        // Convert Python message to our FFI Message format
        let rust_msg = traced_with_gil!(|py| {
            let py_any: Py<PyAny> = py_streaming_msg.into();
            convert_py_message_to_rust(py, &py_any)
        });

        let input_ptr = crate::ffi::message_to_ptr(rust_msg);

        // Cast function pointer and call it
        let rust_fn: extern "C" fn(*const crate::ffi::Message<serde_json::Value>) -> bool =
            unsafe { std::mem::transmute(self.function_ptr) };

        let result = rust_fn(input_ptr);

        // Follow the same pattern as Python filter: submit if true, drop if false
        if result {
            self.next_step.submit(message)
        } else {
            Ok(()) // Drop the message
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> Result<
        Option<sentry_arroyo::processing::strategies::CommitRequest>,
        sentry_arroyo::processing::strategies::StrategyError,
    > {
        self.next_step.join(timeout)?;
        Ok(None)
    }
}

/// Convert a Python streaming message to our generic Rust Message format
fn convert_py_message_to_rust(
    py: Python,
    py_msg: &Py<PyAny>,
) -> crate::ffi::Message<serde_json::Value> {
    // Extract payload as serde_json::Value for safe deserialization
    let payload_obj = py_msg.bind(py).getattr("payload").unwrap();
    let payload_json = py
        .import("json")
        .unwrap()
        .getattr("dumps")
        .unwrap()
        .call1((payload_obj,))
        .unwrap()
        .extract::<String>()
        .unwrap();

    // Parse JSON into serde_json::Value for safe handling
    let payload_value: serde_json::Value = serde_json::from_str(&payload_json).unwrap();

    // Extract headers
    let headers_py: Vec<(String, Vec<u8>)> = py_msg
        .bind(py)
        .getattr("headers")
        .unwrap()
        .extract()
        .unwrap();

    // Extract timestamp
    let timestamp: f64 = py_msg
        .bind(py)
        .getattr("timestamp")
        .unwrap()
        .extract()
        .unwrap();

    // Extract schema
    let schema: Option<String> = py_msg
        .bind(py)
        .getattr("schema")
        .unwrap()
        .extract()
        .unwrap();

    crate::ffi::Message::new(payload_value, headers_py, timestamp, schema)
}

/// Convert our generic Rust Message format back to a Python streaming message
fn convert_rust_message_to_py(
    py: Python,
    rust_msg: crate::ffi::Message<serde_json::Value>,
) -> Py<PyAny> {
    // Convert serde_json::Value back to JSON string, then to Python object
    let payload_json = serde_json::to_string(&rust_msg.payload).unwrap();
    let payload_obj = py
        .import("json")
        .unwrap()
        .getattr("loads")
        .unwrap()
        .call1((payload_json,))
        .unwrap();

    // Create a new Python message with the transformed payload
    // Use PyAnyMessage directly since that's what PyStreamingMessage conversion expects
    let py_msg_class = py
        .import("sentry_streams.rust_streams")
        .unwrap()
        .getattr("PyAnyMessage")
        .unwrap();

    py_msg_class
        .call1((
            payload_obj,
            rust_msg.headers,
            rust_msg.timestamp,
            rust_msg.schema,
        ))
        .unwrap()
        .unbind()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::Watermark;
    use crate::routes::Route;
    use crate::testutils::build_routed_value;
    use crate::testutils::import_py_dep;
    use crate::testutils::make_lambda;
    use crate::utils::traced_with_gil;
    use chrono::Utc;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::noop::Noop;
    use sentry_arroyo::processing::strategies::InvalidMessage;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use sentry_arroyo::types::Partition;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::ffi::CStr;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    fn create_simple_transform_step<T>(
        lambda_body: &CStr,
        next_step: T,
    ) -> Box<dyn ProcessingStrategy<RoutedValue>>
    where
        T: ProcessingStrategy<RoutedValue> + 'static,
    {
        traced_with_gil!(|py| {
            py.run(lambda_body, None, None).expect("Unable to import");
            let callable = make_lambda(py, lambda_body);

            build_map(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                callable,
                Box::new(next_step),
            )
        })
    }

    #[test]
    #[should_panic(
        expected = "Got exception while processing AnyMessage, Arroyo cannot handle error on AnyMessage"
    )]
    fn test_transform_crashes_on_any_msg() {
        crate::testutils::initialize_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut transform = create_simple_transform_step(
            c_str!("lambda x: (_ for _ in ()).throw(InvalidMessageError())"),
            Noop {},
        );

        traced_with_gil!(|py| {
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let _ = transform.submit(message);
        });
    }

    #[test]
    #[should_panic(
        expected = "Python map function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"
    )]
    fn test_transform_crashes_on_normal_exceptions() {
        crate::testutils::initialize_python();

        let mut transform = create_simple_transform_step(c_str!("lambda x: {}[0]"), Noop {});

        traced_with_gil!(|py| {
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let _ = transform.submit(message);
        });
    }

    #[test]
    fn test_transform_handles_msg_invalid_exception() {
        crate::testutils::initialize_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut transform = create_simple_transform_step(
            c_str!("lambda x: (_ for _ in ()).throw(InvalidMessageError())"),
            Noop {},
        );

        traced_with_gil!(|py| {
            let message = Message::new_broker_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                Partition::new(Topic::new("topic"), 2),
                10,
                Utc::now(),
            );
            let SubmitError::InvalidMessage(InvalidMessage { partition, offset }) =
                transform.submit(message).unwrap_err()
            else {
                panic!("Expected SubmitError::InvalidMessage")
            };

            assert_eq!(partition, Partition::new(Topic::new("topic"), 2));
            assert_eq!(offset, 10);
        });
    }

    #[test]
    fn test_build_map() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let callable = make_lambda(
                py,
                c_str!("lambda x: x.replace_payload(x.payload + '_transformed')"),
            );
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

            let mut strategy = build_map(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                callable,
                Box::new(next_step),
            );

            // Expected message
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let result = strategy.submit(message);
            assert!(result.is_ok());

            // Separate route message. Not transformed
            let message2 = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint2".to_string()],
                ),
                BTreeMap::new(),
            );
            let result2 = strategy.submit(message2);
            assert!(result2.is_ok());

            let expected_messages = vec![
                "test_message_transformed".into_py_any(py).unwrap(),
                "test_message".into_py_any(py).unwrap(),
            ];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());

            let watermark_val = RoutedValue {
                route: Route::new(String::from("source"), vec![]),
                payload: RoutedValuePayload::make_watermark_payload(BTreeMap::new()),
            };
            let watermark_msg = Message::new_any_message(watermark_val, BTreeMap::new());
            let watermark_res = strategy.submit(watermark_msg);
            assert!(watermark_res.is_ok());
            let watermark_messages = submitted_watermarks_clone.lock().unwrap();
            assert_eq!(watermark_messages[0], Watermark::new(BTreeMap::new()));
        });
    }
}
