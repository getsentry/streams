use crate::callers::{try_apply_py, ApplyError};
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::{Py, PyAny};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message};
use std::time::Duration;

pub struct Filter {
    pub callable: Py<PyAny>,
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
}

impl Filter {
    /// A strategy that takes a callable, and applies it to messages
    /// to either filter the message out or submit it to the next step.
    /// The callable is expected to take a Message<RoutedValue>
    /// as input and return a bool.
    /// The strategy also handles messages arriving on different routes;
    /// it simply forwards them as-is to the next step.
    pub fn new(
        callable: Py<PyAny>,
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
    ) -> Self {
        Self {
            callable,
            next_step,
            route,
        }
    }
}

impl ProcessingStrategy<RoutedValue> for Filter {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        // WatermarkMessages are submitted to next_step immediately so they aren't passed to the filter function
        if self.route != message.payload().route || message.payload().payload.is_watermark_msg() {
            return self.next_step.submit(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            unreachable!("Watermark message trying to be passed to filter function.")
        };

        let res = traced_with_gil!(|py| {
            try_apply_py(
                py,
                &self.callable,
                (Into::<Py<PyAny>>::into(py_streaming_msg),),
            )
            .and_then(|py_res| py_res.is_truthy(py).map_err(|_| ApplyError::ApplyFailed))
        });

        match (res, &message.inner_message) {
            (Ok(true), _) => self.next_step.submit(message),
            (Ok(false), _) => Ok(()),
            (Err(ApplyError::ApplyFailed), _) => panic!("Python filter function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"),
            (Err(ApplyError::InvalidMessage), InnerMessage::AnyMessage(..)) => panic!("Got exception while processing AnyMessage, Arroyo cannot handle error on AnyMessage"),
            (Err(ApplyError::InvalidMessage), InnerMessage::BrokerMessage(broker_message)) => Err(SubmitError::InvalidMessage(broker_message.into())),
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)?;
        Ok(None)
    }
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
    use crate::transformer::build_filter;
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

    fn create_simple_filter<T>(
        lambda_body: &CStr,
        next_step: T,
    ) -> Box<dyn ProcessingStrategy<RoutedValue>>
    where
        T: ProcessingStrategy<RoutedValue> + 'static,
    {
        traced_with_gil!(|py| {
            py.run(lambda_body, None, None).expect("Unable to import");
            let callable = make_lambda(py, lambda_body);

            build_filter(
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
    fn test_filter_crashes_on_any_msg() {
        pyo3::prepare_freethreaded_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut filter = create_simple_filter(
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
            let _ = filter.submit(message);
        });
    }

    #[test]
    #[should_panic(
        expected = "Python filter function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"
    )]
    fn test_filter_crashes_on_normal_exceptions() {
        pyo3::prepare_freethreaded_python();

        let mut filter = create_simple_filter(c_str!("lambda x: {}[0]"), Noop {});

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
            let _ = filter.submit(message);
        });
    }

    #[test]
    fn test_filter_handles_invalid_msg_exception() {
        pyo3::prepare_freethreaded_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut filter = create_simple_filter(
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
                filter.submit(message).unwrap_err()
            else {
                panic!("Expected SubmitError::InvalidMessage")
            };

            assert_eq!(partition, Partition::new(Topic::new("topic"), 2));
            assert_eq!(offset, 10);
        });
    }

    #[test]
    fn test_build_filter() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let callable = make_lambda(py, c_str!("lambda x: 'test' in x.payload"));
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

            let mut strategy = build_filter(
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

            // Message to filter out
            let message3 = Message::new_any_message(
                build_routed_value(
                    py,
                    "message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let result3 = strategy.submit(message3);
            assert!(result3.is_ok());

            let expected_messages = vec![
                "test_message".into_py_any(py).unwrap(),
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
