use crate::callers::{try_apply_py, ApplyError};
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message};
use std::time::Duration;

pub struct RouterStep {
    pub callable: Py<PyAny>,
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
}

impl RouterStep {
    /// A strategy that routes a message to a single route downstream.
    /// The route is picked by a Python function passed as PyAny. The python function
    /// is expected to return a string that represents the waypoint to add to the
    /// route.
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

impl ProcessingStrategy<RoutedValue> for RouterStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if message.payload().route != self.route {
            return self.next_step.submit(message);
        }

        let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) =
            message.payload().payload
        else {
            // TODO: a future PR will remove this gate on WatermarkMessage and duplicate it for each downstream route.
            return self.next_step.submit(message);
        };

        let res = traced_with_gil!(|py| {
            try_apply_py(
                py,
                &self.callable,
                (Into::<Py<PyAny>>::into(py_streaming_msg),),
            )
            .and_then(|py_res| {
                py_res
                    .extract::<String>(py)
                    .map_err(|_| ApplyError::ApplyFailed)
            })
        });

        match (res, &message.inner_message) {
            (Ok(new_waypoint), _) => {
                let new_message = message.try_map(|payload| {
                    Ok::<RoutedValue, SubmitError<RoutedValue>>(
                        payload.add_waypoint(new_waypoint.clone()),
                    )
                })?;
                self.next_step.submit(new_message)
            }
            // DLQ handled - skip the message and continue processing
            (Err(ApplyError::Skipped), _) => Ok(()),
            (Err(ApplyError::ApplyFailed), _) => panic!("Python route function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError or DlqHandledError"),
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

/// Creates an Arroyo strategy that routes a message to a single route downstream.
/// The route is picked by a Python function passed as PyAny. The python function
/// is expected to return a string that represent the waypoint to add to the
/// route.
pub fn build_router(
    route: &Route,
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    Box::new(RouterStep::new(callable, next, route.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutils::build_routed_value;
    use crate::testutils::import_py_dep;
    use crate::testutils::make_lambda;
    use crate::utils::traced_with_gil;
    use chrono::Utc;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::noop::Noop;
    use sentry_arroyo::processing::strategies::InvalidMessage;
    use sentry_arroyo::types::Partition;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::ffi::CStr;

    fn create_simple_router<T>(
        lambda_body: &CStr,
        next_step: T,
    ) -> Box<dyn ProcessingStrategy<RoutedValue>>
    where
        T: ProcessingStrategy<RoutedValue> + 'static,
    {
        traced_with_gil!(|py| {
            py.run(lambda_body, None, None).expect("Unable to import");
            let callable = make_lambda(py, lambda_body);

            build_router(
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
    fn test_router_crashes_on_any_msg() {
        crate::testutils::initialize_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut router = create_simple_router(
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
            let _ = router.submit(message);
        });
    }

    #[test]
    #[should_panic(
        expected = "Python route function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"
    )]
    fn test_router_crashes_on_normal_exceptions() {
        crate::testutils::initialize_python();

        let mut router = create_simple_router(c_str!("lambda x: {}[0]"), Noop {});

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
            let _ = router.submit(message);
        });
    }

    #[test]
    fn test_router_handles_invalid_msg_exception() {
        crate::testutils::initialize_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut router = create_simple_router(
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
                router.submit(message).unwrap_err()
            else {
                panic!("Expected SubmitError::InvalidMessage")
            };

            assert_eq!(partition, Partition::new(Topic::new("topic"), 2));
            assert_eq!(offset, 10);
        });
    }

    #[test]
    fn test_route_msg() {
        use crate::fake_strategy::assert_messages_match;
        use crate::fake_strategy::FakeStrategy;
        use std::sync::{Arc, Mutex};

        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let callable = make_lambda(py, c_str!("lambda x: 'waypoint2'"));
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

            let mut router = build_router(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                callable,
                Box::new(next_step),
            );

            // Message on matching route - should be routed with waypoint added
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let result = router.submit(message);
            assert!(result.is_ok());

            // Message on different route - should pass through unchanged
            let message2 = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message2".into_py_any(py).unwrap(),
                    "source3",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let result2 = router.submit(message2);
            assert!(result2.is_ok());

            // Check that both messages were submitted
            let messages = submitted_messages_clone.lock().unwrap();
            assert_eq!(messages.len(), 2);

            // Verify the payloads are correct (FakeStrategy stores payloads, not routes)
            let expected_messages = vec![
                "test_message".into_py_any(py).unwrap(),
                "test_message2".into_py_any(py).unwrap(),
            ];
            assert_messages_match(py, expected_messages, &messages);
        });
    }
}
