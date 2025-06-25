use crate::messages::{PyStreamingMessage, RoutedValuePayload};
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::{Py, PyAny, Python};
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, StrategyError, SubmitError,
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
            self.next_step.submit(message)
        } else {
            let res: Result<bool, pyo3::PyErr> = match message.payload().payload {
                RoutedValuePayload::PyStreamingMessage(ref py_payload) => {
                    traced_with_gil!(|py: Python<'_>| {
                        let python_payload: Py<PyAny> = match py_payload {
                            PyStreamingMessage::PyAnyMessage { ref content } => {
                                content.clone_ref(py).into_any()
                            }
                            PyStreamingMessage::RawMessage { ref content } => {
                                content.clone_ref(py).into_any()
                            }
                        };
                        let py_res = self.callable.call1(py, (python_payload,));
                        match py_res {
                            Ok(boolean) => {
                                let filtered = boolean.is_truthy(py).unwrap();
                                Ok(filtered)
                            }
                            Err(e) => Err(e),
                        }
                    })
                }
                RoutedValuePayload::WatermarkMessage(..) => {
                    unreachable!("Watermark message trying to be passed to filter function.")
                }
            };

            match res {
                Ok(bool) => {
                    if bool {
                        self.next_step.submit(message)?;
                    }
                    Ok(())
                }
                Err(_) => match message.inner_message {
                    InnerMessage::BrokerMessage(inner) => {
                        Err(SubmitError::<RoutedValue>::InvalidMessage(InvalidMessage {
                            partition: inner.partition,
                            offset: inner.offset,
                        }))
                    }
                    InnerMessage::AnyMessage(inner) => {
                        panic!("Unexpected message type: {:?}", inner)
                    }
                },
            }
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
    use crate::messages::WatermarkMessage;
    use crate::routes::Route;
    use crate::test_operators::build_routed_value;
    use crate::test_operators::make_lambda;
    use crate::transformer::build_filter;
    use crate::utils::traced_with_gil;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

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
                payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::new(
                    BTreeMap::new(),
                )),
            };
            let watermark_msg = Message::new_any_message(watermark_val, BTreeMap::new());
            let watermark_res = strategy.submit(watermark_msg);
            assert!(watermark_res.is_ok());
            let watermark_messages = submitted_watermarks_clone.lock().unwrap();
            assert_eq!(
                watermark_messages[0],
                WatermarkMessage::new(BTreeMap::new())
            );
        });
    }
}
