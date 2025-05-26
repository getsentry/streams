//! This module contains the implementation of the PythonAdapter Arroyo
//! processing strategy that delegates the processing of messages to the
//! python operator.

use crate::messages::PyStreamingMessage;
use crate::routes::{Route, RoutedValue};
use pyo3::types::{PyDict, PyTuple};
use pyo3::Python;
use pyo3::{prelude::*, IntoPyObjectExt};
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, StrategyError,
};
use sentry_arroyo::types::{Message, Partition, Topic};
use sentry_arroyo::utils::timing::Deadline;
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

/// PythonAdapter is an Arroyo processing strategy that delegates the
/// processing of messages to a Python class that extends the
/// `RustOperatorDelegate` class.
///
/// The python delegate is passed as a `Py<PyAny>` reference.
///
/// Overall This struct has a ProcessingStrategy implementation so it
/// can be wired up to other Arroyo strategies. When it receives a
/// message iot forwards them to the `submit` method of the python
/// delegate. The responses of the `poll` method on the python delegate
/// are then forwarded to the next strategy.
pub struct PythonAdapter {
    pub route: Route,
    pub processing_step: Py<PyAny>,
    transformed_messages: VecDeque<Message<RoutedValue>>,
    // TODO: Add a mutex here
    next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl PythonAdapter {
    pub fn new(
        route: Route,
        delegate_factory: Py<PyAny>,
        next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    ) -> Self {
        Python::with_gil(|py| {
            let processing_step = delegate_factory.call_method0(py, "build").unwrap();

            Self {
                route,
                processing_step,
                next_strategy,
                transformed_messages: VecDeque::new(),
                commit_request_carried_over: None,
            }
        })
    }

    /// Turn a Vector of python payloads provided by the Python delegate
    /// into Message::AnyMessage to be used in Arroyo.
    /// The committable of the forwarded Message are those provided by
    /// the Python delegate.
    fn handle_py_return_value(&mut self, py: Python<'_>, payloads: Vec<Py<PyAny>>) {
        for py_payload in payloads {
            let entry = py_payload.downcast_bound::<PyTuple>(py).unwrap();
            let payload: Py<PyAny> = entry.get_item(0).unwrap().unbind();
            let committable: Py<PyAny> = entry.get_item(1).unwrap().unbind();
            let message = Message::new_any_message(
                RoutedValue {
                    route: self.route.clone(),
                    payload: payload.into(),
                },
                convert_py_committable(py, committable).unwrap(),
            );

            self.transformed_messages.push_back(message);
        }
    }
}

/// Transform a Python `Partition` object into a Rust Arroyo
/// partition object.
fn convert_partition(partition: Bound<'_, PyAny>) -> Result<Partition, PyErr> {
    let partition_index: u16 = partition.getattr("index")?.extract()?;
    let topic = partition.getattr("topic")?;
    let topic_name: String = topic.getattr("name")?.extract()?;
    Ok(Partition {
        topic: Topic::new(&topic_name),
        index: partition_index,
    })
}

fn convert_committable_to_py(
    py: Python<'_>,
    committable: BTreeMap<Partition, u64>,
) -> Result<Py<PyAny>, PyErr> {
    let dict = PyDict::new(py);
    for (partition, offset) in committable {
        let key = PyTuple::new(
            py,
            &[
                partition.topic.as_str().into_py_any(py)?,
                partition.index.into_py_any(py)?,
            ],
        );
        dict.set_item(key?, offset)?;
    }
    Ok(dict.into())
}

fn convert_py_committable(
    py: Python<'_>,
    py_committable: Py<PyAny>,
) -> Result<BTreeMap<Partition, u64>, PyErr> {
    let mut committable = BTreeMap::new();
    let dict = py_committable.downcast_bound::<PyDict>(py)?;
    for (key, value) in dict.iter() {
        let partition = key.downcast::<PyTuple>()?;
        let topic: String = partition.get_item(0)?.extract()?;
        let index: u16 = partition.get_item(1)?.extract()?;
        let offset: u64 = value.extract()?;
        committable.insert(
            Partition {
                topic: Topic::new(&topic),
                index,
            },
            offset,
        );
    }
    Ok(committable)
}

impl ProcessingStrategy<RoutedValue> for PythonAdapter {
    /// Receives a message to process and forwards it to the Python delegate.
    ///
    /// It understand Python some exceptions returned. Specifically:
    /// - MessageRejected is interpreted as backpressure.
    /// - InvalidMessage is interpreted as a message for DLQ.
    /// Any other exception is unexpected and triggers a panic.
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            self.next_strategy.submit(message)
        } else {
            let mut committable = BTreeMap::new();
            for (partition, offset) in message.committable() {
                committable.insert(partition, offset);
            }

            Python::with_gil(|py| {
                let python_payload: Py<PyAny> = match message.payload().payload {
                    PyStreamingMessage::PyAnyMessage { ref content } => {
                        content.clone_ref(py).into_any()
                    }
                    PyStreamingMessage::RawMessage { ref content } => {
                        content.clone_ref(py).into_any()
                    }
                };
                let py_committable = convert_committable_to_py(py, committable).unwrap();
                match self.processing_step.call_method1(
                    py,
                    "submit",
                    (python_payload, py_committable),
                ) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        let error_type = err.get_type(py).name();
                        match error_type.unwrap().to_string().as_str() {
                            "MessageRejected" => {
                                Err(SubmitError::MessageRejected(MessageRejected { message }))
                            }
                            "InvalidMessage" => {
                                let py_err_obj = err.value(py);
                                let offset: u64 =
                                    py_err_obj.getattr("offset").unwrap().extract().unwrap();
                                let partition = py_err_obj.getattr("partition").unwrap();
                                Err(SubmitError::InvalidMessage(InvalidMessage {
                                    offset,
                                    partition: convert_partition(partition).unwrap(),
                                }))
                            }
                            _ => panic!("Unexpected exception from submit: {}", err),
                        }
                    }
                }
            })
        }
    }

    /// Polls messages from the Python delegate.
    ///
    /// This is the method that sends messages to the next ProcessingStrategy.
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let out_messages = Python::with_gil(|py| -> PyResult<Vec<Py<PyAny>>> {
            let ret = self.processing_step.call_method0(py, "poll")?;
            Ok(ret.extract(py).unwrap())
        });

        match out_messages {
            Ok(out_messages) => {
                Python::with_gil(|py| {
                    self.handle_py_return_value(py, out_messages);
                });
                while let Some(msg) = self.transformed_messages.pop_front() {
                    let commit_request = self.next_strategy.poll()?;
                    self.commit_request_carried_over = merge_commit_request(
                        self.commit_request_carried_over.take(),
                        commit_request,
                    );
                    match self.next_strategy.submit(msg) {
                        Err(SubmitError::MessageRejected(MessageRejected {
                            message: transformed_message,
                        })) => {
                            self.transformed_messages.push_front(transformed_message);
                            break;
                        }
                        Err(SubmitError::InvalidMessage(invalid_message)) => {
                            return Err(invalid_message.into());
                        }
                        Ok(_) => {}
                    }
                }

                let commit_request = self.next_strategy.poll()?;
                Ok(merge_commit_request(
                    self.commit_request_carried_over.take(),
                    commit_request,
                ))
            }
            Err(e) => Err(StrategyError::Other(Box::new(e))),
        }
    }

    fn terminate(&mut self) {
        self.next_strategy.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);
        let timeout_secs = timeout.map(|d| d.as_secs());

        let out_messages = Python::with_gil(|py| -> PyResult<Vec<Py<PyAny>>> {
            let ret = self
                .processing_step
                .call_method1(py, "flush", (timeout_secs,))?;
            Ok(ret.extract(py).unwrap())
        });

        match out_messages {
            Ok(out_messages) => {
                Python::with_gil(|py| {
                    self.handle_py_return_value(py, out_messages);
                });
                while let Some(msg) = self.transformed_messages.pop_front() {
                    let commit_request = self.next_strategy.poll()?;
                    self.commit_request_carried_over = merge_commit_request(
                        self.commit_request_carried_over.take(),
                        commit_request,
                    );
                    match self.next_strategy.submit(msg) {
                        Err(SubmitError::MessageRejected(MessageRejected {
                            message: transformed_message,
                        })) => {
                            self.transformed_messages.push_front(transformed_message);
                            if deadline.map_or(false, |d| d.has_elapsed()) {
                                tracing::warn!("Timeout reached");
                                break;
                            }
                        }
                        Err(SubmitError::InvalidMessage(invalid_message)) => {
                            return Err(invalid_message.into());
                        }
                        Ok(_) => {}
                    }
                }
                let commit_request = self.next_strategy.join(timeout)?;
                Ok(merge_commit_request(
                    self.commit_request_carried_over.take(),
                    commit_request,
                ))
            }
            Err(e) => Err(StrategyError::Other(Box::new(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::test_operators::build_routed_value;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::noop::Noop;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::Mutex as RawMutex;

    fn build_operator(py: Python<'_>) -> Bound<'_, PyAny> {
        let class_def = c_str!(
            r#"
# Adding these classes here as I could not import them from
# arroyo
class Topic:
    def __init__(self, name):
        self.name = name

class Partition:
    def __init__(self, topic, index):
        self.topic = topic
        self.index = index

class MessageRejected(Exception):
    pass

class InvalidMessage(Exception):
    def __init__(self, partition, offset):
        self.partition = partition
        self.offset = offset

class RustOperatorDelegate:
    def __init__(self):
        self.payload = None
        self.committable = None

    def submit(self, payload, committable):
        self.committable = committable
        if payload.payload == "ok":
            self.payload = payload
            return
        elif payload.payload == "reject":
            raise MessageRejected()
        elif payload.payload == "invalid":
            raise InvalidMessage(Partition(Topic("topic"), 0), 42)

    def poll(self):
        return [
            (self.payload, self.committable),
            (self.payload, self.committable)
        ]

    def flush(self, timeout: float | None = None):
        return [
            (self.payload, self.committable)
        ]

class RustOperatorDelegateFactory:
    def build(self):
        return RustOperatorDelegate()
    "#
        );
        let scope = PyModule::new(py, "test_scope").unwrap();
        py.run(class_def, Some(&scope.dict()), None).unwrap();
        let operator = scope.getattr("RustOperatorDelegateFactory").unwrap();
        operator.call0().unwrap()
    }

    fn make_msg(py: Python<'_>, payload: &str) -> Message<RoutedValue> {
        let routed_value = build_routed_value(
            py,
            payload.into_py_any(py).unwrap(),
            "source1",
            vec!["waypoint1".to_string()],
        );
        let mut committable = BTreeMap::new();
        committable.insert(
            Partition {
                topic: Topic::new("topic1"),
                index: 0,
            },
            123,
        );
        Message::new_any_message(routed_value, committable)
    }

    #[test]
    fn test_convert_committable_to_py_and_back() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            // Prepare a committable with two partitions
            let mut committable = BTreeMap::new();
            committable.insert(
                Partition {
                    topic: Topic::new("topic1"),
                    index: 0,
                },
                123,
            );
            committable.insert(
                Partition {
                    topic: Topic::new("topic2"),
                    index: 1,
                },
                456,
            );

            // Convert to Python object and back
            let py_obj = convert_committable_to_py(py, committable.clone()).unwrap();
            let committable_back = convert_py_committable(py, py_obj).unwrap();

            // Assert equality
            assert_eq!(committable, committable_back);
        });
    }

    #[test]
    fn test_submit_with_matching_route() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let instance = build_operator(py);
            let mut operator = PythonAdapter::new(
                Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                instance.unbind(),
                Box::new(Noop {}),
            );

            let message = make_msg(py, "ok");
            let res = operator.submit(message);
            assert!(res.is_ok());

            let message = make_msg(py, "reject");
            let res = operator.submit(message);
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(SubmitError::MessageRejected(MessageRejected { .. }))
            ));

            let message = make_msg(py, "invalid");
            let res = operator.submit(message);
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(SubmitError::InvalidMessage(InvalidMessage {
                    partition: Partition { .. },
                    offset: 42
                }))
            ));
        })
    }

    #[test]
    fn test_poll_with_messages() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let instance = build_operator(py);

            let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy::new(submitted_messages, false);

            let mut operator = PythonAdapter::new(
                Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                instance.unbind(),
                Box::new(next_step),
            );

            let message = make_msg(py, "ok");
            let res = operator.submit(message);
            assert!(res.is_ok());

            let commit_request = operator.poll();
            assert!(commit_request.is_ok());

            {
                let expected_messages =
                    vec!["ok".into_py_any(py).unwrap(), "ok".into_py_any(py).unwrap()];
                let actual_messages = submitted_messages_clone.lock().unwrap();
                assert_messages_match(py, expected_messages, actual_messages.deref());
            } // Unlock the MutexGuard around `actual_messages`

            assert_eq!(
                commit_request.unwrap(),
                Some(CommitRequest {
                    positions: HashMap::from([(
                        Partition {
                            topic: Topic::new("topic1"),
                            index: 0,
                        },
                        123
                    )]),
                })
            );

            let commit_request = operator.join(Some(Duration::from_secs(1)));
            assert!(commit_request.is_ok());

            {
                let expected_messages = vec![
                    "ok".into_py_any(py).unwrap(),
                    "ok".into_py_any(py).unwrap(),
                    "ok".into_py_any(py).unwrap(),
                ];
                let actual_messages = submitted_messages_clone.lock().unwrap();
                assert_messages_match(py, expected_messages, actual_messages.deref());
            } // Unlock the MutexGuard around `actual_messages`
        })
    }

    #[test]
    fn test_poll_and_fail() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let instance = build_operator(py);

            let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy::new(submitted_messages, true);

            let mut operator = PythonAdapter::new(
                Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                instance.unbind(),
                Box::new(next_step),
            );

            let message = make_msg(py, "ok");
            let res = operator.submit(message);
            assert!(res.is_ok());

            let commit_request = operator.poll();
            assert!(matches!(
                commit_request,
                Err(StrategyError::InvalidMessage(InvalidMessage {
                    partition: Partition { .. },
                    offset: 0
                }))
            ));

            {
                let expected_messages = vec![];
                let actual_messages = submitted_messages_clone.lock().unwrap();
                assert_messages_match(py, expected_messages, actual_messages.deref());
            } // Unlock the MutexGuard around `actual_messages`
        })
    }
}
