//! This module contains the implementation of the PythonOperator Arroyo
//! processing strategy that delegates the processing of messages to the
//! python operator.

use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use pyo3::Python;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, StrategyError,
};
use sentry_arroyo::types::{Message, Partition, Topic};
use std::collections::BTreeMap;
use std::time::Duration;

pub struct PythonOperator {
    pub route: Route,
    pub processing_step: Py<PyAny>,
    next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
}

impl PythonOperator {
    pub fn new(
        route: Route,
        processing_step: Py<PyAny>,
        next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    ) -> Self {
        Self {
            route,
            processing_step,
            next_strategy,
        }
    }
}

fn convert_partition(partition: Bound<'_, PyAny>) -> Partition {
    let partition_index: u16 = partition.getattr("index").unwrap().extract().unwrap();
    let topic = partition.getattr("topic").unwrap();
    let topic_name: String = topic.getattr("name").unwrap().extract().unwrap();
    Partition {
        topic: Topic::new(&topic_name),
        index: partition_index,
    }
}

impl ProcessingStrategy<RoutedValue> for PythonOperator {
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            self.next_strategy.submit(message)
        } else {
            let msg_committable = message.committable().clone();
            let mut committable = BTreeMap::new();
            for (partition, offset) in msg_committable {
                committable.insert(partition, offset);
            }

            Python::with_gil(|py| {
                let payload = message.payload().payload.clone_ref(py);
                match self.processing_step.call_method1(py, "submit", (payload,)) {
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
                                    partition: convert_partition(partition),
                                }))
                            }
                            _ => panic!("Unexpected exception from submit: {}", err),
                        }
                    }
                }
            })
        }
    }

    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let out_messages = Python::with_gil(|py| -> PyResult<Vec<Py<PyAny>>> {
            let ret = self.processing_step.call_method0(py, "poll")?;
            Ok(ret.extract(py).unwrap())
        });

        match out_messages {
            Ok(out_messages) => {
                for out_message in out_messages {
                    let message = Message::new_any_message(
                        RoutedValue {
                            route: self.route.clone(),
                            payload: out_message,
                        },
                        BTreeMap::new(),
                    );
                    match self.next_strategy.submit(message) {
                        Err(SubmitError::MessageRejected(MessageRejected {
                            message: transformed_message,
                        })) => {
                            //self.transformed_messages.push_front(transformed_message);
                            break;
                        }
                        Err(SubmitError::InvalidMessage(invalid_message)) => {
                            return Err(invalid_message.into());
                        }
                        Ok(_) => {}
                    }
                }
                let commit_request = self.next_strategy.poll()?;
                Ok(commit_request)
            }
            Err(e) => Ok(None),
        }
    }

    fn terminate(&mut self) {
        self.next_strategy.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let out_messages = Python::with_gil(|py| -> PyResult<Vec<Py<PyAny>>> {
            let ret = self.processing_step.call_method0(py, "join")?;
            Ok(ret.extract(py).unwrap())
        });

        match out_messages {
            Ok(out_messages) => {
                for out_message in out_messages {
                    let message = Message::new_any_message(
                        RoutedValue {
                            route: self.route.clone(),
                            payload: out_message,
                        },
                        BTreeMap::new(),
                    );
                    match self.next_strategy.submit(message) {
                        Err(SubmitError::MessageRejected(MessageRejected {
                            message: transformed_message,
                        })) => {
                            //self.transformed_messages.push_front(transformed_message);
                            break;
                        }
                        Err(SubmitError::InvalidMessage(invalid_message)) => {
                            return Err(invalid_message.into());
                        }
                        Ok(_) => {}
                    }
                }
                let commit_request = self.next_strategy.join(timeout)?;
                Ok(commit_request)
            }
            Err(e) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_operators::build_routed_value;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::noop::Noop;

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

class Operator:
    def submit(self, payload):
        if payload == "ok":
            return
        elif payload == "reject":
            raise MessageRejected()
        elif payload == "invalid":
            raise InvalidMessage(Partition(Topic("topic"), 0), 42)

    def poll(self):
        return []

    def flush(self):
        return []
    "#
        );
        let scope = PyModule::new(py, "test_scope").unwrap();
        py.run(class_def, Some(&scope.dict()), None).unwrap();
        let operator = scope.getattr("Operator").unwrap();
        operator.call0().unwrap()
    }

    #[test]
    fn test_submit_with_matching_route() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let instance = build_operator(py);
            let mut operator = PythonOperator::new(
                Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                instance.unbind(),
                Box::new(Noop {}),
            );

            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "ok".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );

            let res = operator.submit(message);
            assert!(res.is_ok());

            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "reject".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let res = operator.submit(message);
            assert!(res.is_err());
            assert!(matches!(
                res,
                Err(SubmitError::MessageRejected(MessageRejected { message }))
            ));

            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "invalid".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
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
}
