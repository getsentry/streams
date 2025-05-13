//! This module contains the implementation of the PythonOperator Arroyo
//! processing strategy that delegates the processing of messages to the
//! python operator.

use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, StrategyError,
};
use sentry_arroyo::types::{Message, Partition};
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

            let payload = message.into_payload().payload;
            let out_messages = Python::with_gil(|py| -> PyResult<Vec<Py<PyAny>>> {
                let ret = self
                    .processing_step
                    .call_method1(py, "submit", (payload,))?;
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
                            committable.clone(),
                        );
                        self.next_strategy.submit(message)?;
                    }
                    Ok(())
                }
                Err(e) => Ok(()),
            }
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
