use crate::routes::{Route, RoutedValue};
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
        if self.route != message.payload().route {
            self.next_step.submit(message)
        } else {
            Python::with_gil(|py: Python<'_>| -> Result<(), SubmitError<RoutedValue>> {
                let python_payload = message.payload().payload.clone_ref(py);
                let py_res = self.callable.call1(py, (python_payload,));

                match py_res {
                    Ok(boolean) => {
                        let filtered = boolean.is_truthy(py).unwrap();
                        if filtered {
                            let _ = self.next_step.submit(message);
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
            })
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
