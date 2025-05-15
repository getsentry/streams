use crate::routes::{Route, RoutedValue};
use pyo3::{Py, PyAny, Python};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::Duration;

pub struct Filter {
    pub callable: Py<PyAny>,
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
}

impl Filter {
    /// A strategy that takes a callable, and applies it to messages
    /// to either filter the message out or submit it to the next step.
    /// The callable is expected to take a Message<Py<PyAny>>
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
            let filtered = Python::with_gil(|py: Python<'_>| -> bool {
                let python_payload = message.payload().payload.clone_ref(py);
                let py_res = self.callable.call1(py, (python_payload,));

                // TODO: Create an exception for Invalid messages in Python
                // This now crashes if the Python code fails.
                let boolean = py_res.unwrap();
                boolean.is_truthy(py).unwrap()
            });

            if filtered {
                let _ = self.next_step.submit(message);
            }

            Ok(())
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
