use crate::routes::{Route, RoutedValue};
use pyo3::{Py, PyAny, Python};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::Duration;

pub struct Filter<N> {
    pub callable: Py<PyAny>,
    pub next_step: N,
    pub route: Route,
}

impl<N> Filter<N> {
    pub fn new(callable: Py<PyAny>, next_step: N, route: Route) -> Self {
        Self {
            callable,
            next_step,
            route,
        }
    }
}

impl<N> ProcessingStrategy<RoutedValue> for Filter<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            self.next_step.submit(message)
        } else {
            Python::with_gil(|py: Python<'_>| {
                let python_payload = message.payload().payload.clone_ref(py);
                let b = self.callable.call1(py, (python_payload,));

                match b {
                    Ok(py_obj) => {
                        let obj = py_obj.is_truthy(py);

                        if let Ok(boolean) = obj {
                            if boolean {
                                let _ = self.next_step.submit(message);
                            }
                        }
                        Ok(())
                    }
                    Err(_) => Ok(()),
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
