use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::types::Message;

/// Executes a Python callable with an Arroyo message containing Any and
/// returns the result.
fn call_python_function(callable: &Py<PyAny>, message: &Message<Py<PyAny>>) -> Py<PyAny> {
    Python::with_gil(|py| {
        let result = callable.call1(py, (message.payload(),)).unwrap();
        result
    })
}

/// Creates an Arroyo transformer strategy that uses a Python callable to
/// transform messages. The callable is expected to take a Message<Py<PyAny>>
/// as input and return a transformed message. The strategy is built on top of
/// the `RunTask` Arroyo strategy.
///
/// This function takes a `next`  step to wire the Arroyo strategy to.
pub fn build_map(
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<Py<PyAny>>>,
) -> Box<dyn ProcessingStrategy<Py<PyAny>>> {
    let mapper = move |message| {
        let transformed = call_python_function(&callable, &message);
        Ok(message.replace(transformed))
    };
    Box::new(RunTask::new(mapper, next))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_build_map() {
        Python::with_gil(|py| {
            let callable = py
                .eval(c_str!("lambda x: x + '_transformed'"), None, None)
                .unwrap()
                .into_py_any(py)
                .unwrap();
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy {
                submitted: submitted_messages,
            };

            let mut strategy = build_map(callable, Box::new(next_step));

            let message =
                Message::new_any_message("test_message".into_py_any(py).unwrap(), BTreeMap::new());
            let result = strategy.submit(message);

            assert!(result.is_ok());

            let value = "test_message_transformed".into_py_any(py).unwrap();
            let expected_messages = vec![value];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());
        });
    }

    #[test]
    fn test_call_python_function() {
        Python::with_gil(|py| {
            let callable = py
                .eval(c_str!("lambda x: x + '_transformed'"), None, None)
                .unwrap()
                .into_py_any(py)
                .unwrap();

            let message =
                Message::new_any_message("test_message".into_py_any(py).unwrap(), BTreeMap::new());

            let result = call_python_function(&callable, &message);

            assert_eq!(
                result.extract::<String>(py).unwrap(),
                "test_message_transformed"
            );
        });
    }
}
