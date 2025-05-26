use crate::callers::call_python_function;
use crate::filter_step::Filter;
use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::types::Message;

/// Creates an Arroyo transformer strategy that uses a Python callable to
/// transform messages. The callable is expected to take a Message<RoutedValue>
/// as input and return a transformed message. The strategy is built on top of
/// the `RunTask` Arroyo strategy.
///
/// This function takes a `next`  step to wire the Arroyo strategy to.
pub fn build_map(
    route: &Route,
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();
    let mapper = move |message: Message<RoutedValue>| {
        if message.payload().route != copied_route {
            Ok(message)
        } else {
            let transformed = call_python_function(&callable, &message);
            // TODO: Create an exception for Invalid messages in Python
            // This now crashes if the Python code fails.
            let route = message.payload().route.clone();
            Ok(message.replace(RoutedValue {
                route,
                payload: transformed.unwrap(),
            }))
        }
    };
    Box::new(RunTask::new(mapper, next))
}

/// Creates an Arroyo-based filter step strategy that uses a Python callable to
/// filter out messages. The callable is expected to take a Message<RoutedValue>
/// as input and return a bool. The strategy is a custom Processing Strategy,
/// defined in sentry_streams/src.
///
/// This function takes a `next` step to wire the Arroyo strategy to.
pub fn build_filter(
    route: &Route,
    callable: Py<PyAny>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let copied_route = route.clone();
    Box::new(Filter::new(callable, next, copied_route))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::routes::Route;
    use crate::test_operators::build_routed_value;
    use crate::test_operators::make_lambda;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_build_map() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let callable = make_lambda(
                py,
                c_str!("lambda x: x.replace_payload(x.payload + '_transformed')"),
            );
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy::new(submitted_messages, false);

            let mut strategy = build_map(
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

            let expected_messages = vec![
                "test_message_transformed".into_py_any(py).unwrap(),
                "test_message".into_py_any(py).unwrap(),
            ];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());
        });
    }
}
