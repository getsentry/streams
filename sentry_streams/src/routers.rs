use crate::callers::call_any_python_function;
use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::{InvalidMessage, ProcessingStrategy, SubmitError};
use sentry_arroyo::types::{InnerMessage, Message};

fn route_message(
    route: &Route,
    callable: &Py<PyAny>,
    message: Message<RoutedValue>,
) -> Result<Message<RoutedValue>, SubmitError<RoutedValue>> {
    if message.payload().route != *route {
        return Ok(message);
    }
    let dest_route = call_any_python_function(callable, &message);
    match dest_route {
        Ok(dest_route) => {
            let new_waypoint = Python::with_gil(|py| dest_route.extract::<String>(py).unwrap());
            message.try_map(|payload| Ok(payload.add_waypoint(new_waypoint.clone())))
        }
        Err(_) => match message.inner_message {
            InnerMessage::BrokerMessage(inner) => {
                Err(SubmitError::InvalidMessage(InvalidMessage {
                    partition: inner.partition,
                    offset: inner.offset,
                }))
            }
            InnerMessage::AnyMessage(inner) => panic!("Unexpected message type: {:?}", inner),
        },
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
    let copied_route = route.clone();
    let mapper =
        move |message: Message<RoutedValue>| route_message(&copied_route, &callable, message);

    Box::new(RunTask::new(mapper, next))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_operators::build_routed_value;
    use crate::test_operators::make_lambda;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use std::collections::BTreeMap;

    #[test]
    fn test_route_msg() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let callable = make_lambda(py, c_str!("lambda x: 'waypoint2'"));

            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );

            let routed = route_message(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                &callable,
                message,
            );

            let routed = routed.unwrap();

            assert_eq!(
                routed.payload().route,
                Route::new(
                    "source1".to_string(),
                    vec!["waypoint1".to_string(), "waypoint2".to_string()]
                )
            );

            let through = route_message(
                &Route::new("source3".to_string(), vec!["waypoint1".to_string()]),
                &callable,
                routed,
            );
            let through = through.unwrap();
            assert_eq!(
                through.payload().route,
                Route::new(
                    "source1".to_string(),
                    vec!["waypoint1".to_string(), "waypoint2".to_string()]
                )
            );
        });
    }
}
