use crate::callers::{try_apply_py, ApplyError};
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::{ProcessingStrategy, SubmitError};
use sentry_arroyo::types::{InnerMessage, Message};

#[allow(clippy::result_large_err)]
fn route_message(
    route: &Route,
    callable: &Py<PyAny>,
    message: Message<RoutedValue>,
) -> Result<Message<RoutedValue>, SubmitError<RoutedValue>> {
    if message.payload().route != *route {
        return Ok(message);
    }

    let RoutedValuePayload::PyStreamingMessage(ref py_streaming_msg) = message.payload().payload
    else {
        // TODO: a future PR will remove this gate on WatermarkMessage and duplicate it for each downstream route.
        return Ok(message);
    };

    let res = traced_with_gil!(|py| {
        try_apply_py(py, callable, (Into::<Py<PyAny>>::into(py_streaming_msg),)).and_then(
            |py_res| {
                py_res
                    .extract::<String>(py)
                    .map_err(|_| ApplyError::ApplyFailed)
            },
        )
    });

    match (res, &message.inner_message) {
        (Ok(new_waypoint), _) => {
            message.try_map(|payload| Ok(payload.add_waypoint(new_waypoint.clone())))
        },
        (Err(ApplyError::ApplyFailed), _) => panic!("Python route function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"),
        (Err(ApplyError::InvalidMessage), InnerMessage::AnyMessage(..)) => panic!("Got exception while processing AnyMessage, Arroyo cannot handle error on AnyMessage"),
        (Err(ApplyError::InvalidMessage),  InnerMessage::BrokerMessage(broker_message)) => Err(SubmitError::InvalidMessage(broker_message.into()))
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
    use crate::testutils::build_routed_value;
    use crate::testutils::import_py_dep;
    use crate::testutils::make_lambda;
    use crate::utils::traced_with_gil;
    use chrono::Utc;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::noop::Noop;
    use sentry_arroyo::processing::strategies::InvalidMessage;
    use sentry_arroyo::types::Partition;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::ffi::CStr;

    fn create_simple_router<T>(
        lambda_body: &CStr,
        next_step: T,
    ) -> Box<dyn ProcessingStrategy<RoutedValue>>
    where
        T: ProcessingStrategy<RoutedValue> + 'static,
    {
        traced_with_gil!(|py| {
            py.run(lambda_body, None, None).expect("Unable to import");
            let callable = make_lambda(py, lambda_body);

            build_router(
                &Route::new("source1".to_string(), vec!["waypoint1".to_string()]),
                callable,
                Box::new(next_step),
            )
        })
    }

    #[test]
    #[should_panic(
        expected = "Got exception while processing AnyMessage, Arroyo cannot handle error on AnyMessage"
    )]
    fn test_router_crashes_on_any_msg() {
        pyo3::prepare_freethreaded_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        let mut router = create_simple_router(
            c_str!("lambda x: (_ for _ in ()).throw(InvalidMessageError())"),
            Noop {},
        );

        traced_with_gil!(|py| {
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let _ = router.submit(message);
        });
    }

    #[test]
    #[should_panic(
        expected = "Python route function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"
    )]
    fn test_router_crashes_on_normal_exceptions() {
        pyo3::prepare_freethreaded_python();

        let mut router = create_simple_router(c_str!("lambda x: {}[0]"), Noop {});

        traced_with_gil!(|py| {
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );
            let _ = router.submit(message);
        });
    }

    #[test]
    #[should_panic(
        expected = "Python route function raised exception that is not sentry_streams.pipeline.exception.InvalidMessageError"
    )]
    fn test_router_handles_invalid_msg_exception() {
        pyo3::prepare_freethreaded_python();

        let mut router = create_simple_router(c_str!("lambda x: {}[0]"), Noop {});

        traced_with_gil!(|py| {
            let message = Message::new_broker_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                Partition::new(Topic::new("topic"), 2),
                10,
                Utc::now(),
            );
            let SubmitError::InvalidMessage(InvalidMessage { partition, offset }) =
                router.submit(message).unwrap_err()
            else {
                panic!("Expected SubmitError::InvalidMessage")
            };

            assert_eq!(partition, Partition::new(Topic::new("topic"), 2));
            assert_eq!(offset, 10);
        });
    }

    #[test]
    fn test_route_msg() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
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
