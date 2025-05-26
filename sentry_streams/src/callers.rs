use crate::messages::PyStreamingMessage;
use crate::routes::RoutedValue;
use pyo3::prelude::*;
use sentry_arroyo::types::Message;

/// Executes a Python callable with an Arroyo message containing Any and
/// returns the result.
pub fn call_python_function(
    callable: &Py<PyAny>,
    message: &Message<RoutedValue>,
) -> Result<PyStreamingMessage, PyErr> {
    Ok(Python::with_gil(|py| match message.payload().payload {
        PyStreamingMessage::PyAnyMessage { ref content } => {
            callable.call1(py, (content.clone_ref(py),))
        }
        PyStreamingMessage::RawMessage { ref content } => {
            callable.call1(py, (content.clone_ref(py),))
        }
    })?
    .into())
}

/// Executes a Python callable with an Arroyo message containing Any and
/// returns the result.
pub fn call_any_python_function(
    callable: &Py<PyAny>,
    message: &Message<RoutedValue>,
) -> Result<Py<PyAny>, PyErr> {
    Python::with_gil(|py| match message.payload().payload {
        PyStreamingMessage::PyAnyMessage { ref content } => {
            callable.call1(py, (content.clone_ref(py),))
        }
        PyStreamingMessage::RawMessage { ref content } => {
            callable.call1(py, (content.clone_ref(py),))
        }
    })
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
    fn test_call_python_function() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let callable = make_lambda(
                py,
                c_str!("lambda x: x.replace_payload(x.payload + '_transformed')"),
            );

            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                BTreeMap::new(),
            );

            let result = call_python_function(&callable, &message).unwrap();

            match result {
                PyStreamingMessage::PyAnyMessage { content } => {
                    let r = content.bind(py).getattr("payload").unwrap().unbind();
                    assert_eq!(r.extract::<String>(py).unwrap(), "test_message_transformed");
                }
                PyStreamingMessage::RawMessage { .. } => {
                    panic!("Expected PyAnyMessage, got RawMessage")
                }
            }
        });
    }
}
