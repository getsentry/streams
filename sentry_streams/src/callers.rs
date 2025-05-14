use crate::routes::RoutedValue;
use pyo3::prelude::*;
use sentry_arroyo::types::Message;

/// Executes a Python callable with an Arroyo message containing Any and
/// returns the result.
pub fn call_python_function(
    callable: &Py<PyAny>,
    message: &Message<RoutedValue>,
) -> Result<Py<PyAny>, PyErr> {
    Python::with_gil(|py| {
        let python_payload = message.payload().payload.clone_ref(py);
        callable.call1(py, (python_payload,))
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
            let callable = make_lambda(py, c_str!("lambda x: x + '_transformed'"));

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

            assert_eq!(
                result.extract::<String>(py).unwrap(),
                "test_message_transformed"
            );
        });
    }
}
