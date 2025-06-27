use pyo3::{import_exception, prelude::*, types::PyTuple};

import_exception!(sentry_streams.pipeline.exception, InvalidMessageError);

pub type ApplyResult<T> = Result<T, ApplyError>;

#[derive(Debug, PartialEq)]
pub enum ApplyError {
    InvalidMessage,
    ApplyFailed,
}

pub fn try_apply_py<'py, N>(
    py: Python<'py>,
    callable: &Py<PyAny>,
    args: N,
) -> ApplyResult<Py<PyAny>>
where
    N: IntoPyObject<'py, Target = PyTuple>,
{
    callable.call1(py, args).map_err(|py_err| {
        py_err.print(py);
        if py_err.is_instance(py, &py.get_type::<InvalidMessageError>()) {
            ApplyError::InvalidMessage
        } else {
            ApplyError::ApplyFailed
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::PyStreamingMessage;
    use crate::messages::RoutedValuePayload;
    use crate::testutils::build_routed_value;
    use crate::testutils::import_py_dep;
    use crate::testutils::make_lambda;
    use crate::utils::traced_with_gil;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::types::Message;
    use std::collections::BTreeMap;

    #[test]
    fn test_apply_py_invalid_msg_err() {
        pyo3::prepare_freethreaded_python();

        import_py_dep("sentry_streams.pipeline.exception", "InvalidMessageError");

        traced_with_gil!(|py| {
            let callable = make_lambda(
                py,
                c_str!("lambda: (_ for _ in ()).throw(InvalidMessageError())"),
            );

            assert!(matches!(
                try_apply_py(py, &callable, ()),
                Err(ApplyError::InvalidMessage)
            ));
        });
    }

    #[test]
    fn test_apply_py_throws_other_exception() {
        pyo3::prepare_freethreaded_python();

        traced_with_gil!(|py| {
            let callable = make_lambda(py, c_str!("lambda x: {}[0]"));

            assert!(matches!(
                try_apply_py(py, &callable, ()),
                Err(ApplyError::ApplyFailed)
            ));
        });
    }

    #[test]
    fn test_call_python_function() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
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

            let result = match message.payload().payload {
                RoutedValuePayload::PyStreamingMessage(ref msg) => {
                    traced_with_gil!(|py| {
                        match &msg {
                            PyStreamingMessage::PyAnyMessage { content } => {
                                try_apply_py(py, &callable, (content.clone_ref(py),))
                            }
                            PyStreamingMessage::RawMessage { content } => {
                                try_apply_py(py, &callable, (content.clone_ref(py),))
                            }
                        }
                        .unwrap()
                        .into()
                    })
                }
                RoutedValuePayload::WatermarkMessage(..) => unreachable!(),
            };

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
