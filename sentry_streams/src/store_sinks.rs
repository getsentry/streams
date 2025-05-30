use sentry_arroyo::processing::strategies::run_task::RunTask;
use sentry_arroyo::processing::strategies::ProcessingStrategy;
use sentry_arroyo::types::Message;

use crate::gcs_writer::GCSWriter;
use crate::routes::{Route, RoutedValue};

pub fn build_gcs_sink(
    route: &Route,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
    bucket: &str,
    object_file: &str,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let writer = GCSWriter::new(bucket, object_file);
    let copied_route = route.clone();

    let gcs_writer = move |message: Message<RoutedValue>| {
        if message.payload().route != copied_route {
            Ok(message)
        } else {
            writer.write_to_gcs(message)
        }
    };
    Box::new(RunTask::new(gcs_writer, next))
}

#[cfg(test)]
mod tests {
    use pyo3::IntoPyObjectExt;
    use pyo3::Python;
    use std::env;
    use std::ops::Deref;
    use std::{collections::BTreeMap, sync::Arc};

    use crate::fake_strategy::assert_messages_match;
    use crate::{fake_strategy::FakeStrategy, test_operators::build_routed_value};
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn test_gcs_sink() {
        pyo3::prepare_freethreaded_python();

        Python::with_gil(|py| {
            let route = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
            let bucket = "test-bucket";
            let object_file = "test-object.txt";
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let next_step = FakeStrategy::new(submitted_messages, false);
            let mut strategy = build_gcs_sink(&route, Box::new(next_step), bucket, object_file);
            env::set_var("GCS_ACCESS_TOKEN", "TEST_TOKEN");

            // Separate route message. Not put into the sink
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint2".to_string()],
                ),
                BTreeMap::new(),
            );
            let _ = strategy.submit(message);
            //assert!(result.is_ok());

            // Separate route message. Not put into the sink
            let message2 = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message2".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint2".to_string()],
                ),
                BTreeMap::new(),
            );
            let result = strategy.submit(message2);
            assert!(result.is_ok());

            let expected_messages = vec![
                "test_message".into_py_any(py).unwrap(),
                "test_message2".into_py_any(py).unwrap(),
            ];
            let actual_messages = submitted_messages_clone.lock().unwrap();

            assert_messages_match(py, expected_messages, actual_messages.deref());
        })
    }
}
