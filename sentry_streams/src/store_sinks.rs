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
