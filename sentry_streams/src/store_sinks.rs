use std::time::Duration;

use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

use crate::gcs_writer::GCSWriter;
use crate::routes::{Route, RoutedValue};

/// A specific sink which initializes a
/// RunTaskInThreads in order to write to GCS
pub struct GCSSink<N> {
    inner: RunTaskInThreads<RoutedValue, RoutedValue, anyhow::Error, N>,
}

impl<N> GCSSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    pub fn new(
        route: Route,
        next_step: N,
        concurrency: &ConcurrencyConfig,
        bucket: &str,
        object_file: &str,
    ) -> Self {
        let next_step = next_step;

        let inner = RunTaskInThreads::new(
            next_step,
            GCSWriter::new(bucket, object_file, route.clone()),
            concurrency,
            Some("GCS"),
        );

        GCSSink { inner }
    }
}

impl<N> ProcessingStrategy<RoutedValue> for GCSSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        self.inner.submit(message)
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}
