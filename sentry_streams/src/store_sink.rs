use std::time::Duration;

use sentry_arroyo::processing::strategies::run_task_in_threads::{
    ConcurrencyConfig, RunTaskInThreads,
};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;

use crate::gcs_writer::GCSWriter;
use crate::routes::RoutedValue;

pub enum Storage {
    GCS,
}

/// A generic WriterStep which initializes a
/// RunTaskInThreads with the appropriate specific Writer
pub struct WriterStep<N> {
    inner: RunTaskInThreads<RoutedValue, (), anyhow::Error, N>,
}

impl<N> WriterStep<N>
where
    N: ProcessingStrategy<()> + 'static,
{
    pub fn new(next_step: N, concurrency: &ConcurrencyConfig, storage: Storage) -> Self {
        let writer = match storage {
            Storage::GCS => GCSWriter::new(),
            _ => panic!("We're cooked"),
        };

        let inner = RunTaskInThreads::new(next_step, writer, concurrency, Some("store"));

        WriterStep { inner }
    }
}

impl<N> ProcessingStrategy<RoutedValue> for WriterStep<N>
where
    N: ProcessingStrategy<()>,
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
