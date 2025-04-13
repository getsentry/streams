use super::*;
use crate::routes::RoutedValue;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct FakeStrategy {
    pub submitted: Arc<Mutex<Vec<Py<PyAny>>>>,
}

impl ProcessingStrategy<RoutedValue> for FakeStrategy {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        self.submitted
            .lock()
            .unwrap()
            .push(message.into_payload().payload);
        Ok(())
    }

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }
}

#[cfg(test)]
pub fn assert_messages_match(
    py: Python<'_>,
    expected_messages: Vec<Py<PyAny>>,
    actual_messages: &[Py<PyAny>],
) {
    assert_eq!(
        expected_messages.len(),
        actual_messages.len(),
        "Message lengths differ"
    );

    for (i, (actual, expected)) in actual_messages
        .iter()
        .zip(expected_messages.iter())
        .enumerate()
    {
        assert!(
            actual.bind(py).eq(expected.bind(py)).unwrap(),
            "Message at index {} differs",
            i
        );
    }
}
