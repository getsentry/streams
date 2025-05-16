use super::*;
use crate::routes::RoutedValue;

use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{AnyMessage, BrokerMessage, InnerMessage, Message, Partition, Topic};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct FakeStrategy {
    pub submitted: Arc<Mutex<Vec<Py<PyAny>>>>,
    pub reject_message: bool,
}

impl ProcessingStrategy<RoutedValue> for FakeStrategy {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(None)
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.reject_message {
            match message.inner_message {
                InnerMessage::BrokerMessage(BrokerMessage { .. }) => {
                    Err(SubmitError::MessageRejected(MessageRejected { message }))
                }
                InnerMessage::AnyMessage(AnyMessage { .. }) => {
                    Err(SubmitError::InvalidMessage(InvalidMessage {
                        offset: 0,
                        partition: Partition {
                            topic: Topic::new("test"),
                            index: 0,
                        },
                    }))
                }
            }
        } else {
            self.submitted
                .lock()
                .unwrap()
                .push(message.into_payload().payload);
            Ok(())
        }
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
