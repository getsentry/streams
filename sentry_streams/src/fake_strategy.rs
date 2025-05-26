use super::*;
use crate::messages::PyStreamingMessage;
use crate::routes::RoutedValue;

use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
    StrategyError, SubmitError,
};
use sentry_arroyo::types::{AnyMessage, BrokerMessage, InnerMessage, Message, Partition, Topic};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct FakeStrategy {
    pub submitted: Arc<Mutex<Vec<Py<PyAny>>>>,
    pub reject_message: bool,
    commit_request: Option<CommitRequest>,
}

impl FakeStrategy {
    pub fn new(submitted: Arc<Mutex<Vec<Py<PyAny>>>>, reject_message: bool) -> Self {
        Self {
            submitted,
            reject_message,
            commit_request: None,
        }
    }
}

fn build_commit_request(message: &Message<RoutedValue>) -> CommitRequest {
    let mut offsets = HashMap::new();
    for (partition, offset) in message.committable() {
        offsets.insert(partition, offset);
    }

    CommitRequest { positions: offsets }
}

impl ProcessingStrategy<RoutedValue> for FakeStrategy {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit_request.take())
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
            self.commit_request = merge_commit_request(
                self.commit_request.take(),
                Some(build_commit_request(&message)),
            );

            Python::with_gil(|py| {
                let msg = match message.into_payload().payload {
                    PyStreamingMessage::PyAnyMessage { content } => {
                        content.bind(py).getattr("payload").unwrap()
                    }
                    PyStreamingMessage::RawMessage { content } => {
                        content.bind(py).getattr("payload").unwrap()
                    }
                };

                self.submitted.lock().unwrap().push(msg.unbind());
            });
            Ok(())
        }
    }

    fn terminate(&mut self) {}

    fn join(&mut self, _: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit_request.take())
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
            "Message at index {} differs {actual} - {expected}",
            i
        );
    }
}
