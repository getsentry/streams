use super::*;
use crate::messages::{PyStreamingMessage, RoutedValuePayload, Watermark, WatermarkMessage};
use crate::routes::RoutedValue;
use crate::utils::traced_with_gil;

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
    pub submitted_watermarks: Arc<Mutex<Vec<Watermark>>>,
    pub reject_message: bool,
    commit_request: Option<CommitRequest>,
}

impl FakeStrategy {
    pub fn new(
        submitted: Arc<Mutex<Vec<Py<PyAny>>>>,
        submitted_watermarks: Arc<Mutex<Vec<Watermark>>>,
        reject_message: bool,
    ) -> Self {
        Self {
            submitted,
            submitted_watermarks,
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

            let msg_payload = message.into_payload().payload;
            match msg_payload {
                RoutedValuePayload::WatermarkMessage(msg) => match msg {
                    WatermarkMessage::Watermark(watermark) => {
                        self.submitted_watermarks.lock().unwrap().push(watermark)
                    }
                    WatermarkMessage::PyWatermark(..) => (),
                },
                RoutedValuePayload::PyStreamingMessage(py_payload) => {
                    traced_with_gil!(|py| {
                        let msg = match py_payload {
                            PyStreamingMessage::PyAnyMessage { content } => {
                                content.bind(py).getattr("payload").unwrap()
                            }
                            PyStreamingMessage::RawMessage { content } => {
                                content.bind(py).getattr("payload").unwrap()
                            }
                        };
                        self.submitted.lock().unwrap().push(msg.unbind());
                    });
                }
            }
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
