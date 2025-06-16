use crate::messages::{RoutedValuePayload, WatermarkMessage};
use crate::routes::{Route, RoutedValue};
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current Unix epoch
fn current_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// A strategy that periodically sends watermark messages.
/// This strategy is added as the first step in a consumer.
/// The Watermark step tracks the committable of each message which passes through the step,
/// then when poll is called the last seen committable is sent in a WatermarkMessage payload.
/// The Arroyo adapter commit step only commits once it recives a watermark
/// message with a specific committable from all branches in a consumer.
pub struct Watermark {
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
    pub period: u64,
    pub last_seen_committable: BTreeMap<Partition, u64>,
    last_sent_timestamp: u64,
}

impl Watermark {
    pub fn new(
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
        period: u64,
    ) -> Self {
        let empty_committable = BTreeMap::new();
        let current_timestamp = current_epoch();
        Self {
            next_step,
            route,
            period,
            last_seen_committable: empty_committable,
            last_sent_timestamp: current_timestamp,
        }
    }

    fn should_send_watermark_msg(&self) -> bool {
        (self.last_sent_timestamp + self.period) < current_epoch()
    }

    fn send_watermark_msg(&mut self) -> Result<(), InvalidMessage> {
        let timestamp = current_epoch();
        let watermark_msg = RoutedValue {
            route: self.route.clone(),
            payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::new(self.last_seen_committable.clone())),
        };
        let result = self
            .next_step
            .submit(Message::new_any_message(watermark_msg, BTreeMap::new()));
        match result {
            Ok(..) => {
                self.last_sent_timestamp = timestamp;
                Ok(())
            }
            Err(err) => match err {
                SubmitError::MessageRejected(..) => Ok(()),
                SubmitError::InvalidMessage(invalid_message) => Err(invalid_message),
            },
        }
    }

    fn update_last_seen_committable(&mut self, message: &Message<RoutedValue>) {
        let mut committable = BTreeMap::new();
        for (partition, offset) in message.committable() {
            committable.insert(partition, offset);
        }
        self.last_seen_committable = committable;
    }
}

impl ProcessingStrategy<RoutedValue> for Watermark {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        if self.should_send_watermark_msg() {
            self.send_watermark_msg()?;
        }
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        self.update_last_seen_committable(&message);
        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)?;
        if self.should_send_watermark_msg() {
            self.send_watermark_msg()?;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::FakeStrategy;
    use crate::routes::Route;
    use crate::test_operators::{build_routed_value, make_committable};
    use crate::utils::traced_with_gil;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::{processing::strategies::ProcessingStrategy};
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_watermark_poll() {
        pyo3::prepare_freethreaded_python();
        traced_with_gil("test_call_python_function", |py| {
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);
            let mut watermark = Watermark::new(
                Box::new(next_step),
                Route {
                    source: String::from("source"),
                    waypoints: vec![],
                },
                10,
            );

            // Watermark step records message committable
            let committable = make_committable(2, 1);
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                committable.clone(),
            );
            let _ = watermark.submit(message);
            assert_eq!(watermark.last_seen_committable, committable);

            // Watermark step updates committable
            let committable = make_committable(3, 2);
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source1",
                    vec!["waypoint1".to_string()],
                ),
                committable.clone(),
            );
            let _ = watermark.submit(message);
            assert_eq!(watermark.last_seen_committable, committable);

            // submitted WatermarkMessage contains the last seen committable
            watermark.last_sent_timestamp = 0;
            let _ = watermark.poll();
            assert_eq!(submitted_watermarks_clone.lock().unwrap()[0], WatermarkMessage::new(committable));
        })
    }
}
