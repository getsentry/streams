use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::warn;

#[cfg(test)]
use crate::mocks::current_epoch;
#[cfg(not(test))]
use crate::time_helpers::current_epoch;

/// A strategy that periodically sends watermark messages.
/// This strategy is added as the first step in a consumer.
/// The Watermark step tracks the committable of each message which passes through the step,
/// then when poll is called the last seen committable is sent in a WatermarkMessage payload.
/// The Arroyo adapter commit step only commits once it recives a watermark
/// message with a specific committable from all branches in a consumer.
pub struct WatermarkEmitter {
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
    pub period: u64,
    pub watermark_committable: BTreeMap<Partition, u64>,
    last_sent_timestamp: u64,
}

impl WatermarkEmitter {
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
            watermark_committable: empty_committable,
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
            payload: RoutedValuePayload::make_watermark_payload(
                self.watermark_committable.clone(),
                timestamp,
            ),
        };
        let result = self.next_step.submit(Message::new_any_message(
            watermark_msg,
            self.watermark_committable.clone(),
        ));
        match result {
            Ok(..) => {
                self.last_sent_timestamp = timestamp;
                self.watermark_committable = BTreeMap::new();
                Ok(())
            }
            Err(err) => match err {
                SubmitError::MessageRejected(..) => Ok(()),
                SubmitError::InvalidMessage(invalid_message) => Err(invalid_message),
            },
        }
    }

    fn merge_watermark_committable(&mut self, message: &Message<RoutedValue>) {
        for (partition, offset) in message.committable() {
            let current_offset = self.watermark_committable.get(&partition).unwrap_or(&0);
            // Message offsets should always be increasing
            if &offset >= current_offset {
                self.watermark_committable.insert(partition, offset);
            } else {
                warn!("Received offset lower than current offset for partition {partition}: {offset} vs {current_offset}");
            }
        }
    }
}

impl ProcessingStrategy<RoutedValue> for WatermarkEmitter {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        if self.should_send_watermark_msg() {
            self.send_watermark_msg()?;
        }
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        self.merge_watermark_committable(&message);
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
    use crate::commit_policy::WatermarkCommitOffsets;
    use crate::consumer::build_chain;
    use crate::fake_strategy::{assert_watermarks_match, FakeStrategy};
    use crate::messages::Watermark;
    use crate::mocks::set_timestamp;
    use crate::operators::RuntimeOperator;
    use crate::routes::Route;
    use crate::testutils::{build_routed_value, make_committable, make_lambda, make_msg};
    use crate::utils::traced_with_gil;
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::run_task_in_threads::ConcurrencyConfig;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use sentry_arroyo::types::Topic;
    use std::collections::HashMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_watermark_poll() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);
            let mut watermark = WatermarkEmitter::new(
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
            assert_eq!(watermark.watermark_committable, committable);

            // Watermark step merges committables
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
            let expected_committable = make_committable(4, 1);
            assert_eq!(watermark.watermark_committable, expected_committable);

            // submitted WatermarkMessage contains the last seen committable
            watermark.last_sent_timestamp = 0;
            set_timestamp(20);
            let _ = watermark.poll();
            assert_watermarks_match(
                vec![Watermark::new(expected_committable, 0)],
                submitted_watermarks_clone.lock().unwrap().deref(),
            );
            set_timestamp(0);
        })
    }

    #[test]
    fn test_watermark_to_commit_step() {
        // TODO: once Router works with watermarks, ensure commit request is not returned
        // from commit step if messages from all routes haven't reached commit step
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let callable = make_lambda(py, c_str!("lambda x: x"));
            let map_step = Py::new(
                py,
                RuntimeOperator::Map {
                    route: Route::new("source".to_string(), vec![]),
                    function: callable,
                },
            )
            .unwrap();
            let mut watermark_step = build_chain(
                "source",
                &[map_step],
                Box::new(WatermarkCommitOffsets::new(1)),
                &ConcurrencyConfig::new(1),
                &None,
            );

            let msg1 = make_msg(
                Some(b"test_message".to_vec()),
                BTreeMap::from([(Partition::new(Topic::new("test_topic"), 0), 100)]),
            );
            let _ = watermark_step.submit(msg1);

            let msg2 = make_msg(
                Some(b"test_message2".to_vec()),
                BTreeMap::from([(Partition::new(Topic::new("test_topic"), 1), 80)]),
            );
            let _ = watermark_step.submit(msg2);
            let res = watermark_step.poll().unwrap();
            assert!(res.is_none());

            set_timestamp(20);
            let res = watermark_step.poll().unwrap();
            assert_eq!(
                res,
                Some(CommitRequest {
                    positions: HashMap::from([
                        (Partition::new(Topic::new("test_topic"), 0), 100),
                        (Partition::new(Topic::new("test_topic"), 1), 80),
                    ])
                })
            );
            set_timestamp(0);
        });
    }
}
