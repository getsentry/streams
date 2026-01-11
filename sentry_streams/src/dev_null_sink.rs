//! This module provides an implementation of the DevNullSink pipeline
//! step that discards all messages (similar to /dev/null).
//!
//! This sink is useful for testing and benchmarking pipelines where
//! you want to measure throughput without the overhead of actual I/O.
use crate::routes::{Route, RoutedValue};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::Duration;

/// Implements the DevNullSink logic.
///
/// This is an Arroyo strategy that discards messages matching its route
/// and forwards all other messages (including watermarks) to the next step.
pub struct DevNullSink<N> {
    /// The route this strategy processes. Every message not for this
    /// strategy is sent to the `next` strategy without being processed.
    route: Route,
    next_strategy: N,
}

impl<N> DevNullSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    pub fn new(route: Route, next_strategy: N) -> Self {
        DevNullSink {
            route,
            next_strategy,
        }
    }
}

impl<N> ProcessingStrategy<RoutedValue> for DevNullSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    /// Polls the next strategy and returns any commit requests.
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_strategy.poll()
    }

    /// Submit the message. If the route matches and it's not a watermark,
    /// discard it. Otherwise, forward to the next strategy.
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        // Always forward watermarks to ensure proper pipeline operation
        if message.payload().payload.is_watermark_msg() {
            return self.next_strategy.submit(message);
        }

        // If the route matches, discard the message (do nothing)
        if self.route == message.payload().route {
            Ok(())
        } else {
            // Forward messages that don't match our route
            self.next_strategy.submit(message)
        }
    }

    fn terminate(&mut self) {
        self.next_strategy.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_strategy.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::RoutedValuePayload;
    use crate::routes::Route;
    use crate::testutils::make_raw_routed_msg;
    use crate::utils::traced_with_gil;
    use sentry_arroyo::types::Partition;
    use sentry_arroyo::types::Topic;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex as RawMutex;

    #[test]
    fn test_devnull_discards_matching_messages() {
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let next_step = FakeStrategy::new(
            submitted_messages.clone(),
            submitted_watermarks.clone(),
            false,
        );

        let mut sink = DevNullSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            next_step,
        );

        traced_with_gil!(|py| {
            // Message that matches the route should be discarded
            let matching_message =
                make_raw_routed_msg(py, b"test1".to_vec(), "source", vec!["wp1".to_string()]);
            sink.submit(matching_message).unwrap();

            // Message that doesn't match should be forwarded
            let non_matching_message = make_raw_routed_msg(py, b"test2".to_vec(), "source", vec![]);
            sink.submit(non_matching_message).unwrap();

            sink.join(None).unwrap();

            // Only the non-matching message should have been forwarded
            let messages = submitted_messages.lock().unwrap();
            assert_eq!(messages.len(), 1);
        });
    }

    #[test]
    fn test_devnull_forwards_watermarks() {
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks_clone = submitted_watermarks.clone();
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

        let mut sink = DevNullSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            next_step,
        );

        // Create a watermark message that matches the route
        let watermark_payload = BTreeMap::from([
            (Partition::new(Topic::new("Topic1"), 0), 0),
            (Partition::new(Topic::new("Topic1"), 1), 1),
        ]);
        let watermark_msg = sentry_arroyo::types::Message::new_any_message(
            crate::routes::RoutedValue {
                route: Route::new("source".to_string(), vec!["wp1".to_string()]),
                payload: RoutedValuePayload::make_watermark_payload(watermark_payload.clone(), 0),
            },
            BTreeMap::new(),
        );

        sink.submit(watermark_msg).unwrap();
        sink.join(None).unwrap();

        // Watermark should be forwarded even though the route matches
        let watermarks = submitted_watermarks_clone.lock().unwrap();
        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].committable, watermark_payload);
    }
}
