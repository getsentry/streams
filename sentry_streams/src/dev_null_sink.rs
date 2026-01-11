//! This module provides an implementation of the DevNullSink pipeline
//! step that discards all messages (similar to /dev/null).
//!
//! This sink is useful for testing and benchmarking pipelines where
//! you want to measure throughput without the overhead of actual I/O.
//! It simulates batching behavior with configurable delays.
use crate::routes::{Route, RoutedValue};
use rand::Rng;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::{Duration, Instant};

/// Implements the DevNullSink logic.
///
/// This is an Arroyo strategy that discards messages matching its route
/// and forwards all other messages (including watermarks) to the next step.
/// Simulates batching behavior with configurable delays.
pub struct DevNullSink<N> {
    /// The route this strategy processes. Every message not for this
    /// strategy is sent to the `next` strategy without being processed.
    route: Route,
    next_strategy: N,

    // Batch configuration
    batch_size: Option<usize>,
    batch_time: Option<Duration>,
    average_sleep_time: Option<Duration>,
    max_sleep_time: Option<Duration>,

    // Batch state
    current_batch_size: usize,
    batch_start_time: Instant,
}

impl<N> DevNullSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    pub fn new(
        route: Route,
        next_strategy: N,
        batch_size: Option<usize>,
        batch_time_ms: Option<f64>,
        average_sleep_time_ms: Option<f64>,
        max_sleep_time_ms: Option<f64>,
    ) -> Self {
        DevNullSink {
            route,
            next_strategy,
            batch_size,
            batch_time: batch_time_ms.map(|ms| Duration::from_millis(ms as u64)),
            average_sleep_time: average_sleep_time_ms.map(|ms| Duration::from_millis(ms as u64)),
            max_sleep_time: max_sleep_time_ms.map(|ms| Duration::from_millis(ms as u64)),
            current_batch_size: 0,
            batch_start_time: Instant::now(),
        }
    }

    /// Check if the batch should be flushed based on size or time.
    fn should_flush_batch(&self) -> bool {
        // Check batch size
        if let Some(size) = self.batch_size {
            if self.current_batch_size >= size {
                return true;
            }
        }

        // Check batch time
        if let Some(time) = self.batch_time {
            if self.batch_start_time.elapsed() >= time {
                return true;
            }
        }

        false
    }

    /// Generate a random sleep duration around the average, capped by max.
    fn generate_sleep_duration(&self) -> Option<Duration> {
        if self.average_sleep_time.is_none() {
            return None;
        }

        let average = self.average_sleep_time.unwrap();
        let max = self.max_sleep_time.unwrap_or(average * 2);

        let mut rng = rand::thread_rng();
        // Generate a random value around the average using a triangular distribution
        // This gives us values clustered around the average with a max limit
        let min_factor = 0.5;
        let max_factor = 1.5;
        let factor: f64 = rng.gen_range(min_factor..=max_factor);

        let sleep_duration = average.mul_f64(factor);

        // Cap at max_sleep_time
        Some(sleep_duration.min(max))
    }

    /// Reset the batch state.
    fn reset_batch(&mut self) {
        self.current_batch_size = 0;
        self.batch_start_time = Instant::now();
    }
}

impl<N> ProcessingStrategy<RoutedValue> for DevNullSink<N>
where
    N: ProcessingStrategy<RoutedValue> + 'static,
{
    /// Polls the next strategy and checks if batch should be flushed.
    /// If batch is full, sleeps for a random duration to simulate processing.
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        // Check if we should flush the batch
        if self.should_flush_batch() && self.current_batch_size > 0 {
            // Simulate processing time with random sleep
            if let Some(sleep_duration) = self.generate_sleep_duration() {
                std::thread::sleep(sleep_duration);
            }
            self.reset_batch();
        }

        self.next_strategy.poll()
    }

    /// Submit the message. If the route matches and it's not a watermark,
    /// discard it and increment batch counter. Otherwise, forward to the next strategy.
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        // Always forward watermarks to ensure proper pipeline operation
        if message.payload().payload.is_watermark_msg() {
            return self.next_strategy.submit(message);
        }

        // If the route matches, discard the message and increment batch counter
        if self.route == message.payload().route {
            self.current_batch_size += 1;
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
            None, // batch_size
            None, // batch_time_ms
            None, // average_sleep_time_ms
            None, // max_sleep_time_ms
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
            None, // batch_size
            None, // batch_time_ms
            None, // average_sleep_time_ms
            None, // max_sleep_time_ms
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

    #[test]
    fn test_devnull_batch_by_size() {
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

        let mut sink = DevNullSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            next_step,
            Some(3),   // batch_size
            None,      // batch_time_ms
            Some(1.0), // average_sleep_time_ms (1ms)
            Some(2.0), // max_sleep_time_ms (2ms)
        );

        traced_with_gil!(|py| {
            // Submit messages that match the route
            for _ in 0..3 {
                let message =
                    make_raw_routed_msg(py, b"test".to_vec(), "source", vec!["wp1".to_string()]);
                sink.submit(message).unwrap();
            }

            // Batch counter should be 3
            assert_eq!(sink.current_batch_size, 3);

            // Poll should flush the batch (with sleep)
            sink.poll().unwrap();

            // Batch should be reset
            assert_eq!(sink.current_batch_size, 0);

            // Should have slept (sleep was called)
            // Note: We can't reliably assert on timing in tests due to system variance
        });
    }

    #[test]
    fn test_devnull_batch_by_time() {
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

        let mut sink = DevNullSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            next_step,
            None,       // batch_size
            Some(10.0), // batch_time_ms (10ms)
            Some(1.0),  // average_sleep_time_ms (1ms)
            Some(2.0),  // max_sleep_time_ms (2ms)
        );

        traced_with_gil!(|py| {
            // Submit one message
            let message =
                make_raw_routed_msg(py, b"test".to_vec(), "source", vec!["wp1".to_string()]);
            sink.submit(message).unwrap();

            assert_eq!(sink.current_batch_size, 1);

            // Wait for batch time to expire
            std::thread::sleep(Duration::from_millis(15));

            // Poll should flush the batch
            sink.poll().unwrap();

            // Batch should be reset
            assert_eq!(sink.current_batch_size, 0);
        });
    }

    #[test]
    fn test_devnull_no_batch_config() {
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(RawMutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(RawMutex::new(Vec::new()));
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);

        let mut sink = DevNullSink::new(
            Route::new("source".to_string(), vec!["wp1".to_string()]),
            next_step,
            None, // batch_size
            None, // batch_time_ms
            None, // average_sleep_time_ms
            None, // max_sleep_time_ms
        );

        traced_with_gil!(|py| {
            // Submit messages
            for _ in 0..10 {
                let message =
                    make_raw_routed_msg(py, b"test".to_vec(), "source", vec!["wp1".to_string()]);
                sink.submit(message).unwrap();
            }

            // Messages should be counted but never flushed (no batch config)
            assert_eq!(sink.current_batch_size, 10);

            // Poll should not flush
            sink.poll().unwrap();
            assert_eq!(sink.current_batch_size, 10);
        });
    }
}
