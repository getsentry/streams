//! This module tracks backpressure that can raise during the processing
//! of messages through the pipeline.
//! Every step in arroyo can raise a MessageRejected error to signal backpressure
//! to the previoius step in the pipeline.
//! Tracking this event is critical to understand the bottleneck in the pipeline.
//!
//! Here we track these events by wrapping a strategy and producing counters and
//! event-duration histograms to track backpressure.

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::{Duration, Instant};

use crate::metrics::{record_counter, record_histogram};
use crate::routes::RoutedValue;

/// Tracks a single backpressure event for histogram sampling.
/// Events still open when the consumer stops are not recorded.
#[derive(Debug, Default)]
pub struct BackpressureTracker {
    start: Option<Instant>,
}

impl BackpressureTracker {
    /// Start or continue an event (timer begins on first rejection in a run).
    pub fn on_message_rejected(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
        }
    }

    /// If an event was active, record one histogram sample (milliseconds) and clear.
    pub fn on_success(&mut self, step: &str, duration_metric: &str) {
        if let Some(start) = self.start.take() {
            let elapsed_ms = duration_ms(start.elapsed());
            record_histogram(duration_metric, &[("step", step)], elapsed_ms);
        }
    }
}

fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Increment `send_backpressure` and extend the send-duration event tracker.
pub fn record_send_rejected(step: &str, tracker: &mut BackpressureTracker) {
    record_counter("send_backpressure", &[("step", step)], 1);
    tracker.on_message_rejected();
}

/// Increment `receive_backpressure` and extend the receive-duration event tracker.
pub fn record_rcvd_rejected(step: &str, tracker: &mut BackpressureTracker) {
    record_counter("receive_backpressure", &[("step", step)], 1);
    tracker.on_message_rejected();
}

/// End a send backpressure event after a successful submit path.
pub fn send_on_success(step: &str, tracker: &mut BackpressureTracker) {
    tracker.on_success(step, "send_backpressure_duration");
}

/// End a receive backpressure event after downstream accepted a submit.
pub fn recv_on_success(step: &str, tracker: &mut BackpressureTracker) {
    tracker.on_success(step, "receive_backpressure_duration");
}

/// Wraps the downstream step and records receive-side backpressure when `inner.submit` returns
/// `MessageRejected`.
pub struct BackpressureNext {
    inner: Box<dyn ProcessingStrategy<RoutedValue>>,
    step_label: String,
    recv_tracker: BackpressureTracker,
}

impl BackpressureNext {
    pub fn new(inner: Box<dyn ProcessingStrategy<RoutedValue>>, step_label: String) -> Self {
        Self {
            inner,
            step_label,
            recv_tracker: BackpressureTracker::default(),
        }
    }
}

impl ProcessingStrategy<RoutedValue> for BackpressureNext {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        // `SubmitError::MessageRejected` is only returned from `submit`, not from `poll`.
        // The wrapped `next` may still see backpressure when the inner strategy calls
        // `submit` during its own `poll` (e.g. watermark flush, retrying pending messages);
        // those calls go through `BackpressureNext::submit` and are instrumented there.
        self.inner.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        match self.inner.submit(message) {
            Ok(()) => {
                recv_on_success(&self.step_label, &mut self.recv_tracker);
                Ok(())
            }
            Err(SubmitError::MessageRejected(mr)) => {
                record_rcvd_rejected(&self.step_label, &mut self.recv_tracker);
                Err(SubmitError::MessageRejected(mr))
            }
            Err(e) => Err(e),
        }
    }

    fn terminate(&mut self) {
        self.inner.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.inner.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::RoutedValuePayload;
    use crate::metrics::{init_streams_recorder_for_tests, PIPELINE_METRIC_PREFIX};
    use crate::routes::{Route, RoutedValue};
    use chrono::Utc;
    use metrics::{with_local_recorder, Key, Label};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use sentry_arroyo::types::{Message, Partition, Topic};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[test]
    fn backpressure_next_increments_receive_backpressure_when_inner_rejects() {
        init_streams_recorder_for_tests();
        let debug = DebuggingRecorder::new();
        let snapshotter = debug.snapshotter();
        let step_label = "upstream_map";

        let routed = RoutedValue {
            route: Route::new("src".to_string(), vec![]),
            payload: RoutedValuePayload::make_watermark_payload(BTreeMap::new(), 1),
        };
        let msg =
            Message::new_broker_message(routed, Partition::new(Topic::new("t"), 0), 1, Utc::now());

        let fake = FakeStrategy::new(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
            true,
        );
        let mut next = BackpressureNext::new(Box::new(fake), step_label.to_string());

        with_local_recorder(&debug, || match next.submit(msg) {
            Err(SubmitError::MessageRejected(_)) => {}
            other => panic!("expected MessageRejected, got {other:?}"),
        });

        let key = CompositeKey::new(
            MetricKind::Counter,
            Key::from_parts(
                format!("{PIPELINE_METRIC_PREFIX}.receive_backpressure"),
                vec![Label::new("step", step_label)],
            ),
        );
        let map = snapshotter.snapshot().into_hashmap();
        assert_eq!(map.get(&key), Some(&(None, None, DebugValue::Counter(1))));
    }

    #[test]
    fn event_consecutive_rejections_one_histogram_on_success() {
        let mut t = BackpressureTracker::default();
        t.on_message_rejected();
        std::thread::sleep(Duration::from_millis(2));
        t.on_message_rejected();
        // No panic; second rejection does not restart timer
        assert!(t.start.is_some());
        t.on_success("step_a", "receive_backpressure_duration");
        assert!(t.start.is_none());
    }

    #[test]
    fn event_success_without_rejection_no_histogram() {
        let mut t = BackpressureTracker::default();
        t.on_success("step_b", "send_backpressure_duration");
        assert!(t.start.is_none());
    }
}
