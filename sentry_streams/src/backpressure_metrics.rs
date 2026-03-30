//! Backpressure metrics: counters and episode-duration histograms for
//! `MessageRejected` (send vs receive). Incomplete episodes at shutdown are dropped.

use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::Message;
use std::time::{Duration, Instant};

use crate::routes::RoutedValue;

const PREFIX: &str = "streams.pipeline";

/// Tracks a single backpressure episode for histogram sampling.
/// Episodes still open when the consumer stops are not recorded.
#[derive(Debug, Default)]
pub struct EpisodeTracker {
    start: Option<Instant>,
}

impl EpisodeTracker {
    /// Start or continue an episode (timer begins on first rejection in a run).
    pub fn on_message_rejected(&mut self) {
        if self.start.is_none() {
            self.start = Some(Instant::now());
        }
    }

    /// If an episode was active, record one histogram sample (milliseconds) and clear.
    pub fn on_success(&mut self, step: &str, duration_metric: &str) {
        if let Some(start) = self.start.take() {
            let elapsed_ms = duration_ms(start.elapsed());
            let key = format!("{}.{}", PREFIX, duration_metric);
            let labels = vec![("step".to_string(), step.to_string())];
            metrics::histogram!(key, &labels).record(elapsed_ms);
        }
    }
}

fn duration_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn counter_increment(key: String, step: &str) {
    let labels = vec![("step".to_string(), step.to_string())];
    metrics::counter!(key, &labels).increment(1);
}

/// Increment `send_backpressure` and extend the send-duration episode tracker.
pub fn record_send_rejected(step: &str, tracker: &mut EpisodeTracker) {
    counter_increment(format!("{}.send_backpressure", PREFIX), step);
    tracker.on_message_rejected();
}

/// Increment `receive_backpressure` and extend the receive-duration episode tracker.
pub fn record_rcvd_rejected(step: &str, tracker: &mut EpisodeTracker) {
    counter_increment(format!("{}.receive_backpressure", PREFIX), step);
    tracker.on_message_rejected();
}

/// End a send backpressure episode after a successful submit path.
pub fn send_on_success(step: &str, tracker: &mut EpisodeTracker) {
    tracker.on_success(step, "send_backpressure_duration");
}

/// End a receive backpressure episode after downstream accepted a submit.
pub fn recv_on_success(step: &str, tracker: &mut EpisodeTracker) {
    tracker.on_success(step, "receive_backpressure_duration");
}

/// Wraps the downstream step and records receive-side backpressure when `inner.submit` returns
/// `MessageRejected`.
pub struct BackpressureNext {
    inner: Box<dyn ProcessingStrategy<RoutedValue>>,
    step_label: String,
    recv_tracker: EpisodeTracker,
}

impl BackpressureNext {
    pub fn new(inner: Box<dyn ProcessingStrategy<RoutedValue>>, step_label: String) -> Self {
        Self {
            inner,
            step_label,
            recv_tracker: EpisodeTracker::default(),
        }
    }
}

impl ProcessingStrategy<RoutedValue> for BackpressureNext {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
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

    #[test]
    fn episode_consecutive_rejections_one_histogram_on_success() {
        let mut t = EpisodeTracker::default();
        t.on_message_rejected();
        std::thread::sleep(Duration::from_millis(2));
        t.on_message_rejected();
        // No panic; second rejection does not restart timer
        assert!(t.start.is_some());
        t.on_success("step_a", "receive_backpressure_duration");
        assert!(t.start.is_none());
    }

    #[test]
    fn episode_success_without_rejection_no_histogram() {
        let mut t = EpisodeTracker::default();
        t.on_success("step_b", "send_backpressure_duration");
        assert!(t.start.is_none());
    }
}
