use std::collections::HashMap;
use std::time::{Duration, Instant};

use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};

use crate::messages::{RoutedValuePayload, WatermarkMessage};
#[cfg(test)]
use crate::mocks::current_epoch;
use crate::routes::RoutedValue;
#[cfg(not(test))]
use crate::time_helpers::current_epoch;

/// Histogram: seconds from watermark `message_time` (or 0 if absent) to commit decision.
const METRIC_WATERMARK_COMMIT_LATENCY: &str = "streams.pipeline.consumer.watermark_commit_latency";

/// Records the committable of a received Watermark and records how many times that watermark has been seen.
#[derive(Clone, Debug)]
struct WatermarkTracker {
    num_watermarks: u64,
    committable: HashMap<Partition, u64>,
    time_added: Instant,
    message_time: Option<f64>,
}

/// WatermarkCommitOffsets is a commit policy that only commits once it receives a copy of a Watermark
/// for each downstream route.
/// The algorithm works like so:
/// - when a Watermark is submitted to the commit step, either add a new WatermarkTracker to the commit step's
///   `watermarks` buffer or increment the existing WatermarkTracker's `num_watermarks` counter
/// - when the commit step is polled, for each WatermarkTracker in the `watermarks` buffer, combine the committable
///   of all watermarks that have `num_watermarks` == `num_branches` and return that as a CommitRequest
/// - if any WatermarkTracker hasn't gotten the required number of Watermark copies in 5 min since the
///   WatermarkTracker was created, delete that WatermarkTracker from the `watermarks` buffer
///   (to prevent an unbounded buffer if one branch of a pipeline breaks and stops forwarding watermarks)
#[derive(Clone, Debug)]
pub struct WatermarkCommitOffsets {
    pub num_branches: u64,
    watermarks: HashMap<u64, WatermarkTracker>,
}

impl WatermarkCommitOffsets {
    pub fn new(num_branches: u64) -> Self {
        WatermarkCommitOffsets {
            watermarks: Default::default(),
            num_branches,
        }
    }

    fn commit(&mut self) -> Option<CommitRequest> {
        // check if there is anything to commit first, since this is much cheaper than getting the
        // current time
        if self.watermarks.is_empty() {
            return None;
        }
        let empty_commit_request = CommitRequest {
            positions: Default::default(),
        };
        let mut to_remove = vec![];
        let mut commit_request = empty_commit_request.clone();
        // Track the oldest (minimum) message_time across all watermarks that contribute
        // to the merged commit, so we record latency once per commit() based on the
        // watermark furthest behind.
        let mut oldest_message_time: Option<f64> = None;
        for (ts, watermark) in self.watermarks.iter() {
            if watermark.num_watermarks == self.num_branches {
                let current_request = CommitRequest {
                    positions: watermark.committable.clone(),
                };
                commit_request =
                    merge_commit_request(Some(commit_request), Some(current_request)).unwrap();

                if let Some(t) = watermark.message_time {
                    oldest_message_time = Some(match oldest_message_time {
                        Some(prev) => prev.min(t),
                        None => t,
                    });
                }

                to_remove.push(ts.clone());
            // Clean up any hanging watermarks which still haven't gotten all their copies in 5 min
            // from when the first copy was seen
            } else if watermark.time_added.elapsed() >= Duration::from_secs(300) {
                to_remove.push(ts.clone());
            }
        }
        for ts in to_remove {
            self.watermarks.remove(&ts);
        }

        if commit_request != empty_commit_request {
            let secs = oldest_message_time
                .map(|t| ((current_epoch() as f64) - t).max(0.0))
                .unwrap_or(0.0);
            metrics::histogram!(METRIC_WATERMARK_COMMIT_LATENCY).record(secs);
            Some(commit_request)
        } else {
            None
        }
    }
}

impl ProcessingStrategy<RoutedValue> for WatermarkCommitOffsets {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit())
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if let RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(watermark)) =
            &message.payload().payload
        {
            match self.watermarks.get(&watermark.timestamp) {
                Some(tracker) => {
                    self.watermarks.insert(
                        watermark.timestamp,
                        WatermarkTracker {
                            num_watermarks: tracker.num_watermarks + 1,
                            committable: tracker.committable.clone(),
                            time_added: tracker.time_added,
                            message_time: tracker.message_time,
                        },
                    );
                }
                None => {
                    let mut committable: HashMap<Partition, u64> = Default::default();
                    for (partition, offset) in message.committable() {
                        committable.insert(partition, offset);
                    }
                    self.watermarks.insert(
                        watermark.timestamp,
                        WatermarkTracker {
                            num_watermarks: 1,
                            committable: committable,
                            time_added: Instant::now(),
                            message_time: watermark.message_time,
                        },
                    );
                }
            };
        }
        Ok(())
    }

    fn terminate(&mut self) {}

    fn join(
        &mut self,
        _: Option<std::time::Duration>,
    ) -> Result<Option<CommitRequest>, StrategyError> {
        Ok(self.commit())
    }
}

#[cfg(test)]
mod tests {
    use crate::messages::Watermark;
    use crate::mocks::set_timestamp;
    use crate::{routes::Route, testutils::make_committable};

    use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Minimal histogram-only recorder modeled on `pipeline_stats::tests::CaptureRecorder`.
    #[derive(Default)]
    struct CaptureRecorder {
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl Recorder for CaptureRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
        fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> metrics::Counter {
            metrics::Counter::noop()
        }
        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::from_arc(Arc::new(CaptureHistogram {
                key: key.clone(),
                histograms: Arc::clone(&self.histograms),
            }))
        }
    }

    struct CaptureHistogram {
        key: Key,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl metrics::HistogramFn for CaptureHistogram {
        fn record(&self, value: f64) {
            self.histograms
                .lock()
                .unwrap()
                .push((self.key.clone(), value));
        }
    }

    #[test]
    fn test_commit_offsets() {
        // Pin current_epoch so the latency metric is deterministic.
        set_timestamp(100);
        let mut commit_step = WatermarkCommitOffsets::new(2);

        let watermark = Watermark::with_message_time(make_committable(3, 0), 0, Some(80.0));
        let mut messages = vec![];
        for waypoint in ["route1", "route2"] {
            messages.push(Message::new_any_message(
                RoutedValue {
                    route: Route {
                        source: "source1".to_string(),
                        waypoints: vec![waypoint.to_string()],
                    },
                    payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(
                        watermark.clone(),
                    )),
                },
                make_committable(3, 0),
            ));
        }

        // First watermark is tracked in commit policy
        let _ = commit_step.submit(messages[0].clone());
        let ts = commit_step.watermarks.keys().next().unwrap().clone();
        assert_eq!(commit_step.watermarks[&ts].num_watermarks, 1);
        if let Ok(Some(req)) = commit_step.poll() {
            panic!(
                "Commit step returned commit request with only 1 out of 2 watermarks: {:?}",
                req
            );
        }

        // Second watermark actually returns CommitRequest on poll() and records the latency
        // metric once based on the (only) tracker's message_time.
        let _ = commit_step.submit(messages[1].clone());
        assert_eq!(commit_step.watermarks[&ts].num_watermarks, 2);

        let histograms = Arc::new(Mutex::new(Vec::<(Key, f64)>::new()));
        let recorder = CaptureRecorder {
            histograms: Arc::clone(&histograms),
        };
        let result = {
            let _guard = metrics::set_default_local_recorder(&recorder);
            commit_step.poll()
        };
        assert!(
            matches!(result, Ok(Some(_))),
            "Commit step returned didn't return CommitRequest with 2 watermarks"
        );

        let recorded: Vec<f64> = histograms
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.name() == METRIC_WATERMARK_COMMIT_LATENCY)
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(
            recorded,
            vec![20.0],
            "expected exactly one latency sample of (current_epoch - message_time)"
        );

        set_timestamp(0);
    }
}
