use std::collections::HashMap;
use std::time::{Duration, Instant};

use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};

use crate::messages::{RoutedValuePayload, WatermarkMessage};
use crate::routes::RoutedValue;

/// Records the committable of a received Watermark and records how many times that watermark has been seen.
#[derive(Clone, Debug)]
struct WatermarkTracker {
    num_watermarks: u64,
    committable: HashMap<Partition, u64>,
    time_added: Instant,
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
        for (ts, watermark) in self.watermarks.iter() {
            if watermark.num_watermarks == self.num_branches {
                let current_request = CommitRequest {
                    positions: watermark.committable.clone(),
                };
                commit_request =
                    merge_commit_request(Some(commit_request), Some(current_request)).unwrap();
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
                            time_added: tracker.time_added.clone(),
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
    use crate::{messages::Watermark, routes::Route, testutils::make_committable};

    use super::*;

    #[test]
    fn test_commit_offsets() {
        let mut commit_step = WatermarkCommitOffsets::new(2);

        let watermark = Watermark::new(make_committable(3, 0), 0);
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

        // Second watermark actually returns CommitRequest on poll()
        let _ = commit_step.submit(messages[1].clone());
        assert_eq!(commit_step.watermarks[&ts].num_watermarks, 2);
        if let Ok(None) = commit_step.poll() {
            panic!("Commit step returned didn't return CommitRequest with 2 watermarks");
        }
    }
}
