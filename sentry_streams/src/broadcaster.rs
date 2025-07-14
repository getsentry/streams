use crate::committable::clone_committable;
use crate::routes::{Route, RoutedValue};
use sentry_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// MessageIdentifier is used to uniquely identify a routed message copy
/// when it is stored in pending_messages after previously returning MessageRejected
#[derive(Clone, Debug, PartialEq)]
pub struct MessageIdentifier {
    pub route: Route,
    pub committable: BTreeMap<Partition, u64>,
}

impl Eq for MessageIdentifier {}

impl Hash for MessageIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.route.waypoints.hash(state);
        for (k, v) in &self.committable {
            k.hash(state);
            v.hash(state);
        }
    }
}

/// Takes a message and a list of downstream routes,
/// returns a Vec of message copies each corresponding to a downstream route.
pub fn generate_broadcast_messages(
    downstream_branches: &Vec<String>,
    message: Message<RoutedValue>,
) -> Vec<Message<RoutedValue>> {
    let mut res = Vec::new();
    let branches = downstream_branches.clone();
    for branch in branches {
        let clone = message.payload().clone();
        let routed_clone = clone.add_waypoint(branch);
        let committable = clone_committable(&message);
        let routed_message = Message::new_any_message(routed_clone, committable);
        res.push(routed_message);
    }
    res
}

pub struct Broadcaster {
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
    pub pending_messages: HashMap<MessageIdentifier, Message<RoutedValue>>,
    pub downstream_branches: Vec<String>,
}

impl Broadcaster {
    pub fn new(
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
        downstream_branches: Vec<String>,
    ) -> Self {
        Self {
            next_step,
            route,
            downstream_branches,
            pending_messages: HashMap::new(),
        }
    }

    /// Attempts to submit a pending message, if successful deletes it from the pending buffer.
    fn submit_pending_message(
        &mut self,
        message: Message<RoutedValue>,
        identifier: &MessageIdentifier,
    ) -> Result<(), SubmitError<RoutedValue>> {
        self.next_step.submit(message)?;
        self.pending_messages.remove(identifier);
        Ok(())
    }

    /// Attempts to re-submit all pending messages.
    fn flush_pending(&mut self) -> Result<(), StrategyError> {
        let ids: Vec<MessageIdentifier> = self.pending_messages.keys().cloned().collect();
        for identifier in ids {
            let msg = self.pending_messages.get(&identifier).unwrap();
            // we only need to take action here if the returned error is `InvalidMessage`
            if let Err(SubmitError::InvalidMessage(e)) =
                self.submit_pending_message(msg.clone(), &identifier)
            {
                self.pending_messages.remove(&identifier);
                return Err(e.into());
            }
        }
        Ok(())
    }
}

impl ProcessingStrategy<RoutedValue> for Broadcaster {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.flush_pending()?;
        self.next_step.poll()
    }

    /// Instead of submitting messages to the next step, `submit` puts
    /// the routed message clones into an in-memory buffer that gets flushed on `poll` and `join`.
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        // To preserve ordering we block new messages even if they're not routed for this branch
        if !self.pending_messages.is_empty() {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }

        let unfolded_messages = generate_broadcast_messages(&self.downstream_branches, message);
        for msg in unfolded_messages {
            let msg_key = MessageIdentifier {
                route: msg.payload().route.clone(),
                committable: clone_committable(&msg),
            };
            self.pending_messages.insert(msg_key, msg);
        }
        Ok(())
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.flush_pending()?;
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::{
        assert_messages_match, assert_routes_match, assert_watermarks_match, submitted_payloads,
        submitted_watermark_payloads,
    };
    use crate::fake_strategy::{submitted_routes, FakeStrategy};
    use crate::messages::{RoutedValuePayload, Watermark, WatermarkMessage};
    use crate::routes::Route;
    use crate::testutils::{build_routed_value, make_committable};
    use crate::utils::traced_with_gil;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};
    use std::vec;

    #[test]
    fn test_message_rejected() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(
                submitted_messages.clone(),
                submitted_watermarks.clone(),
                true,
            );
            let mut step = Broadcaster::new(
                Box::new(next_step),
                Route {
                    source: "source".to_string(),
                    waypoints: vec![],
                },
                vec!["branch1".to_string(), "branch2".to_string()],
            );

            // Assert MessageRejected adds message to pending
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source",
                    vec![],
                ),
                make_committable(1, 0),
            );
            let _ = step.submit(message.clone());
            assert_eq!(step.pending_messages.len(), 1);

            // Assert MessageRejected adds Watermark to pending
            let watermark = Message::new_any_message(
                RoutedValue {
                    route: Route {
                        source: "source".to_string(),
                        waypoints: vec![],
                    },
                    payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(
                        Watermark::new(make_committable(2, 0)),
                    )),
                },
                make_committable(2, 0),
            );
            let _ = step.submit(watermark);
            assert_eq!(step.pending_messages.len(), 2);

            // Assert pending message gets sent on repeat submit
            step.next_step = Box::new(FakeStrategy::new(
                submitted_messages,
                submitted_watermarks,
                false,
            ));
            let message = Message::new_any_message(
                build_routed_value(
                    py,
                    "test_message".into_py_any(py).unwrap(),
                    "source",
                    vec![],
                ),
                make_committable(1, 0),
            );
            let _ = step.submit(message.clone());
            assert_eq!(step.pending_messages.len(), 1);
            let actual_messages = submitted_messages_clone.lock().unwrap();
            let actual_payloads = submitted_payloads(actual_messages.deref());
            assert_messages_match(
                py,
                vec![
                    "test_message".into_py_any(py).unwrap(),
                    "test_message".into_py_any(py).unwrap(),
                ],
                &actual_payloads,
            );
            let actual_routes = submitted_routes(actual_messages.deref());
            assert_routes_match(
                vec![
                    Route {
                        source: "source".to_string(),
                        waypoints: vec!["branch1".to_string()],
                    },
                    Route {
                        source: "source".to_string(),
                        waypoints: vec!["branch2".to_string()],
                    },
                ],
                &actual_routes,
            );

            // Assert poll clears remaining pending messages
            let _ = step.poll();
            assert_eq!(step.pending_messages.len(), 0);
            let actual_watermarks = submitted_watermarks_clone.lock().unwrap();
            let watermark_payloads = submitted_watermark_payloads(actual_watermarks.deref());
            assert_watermarks_match(
                vec![Watermark::new(make_committable(2, 0))],
                &watermark_payloads,
            );
            let actual_routes = submitted_routes(actual_watermarks.deref());
            assert_routes_match(
                vec![
                    Route {
                        source: "source".to_string(),
                        waypoints: vec!["branch1".to_string()],
                    },
                    Route {
                        source: "source".to_string(),
                        waypoints: vec!["branch2".to_string()],
                    },
                ],
                &actual_routes,
            );
        })
    }
}
