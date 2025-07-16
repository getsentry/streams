use crate::committable::clone_committable;
use crate::routes::{Route, RoutedValue};
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
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

    /// Attempts to re-submit a pending message, if successful deletes it from the pending buffer.
    fn retry_pending_message(
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
                self.retry_pending_message(msg.clone(), &identifier)
            {
                self.pending_messages.remove(&identifier);
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Attempts to submit a message to the next step, if backpressure
    /// is raised then adds the message to the pending buffer.
    fn submit_to_next_step(
        &mut self,
        message: Message<RoutedValue>,
        identifier: MessageIdentifier,
    ) -> Result<(), SubmitError<RoutedValue>> {
        let msg_clone = message.clone();
        match self.next_step.submit(message) {
            Ok(..) => Ok(()),
            Err(e) => {
                self.pending_messages.insert(identifier, msg_clone);
                Err(e)
            }
        }
    }

    // If the message is in the pending buffer, attempts to submit it to the
    /// next step
    fn handle_submit(
        &mut self,
        message: Message<RoutedValue>,
    ) -> Result<(), SubmitError<RoutedValue>> {
        let identifier = MessageIdentifier {
            route: message.payload().route.clone(),
            committable: clone_committable(&message),
        };
        if self.pending_messages.contains_key(&identifier) {
            self.retry_pending_message(message, &identifier)
        } else {
            self.submit_to_next_step(message, identifier)
        }
    }
}

impl ProcessingStrategy<RoutedValue> for Broadcaster {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.flush_pending()?;
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }
        let unfolded_messages = generate_broadcast_messages(&self.downstream_branches, message);
        for msg in unfolded_messages {
            self.handle_submit(msg)?;
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
    use crate::fake_strategy::FakeStrategy;
    use crate::fake_strategy::{assert_messages_match, assert_watermarks_match};
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
                    source: String::from("source"),
                    waypoints: vec![],
                },
                vec![String::from("branch1"), String::from("branch2")],
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
                        source: String::from("source"),
                        waypoints: vec![],
                    },
                    payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(
                        Watermark::new(make_committable(2, 0), 0),
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
            assert_messages_match(
                py,
                vec![
                    "test_message".into_py_any(py).unwrap(),
                    "test_message".into_py_any(py).unwrap(),
                ],
                actual_messages.deref(),
            );

            // Assert poll clears remaining pending messages
            let _ = step.poll();
            assert_eq!(step.pending_messages.len(), 0);
            let actual_watermarks = submitted_watermarks_clone.lock().unwrap();
            assert_watermarks_match(
                vec![Watermark::new(make_committable(2, 0), 0)],
                actual_watermarks.deref(),
            );
        })
    }
}
