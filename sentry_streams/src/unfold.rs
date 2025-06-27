use crate::callers::{try_apply_py, ApplyError, ApplyResult};
use crate::messages::RoutedValuePayload;
use crate::routes::{Route, RoutedValue};
use crate::committable::clone_committable;
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use pyo3::types::PyList;
use sentry_arroyo::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy, StrategyError, SubmitError
};
use sentry_arroyo::types::{Message, Partition};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// MessageIdentifier is used to uniquely identify a routed message copy
/// when it is stored in pending_messages after previously returning MessageRejected
#[derive(PartialEq, Debug)]
pub struct MessageIdentifier {
    route: Route,
    committable: BTreeMap<Partition, u64>,
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

pub struct Unfold {
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
    pub callable: Py<PyAny>,
    pub pending_messages: HashMap<MessageIdentifier, Message<RoutedValue>>,
}

impl Unfold {
    fn new(
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
        callable: Py<PyAny>,
    ) -> Self {
        Self {
            next_step,
            route,
            callable,
            pending_messages: HashMap::new(),
        }
    }

    fn retry_pending_message(
        &mut self,
        message: Message<RoutedValue>,
        identifier: MessageIdentifier,
    ) -> Result<(), SubmitError<RoutedValue>> {
        self.next_step.submit(message)?;
        self.pending_messages.remove(&identifier);
        Ok(())
    }

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

    fn handle_submit(
        &mut self,
        message: Message<RoutedValue>,
    ) -> Result<(), SubmitError<RoutedValue>> {
        let identifier = MessageIdentifier {
            route: message.payload().route.clone(),
            committable: clone_committable(&message),
        };
        if self.pending_messages.contains_key(&identifier) {
            self.retry_pending_message(message, identifier)
        } else {
            self.submit_to_next_step(message, identifier)
        }
    }
}

impl ProcessingStrategy<RoutedValue> for Unfold {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }
        let py_message: Py<PyAny> = (&message.payload().payload).into();
        let transformed_messages = traced_with_gil!(|py| {
            let res = try_apply_py(py, &self.callable, (py_message,))
                    .and_then(|py_res| py_res.extract::<Vec<RoutedValuePayload>>(py).map_err(|_| ApplyError::ApplyFailed));

            //         ApplyResult::Ok(msgs) => {
            //             msgs.extract()
            //         },
            //         ApplyResult::Err(e) => {
            //             // TODO: handle errors
            //             match e {
            //                 ApplyError::InvalidMessage => return SubmitError::MessageRejected(MessageRejected { message }),
            //                 ApplyError::ApplyFailed => return SubmitError::InvalidMessage(InvalidMessage { message }),
            //             }
            //         }
            //     }
            });
        // let transformed_messages = (self.callable)(message)?;
        for msg in transformed_messages {
            self.handle_submit(msg)?;
        }
        Ok(())
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::assert_messages_match;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::WatermarkMessage;
    use crate::routes::Route;
    use crate::transformer::build_filter;
    use crate::utils::traced_with_gil;
    use pyo3::ffi::c_str;
    use pyo3::IntoPyObjectExt;
    use sentry_arroyo::processing::strategies::ProcessingStrategy;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_submit() {
        pyo3::prepare_freethreaded_python();
        traced_with_gil!(|py| {
            let callable = make_lambda(py, c_str!("lambda x: 'test' in x.payload"));
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);
        })
    }
}
