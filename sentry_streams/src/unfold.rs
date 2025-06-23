use crate::callers::call_any_python_function;
use crate::routes::{Route, RoutedValue};
use crate::utils::clone_committable;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Unfold's TransformFunction can be either a user-defined Python function
/// (like in FlatMap) or a rust function owned by the platform (like for Broadcast)
pub enum TransformFunction<F> {
    PyFunction(Py<PyAny>),
    RustFunction(F),
}

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

pub struct Unfold<F> {
    pub next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    pub route: Route,
    pub callable: TransformFunction<F>,
    pub pending_messages: HashMap<MessageIdentifier, Message<RoutedValue>>,
}

impl<F> Unfold<F> {
    fn new(
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
        route: Route,
        callable: TransformFunction<F>,
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

impl<F> ProcessingStrategy<RoutedValue> for Unfold<F>
where
    F: FnMut(Message<RoutedValue>) -> Result<Vec<Message<RoutedValue>>, SubmitError<RoutedValue>>
        + Send
        + Sync
        + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }
        let transformed_messages = match self.callable {
            TransformFunction::PyFunction(py_function) => {
                let py_transformed_messages =
                    call_any_python_function(&py_function, message).unwrap();
                py_transformed_messages.extract(py)
            }
        };
        let transformed_messages = (self.callable)(message)?;
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
    use crate::test_operators::build_routed_value;
    use crate::test_operators::make_lambda;
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
        traced_with_gil("test_build_filter", |py| {
            let callable = make_lambda(py, c_str!("lambda x: 'test' in x.payload"));
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_messages_clone = submitted_messages.clone();
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks_clone = submitted_watermarks.clone();
            let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks, false);
        })
    }
}
