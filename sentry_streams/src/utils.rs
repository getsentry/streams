use pyo3::Python;
use sentry_arroyo::types::{Message, Partition};
use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};
use tracing::warn;

use crate::routes::RoutedValue;

// Use this wrapper instead of directly using with_gil()
pub fn traced_with_gil<F, R>(label: &str, function: F) -> R
where
    F: FnOnce(Python) -> R,
{
    let thread_id = thread::current().id();
    let start_time = Instant::now();

    let result = Python::with_gil(|py| {
        let acquire_time = Instant::now().duration_since(start_time);

        if acquire_time > Duration::from_secs(1) {
            warn!(
                "[{:?}] Function [{}] Took {:?} to acquire GIL",
                thread_id, label, acquire_time,
            );
        }

        function(py)
    });

    result
}

/// Returns a clone of a message's committable
pub fn clone_committable(message: &Message<RoutedValue>) -> BTreeMap<Partition, u64> {
    let mut committable = BTreeMap::new();
    for (partition, offset) in message.committable() {
        committable.insert(partition, offset);
    }
    committable
}
