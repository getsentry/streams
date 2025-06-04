use pyo3::Python;
use std::thread;
use std::time::{Duration, Instant};
use tracing::warn;

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
