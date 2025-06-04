use log::warn;
use pyo3::Python;
use std::thread;
use std::time::{Duration, Instant};

pub fn traced_with_gil<F, R>(label: &str, function: F) -> R
where
    F: FnOnce(Python) -> R,
{
    let thread_id = thread::current().id();
    let start = Instant::now();

    let result = Python::with_gil(|py| {
        let acquire_time = Instant::now().duration_since(start);

        if acquire_time > Duration::from_millis(10) {
            warn!(
                "[{:?}] Function [{}] Took {:?} to acquire GIL",
                thread_id, label, acquire_time,
            );
        }

        function(py)
    });

    result
}
