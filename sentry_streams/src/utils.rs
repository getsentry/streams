/// Use this wrapper instead of directly using `with_gil()`.
/// This wrapper automatically emits a warning trace when the time to acquire
/// the python GIL is higher than some threshold (defaults to 1 second).
/// The optional warn_threshold value can be supplied in the first position to
/// change that.
/// The function syntax is exactly the same as `pyo3::Python::with_gil`.
///
///
/// # Examples
///
/// ```
/// let py_err = ...;
/// traced_with_gil!(|py| { py_err.print(py) }).unwrap();
///
/// let py_err = ...;
/// traced_with_gil!(Duration::from_secs(10), |py| {
///     py_err.print(py)
/// }).unwrap();
/// ```
///
macro_rules! traced_with_gil {
    ($function: expr) => {
        traced_with_gil!(std::time::Duration::from_secs(1), $function)
    };

    ($warn_threshold: expr, $function: expr) => {{
        crate::utils::__traced_with_gil(
            &format!("{}:{}:{}", file!(), line!(), column!()),
            $warn_threshold,
            $function,
        )
    }};
}

pub(crate) use traced_with_gil;

use pyo3::Python;
use std::{
    thread,
    time::{Duration, Instant},
};
use tracing::warn;

#[doc(hidden)]
pub(crate) fn __traced_with_gil<F, R>(label: &str, warn_threshold: Duration, function: F) -> R
where
    F: FnOnce(Python) -> R,
{
    let thread_id = thread::current().id();
    let start_time = Instant::now();

    Python::with_gil(|py| {
        let acquire_time = Instant::now().duration_since(start_time);

        if acquire_time > warn_threshold {
            warn!(
                "{} with {:?} took {:?} to acquire GIL",
                label, thread_id, acquire_time,
            );
        }

        function(py)
    })
}
