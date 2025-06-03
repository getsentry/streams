use std::time::Instant;

use pyo3::Python;

pub fn traced_with_gil<F, R>(label: &str, f: F) -> R
where
    F: FnOnce(Python) -> R,
{
    let thread_id = std::thread::current().id();
    println!("[{:?}] [{}] Attempting to acquire GIL...", thread_id, label);
    let start = Instant::now();

    // We're getting stuck here
    let result = Python::with_gil(|py| {
        println!(
            "[{:?}] [{}] Acquired GIL after {:?}",
            thread_id,
            label,
            Instant::now().duration_since(start)
        );

        f(py)
    });

    println!("[{:?}] [{}] Released GIL", thread_id, label);
    result
}
