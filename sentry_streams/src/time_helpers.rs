use std::time::{SystemTime, UNIX_EPOCH};

// Needed as rust_analyzer doesn't play nice with conditional compilation
#[allow(dead_code)]
/// Returns the current Unix epoch
pub fn current_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
