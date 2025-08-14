use std::cell::Cell;

thread_local! {
    static TIMESTAMP: Cell<u64> = const { Cell::new(0) };
}

// Needed as rust_analyzer doesn't play nice with conditional compilation
#[allow(dead_code)]
pub fn set_timestamp(timestamp: u64) {
    TIMESTAMP.with(|ts| ts.set(timestamp));
}

// Needed as rust_analyzer doesn't play nice with conditional compilation
#[allow(dead_code)]
/// Used to mock current_epoch in tests. Returns a fixed timestamp set via set_timestamp().
pub fn current_epoch() -> u64 {
    TIMESTAMP.with(|timestamp| timestamp.get())
}
