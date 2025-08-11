use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Import the macros from rust_streams
use rust_streams::{convert_via_json, rust_function, Message};

/// Test data structure for type validation tests
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestMessage {
    pub id: u64,
    pub content: String,
}

// Implement type conversion for TestMessage
convert_via_json!(TestMessage);

rust_function!(TestFilterCorrect, TestMessage, bool, |msg: Message<
    TestMessage,
>|
 -> bool {
    let (payload, _) = msg.take();
    payload.id > 0
});

rust_function!(TestMapCorrect, TestMessage, String, |msg: Message<
    TestMessage,
>|
 -> String {
    let (payload, _) = msg.take();
    format!("Processed: {}", payload.content)
});

// Wrong type map function - accepts bool instead of TestMessage
rust_function!(
    TestMapWrongType,
    bool, // This expects bool, but will get TestMessage in the test
    String,
    |msg: Message<bool>| -> String {
        let (payload, _) = msg.take();
        if payload {
            "true".to_string()
        } else {
            "false".to_string()
        }
    }
);

// Map that accepts String (for testing chained operations)
rust_function!(TestMapString, String, u64, |msg: Message<String>| -> u64 {
    let (payload, _) = msg.take();
    payload.len() as u64
});

/// PyO3 module definition
#[pymodule]
fn rust_test_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TestFilterCorrect>()?;
    m.add_class::<TestMapCorrect>()?;
    m.add_class::<TestMapWrongType>()?;
    m.add_class::<TestMapString>()?;
    Ok(())
}
