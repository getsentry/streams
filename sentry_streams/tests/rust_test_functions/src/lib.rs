use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

// Import the macros from rust_streams
use rust_streams::{rust_filter_function, rust_map_function, Message};

/// Test data structure for type validation tests
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestMessage {
    pub id: u64,
    pub content: String,
}

rust_filter_function!(TestFilterCorrect, TestMessage, |msg: Message<
    TestMessage,
>|
 -> bool {
    msg.payload.id > 0
});

rust_map_function!(TestMapCorrect, TestMessage, String, |msg: Message<
    TestMessage,
>|
 -> Message<String> {
    Message::new(
        format!("Processed: {}", msg.payload.content),
        msg.headers,
        msg.timestamp,
        msg.schema,
    )
});

// Wrong type map function - accepts bool instead of TestMessage
rust_map_function!(
    TestMapWrongType,
    bool, // This expects bool, but will get TestMessage in the test
    String,
    |msg: Message<bool>| -> Message<String> {
        Message::new(
            if msg.payload {
                "true".to_string()
            } else {
                "false".to_string()
            },
            msg.headers,
            msg.timestamp,
            msg.schema,
        )
    }
);

// Map that accepts String (for testing chained operations)
rust_map_function!(
    TestMapString,
    String,
    u64,
    |msg: Message<String>| -> Message<u64> {
        Message::new(
            msg.payload.len() as u64,
            msg.headers,
            msg.timestamp,
            msg.schema,
        )
    }
);

/// PyO3 module definition
#[pymodule]
fn rust_test_functions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TestFilterCorrect>()?;
    m.add_class::<TestMapCorrect>()?;
    m.add_class::<TestMapWrongType>()?;
    m.add_class::<TestMapString>()?;
    Ok(())
}
