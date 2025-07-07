#[allow(unused_imports)]
use serde::{Deserialize, Serialize};

/// Generic Message struct for use with Rust callbacks
/// This matches the PyMessage structure but with a generic payload
#[derive(Debug, Clone)]
pub struct Message<T> {
    pub payload: T,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: f64,
    pub schema: Option<String>,
}

impl<T> Message<T> {
    pub fn new(
        payload: T,
        headers: Vec<(String, Vec<u8>)>,
        timestamp: f64,
        schema: Option<String>,
    ) -> Self {
        Self {
            payload,
            headers,
            timestamp,
            schema,
        }
    }

    /// Transform the message payload while keeping metadata
    pub fn map<U, F>(self, f: F) -> Message<U>
    where
        F: FnOnce(T) -> U,
    {
        Message {
            payload: f(self.payload),
            headers: self.headers,
            timestamp: self.timestamp,
            schema: self.schema,
        }
    }
}

/// Convert a boxed message to an opaque pointer for FFI
pub fn message_to_ptr<T>(msg: Message<T>) -> *const Message<T> {
    Box::into_raw(Box::new(msg))
}

/// Convert an opaque pointer back to a boxed message for FFI
pub unsafe fn ptr_to_message<T>(ptr: *const Message<T>) -> Message<T> {
    *Box::from_raw(ptr as *mut Message<T>)
}

/// Free a message pointer (used for cleanup)
pub unsafe fn free_message_ptr<T>(ptr: *const Message<T>) {
    if !ptr.is_null() {
        let _ = Box::from_raw(ptr as *mut Message<T>);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestPayload {
        id: u64,
        name: String,
    }

    #[test]
    fn test_message_creation() {
        let payload = TestPayload {
            id: 42,
            name: "test".to_string(),
        };

        let headers = vec![("content-type".to_string(), b"application/json".to_vec())];

        let msg = Message::new(
            payload.clone(),
            headers.clone(),
            1234567890.5,
            Some("test_schema".to_string()),
        );

        assert_eq!(msg.payload.id, 42);
        assert_eq!(msg.payload.name, "test");
        assert_eq!(msg.headers, headers);
        assert_eq!(msg.timestamp, 1234567890.5);
        assert_eq!(msg.schema, Some("test_schema".to_string()));
    }

    #[test]
    fn test_message_map() {
        let original = Message::new(
            TestPayload {
                id: 1,
                name: "input".to_string(),
            },
            vec![],
            0.0,
            None,
        );

        let transformed = original.map(|payload| TestPayload {
            id: payload.id * 2,
            name: format!("transformed_{}", payload.name),
        });

        assert_eq!(transformed.payload.id, 2);
        assert_eq!(transformed.payload.name, "transformed_input");
    }

    #[test]
    fn test_ptr_roundtrip() {
        let original = Message::new(
            TestPayload {
                id: 99,
                name: "ptr_test".to_string(),
            },
            vec![("header".to_string(), b"value".to_vec())],
            123.456,
            Some("schema".to_string()),
        );

        // Convert to pointer and back
        let ptr = message_to_ptr(original.clone());
        let recovered = unsafe { ptr_to_message(ptr) };

        assert_eq!(original.payload.id, recovered.payload.id);
        assert_eq!(original.payload.name, recovered.payload.name);
        assert_eq!(original.headers, recovered.headers);
        assert_eq!(original.timestamp, recovered.timestamp);
        assert_eq!(original.schema, recovered.schema);
    }
}
