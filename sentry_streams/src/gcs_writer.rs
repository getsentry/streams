use crate::routes::Route;
use crate::routes::RoutedValue;
use anyhow::anyhow;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::Python;
use reqwest::blocking::Client;
use reqwest::blocking::ClientBuilder;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Response;
use sentry_arroyo::processing::strategies::run_task_in_threads::RunTaskError;
use sentry_arroyo::processing::strategies::MessageRejected;
use sentry_arroyo::processing::strategies::SubmitError;
use sentry_arroyo::types::Message;
pub struct GCSWriter {
    client: Client,
    url: String,
    route: Route,
}

fn pybytes_to_bytes(payload: &RoutedValue) -> Vec<u8> {
    let payload = Python::with_gil(|py| {
        let payload = payload.payload.clone_ref(py);
        let py_bytes: &Bound<PyBytes> = payload.bind(py).downcast().unwrap();
        py_bytes.as_bytes().to_vec()
    });
    payload
}

impl GCSWriter {
    pub fn new(bucket: &str, object: &str, route: Route) -> Self {
        let client = ClientBuilder::new();
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            bucket, object
        );

        let access_token = std::env::var("GCP_ACCESS_TOKEN")
            .expect("Set GCP_ACCESS_TOKEN env variable with GCP authorization token");

        let mut headers = HeaderMap::with_capacity(2);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", access_token)).unwrap(),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/octet-stream").unwrap(),
        );

        let client = client.default_headers(headers).build().unwrap();

        GCSWriter { client, url, route }
    }

    pub fn get_task(
        &self,
        message: Message<RoutedValue>,
    ) -> Result<Message<RoutedValue>, SubmitError<RoutedValue>> {
        let client = self.client.clone();
        let url = self.url.clone();
        // This assumes that message.payload() is a Python bytes string
        let bytes = pybytes_to_bytes(message.payload());

        let res = client.post(&url).body(bytes).send();

        match res {
            Ok(_) => Ok(message),
            _ => Err(SubmitError::MessageRejected(MessageRejected { message })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        let route = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
        let payload = Python::with_gil(|py| PyBytes::new(py, b"hello").into());

        let routed_value = RoutedValue { route, payload };

        let bytes = pybytes_to_bytes(&routed_value);

        assert_eq!(bytes, b"hello".to_vec());
    }
}
