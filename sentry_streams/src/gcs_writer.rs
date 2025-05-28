use std::time::Instant;

use crate::routes::Route;
use crate::routes::RoutedValue;
use anyhow::anyhow;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::Python;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use reqwest::ClientBuilder;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};
use sentry_arroyo::types::Message;
pub struct GCSWriter {
    client: Client,
    url: String,
    route: Route,
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
}

fn pybytes_to_bytes(payload: &RoutedValue) -> Vec<u8> {
    let payload = Python::with_gil(|py| {
        let payload = payload.payload.clone_ref(py);
        let py_bytes: &Bound<PyBytes> = payload.bind(py).downcast().unwrap();
        py_bytes.as_bytes().to_vec()
    });
    payload
}

impl TaskRunner<RoutedValue, RoutedValue, anyhow::Error> for GCSWriter {
    fn get_task(&self, message: Message<RoutedValue>) -> RunTaskFunc<RoutedValue, anyhow::Error> {
        let client = self.client.clone();
        let url = self.url.clone();
        let route = message.payload().route.clone();
        let actual_route = self.route.clone();

        Box::pin(async move {
            if route == actual_route {
                let start = Instant::now();
                // This assumes that message.payload() is a Python bytes string
                let bytes = pybytes_to_bytes(message.payload());
                let finish = Instant::now();
                println!("time taken {:?}", finish - start);

                if finish - start > std::time::Duration::from_secs(10) {
                    panic!("We're cooked");
                }

                client
                    .post(&url)
                    .body(bytes)
                    .send()
                    .await
                    .map_err(|e| anyhow!(e))
                    .map_err(RunTaskError::Other)?;

                println!("FINISHED WRITE");
            }
            Ok(message)
        })
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
