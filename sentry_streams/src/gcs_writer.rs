use crate::messages::PyStreamingMessage;
use crate::routes::Route;
use crate::routes::RoutedValue;
use crate::utils::traced_with_gil;
use core::panic;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::Python;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use reqwest::ClientBuilder;
use sentry_arroyo::processing::strategies::run_task_in_threads::RunTaskError;
use sentry_arroyo::processing::strategies::run_task_in_threads::RunTaskFunc;
use sentry_arroyo::processing::strategies::run_task_in_threads::TaskRunner;
use sentry_arroyo::types::Message;
pub struct GCSWriter {
    client: Client,
    url: String,
    route: Route,
}

fn pybytes_to_bytes(message: &Message<RoutedValue>, py: Python<'_>) -> PyResult<Vec<u8>> {
    match message.payload().payload {
        PyStreamingMessage::PyAnyMessage { .. } => {
            panic!("Unsupported message type: GCS writers only support RawMessage");
        }
        PyStreamingMessage::RawMessage { ref content } => {
            let payload_content = content.bind(py).getattr("payload").unwrap();
            let py_bytes: &Bound<PyBytes> = payload_content.downcast().unwrap();
            Ok(py_bytes.as_bytes().to_vec())
        }
    }
}

impl GCSWriter {
    pub fn new(bucket: &str, object: &str, route: Route) -> Self {
        let client = ClientBuilder::new();
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            bucket, object
        );

        // TODO: Avoid having this step in the pipeline pick up environment variables.
        // Have a centralized place where all config and env vars are set
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

impl TaskRunner<RoutedValue, RoutedValue, anyhow::Error> for GCSWriter {
    // Async task to write to GCS via HTTP
    fn get_task(&self, message: Message<RoutedValue>) -> RunTaskFunc<RoutedValue, anyhow::Error> {
        let client = self.client.clone();
        let url = self.url.clone();
        let route = message.payload().route.clone();
        let actual_route = self.route.clone();

        let bytes =
            traced_with_gil("GCSWriter get_task", |py| pybytes_to_bytes(&message, py)).unwrap();

        Box::pin(async move {
            // TODO: This route-based forwarding does not need to be
            // run with multiple threads. Look into removing this from the async task.
            if route != actual_route {
                return Ok(message);
            }

            let res: Result<reqwest::Response, reqwest::Error> =
                client.post(&url).body(bytes).send().await;

            let response = res.unwrap();
            let status = response.status();

            if !status.is_success() {
                if status.is_client_error() {
                    let body = response.text().await;
                    panic!(
                        "Client-side error encountered while attempting write to GCS. Status code: {}, Response body: {:?}",
                        status,
                        body
                    )
                } else {
                    Err(RunTaskError::RetryableError)
                }
            } else {
                Ok(message)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::test_operators::make_raw_routed_msg;

    use super::*;

    #[test]
    fn test_to_bytes() {
        pyo3::prepare_freethreaded_python();
        traced_with_gil("test_to_bytes", |py| {
            let arroyo_msg = make_raw_routed_msg(py, b"hello".to_vec(), "source1", vec![]);
            assert_eq!(
                pybytes_to_bytes(&arroyo_msg, py).unwrap(),
                b"hello".to_vec()
            );
        });
    }
}
