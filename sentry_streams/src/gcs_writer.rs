use crate::messages::PyStreamingMessage;
use crate::messages::RoutedValuePayload;
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

use gcp_auth::{provider, TokenProvider};
use std::sync::Arc;
use tokio::sync::OnceCell;

pub struct GCSWriter {
    client: Client,
    bucket: String,
    route: Route,
    object_generator: Py<PyAny>,
    auth_provider: Arc<OnceCell<Arc<dyn TokenProvider>>>,
}

fn pybytes_to_bytes(message: &PyStreamingMessage, py: Python<'_>) -> PyResult<Vec<u8>> {
    match message {
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
    pub fn new(bucket: &str, object_generator: Py<PyAny>, route: Route) -> Self {
        // Build a simple client with just Content-Type header
        // Authorization header will be added per-request with fresh token
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let client = ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();

        GCSWriter {
            client,
            bucket: bucket.to_string(),
            route,
            object_generator,
            auth_provider: Arc::new(OnceCell::new()),
        }
    }
}

fn object_gen_fn(object_generator: Py<PyAny>, py: Python<'_>) -> PyResult<String> {
    let res: Py<PyAny> = object_generator.call0(py)?;
    res.extract(py)
}

impl TaskRunner<RoutedValue, RoutedValue, anyhow::Error> for GCSWriter {
    // Async task to write to GCS via HTTP
    fn get_task(&self, message: Message<RoutedValue>) -> RunTaskFunc<RoutedValue, anyhow::Error> {
        let client = self.client.clone();
        let object =
            traced_with_gil!(|py| { object_gen_fn(self.object_generator.clone_ref(py), py) })
                .unwrap();
        let object_name = object.clone();

        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.bucket.clone(),
            object
        );
        let bucket_str = format!("{}", self.bucket);

        let route = message.payload().route.clone();
        let actual_route = self.route.clone();

        let bytes: Vec<u8> = match message.payload().payload {
            RoutedValuePayload::PyStreamingMessage(ref py_message) => {
                traced_with_gil!(|py| pybytes_to_bytes(py_message, py)).unwrap()
            }
            RoutedValuePayload::WatermarkMessage(..) => {
                return Box::pin(async move { Ok(message) });
            }
        };

        let bytes_len = bytes.len();

        let auth_provider_cell = self.auth_provider.clone();

        Box::pin(async move {
            // TODO: This route-based forwarding does not need to be
            // run with multiple threads. Look into removing this from the async task.
            if route != actual_route {
                return Ok(message);
            }

            // Lazily initialize the auth provider on first use. Since it is async, it may call
            // external services, so we don't want it to block initialization. If we fail to get an
            // auth provider the error is fatal and should stop the pipeline.
            let auth_provider = auth_provider_cell
                .get_or_init(|| async {
                    provider().await.expect("Failed to get gcp_auth provider")
                })
                .await;

            // Get a fresh token (gcp_auth caches and only refreshes when expired)
            // If getting a token fails we should be able to retry.
            let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
            let token = auth_provider.token(scopes).await.map_err(|e| {
                tracing::warn!("Failed to obtain token: {:?}", e);
                RunTaskError::RetryableError
            })?;

            let response = client
                .post(&url)
                .header(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {}", token.as_str())).unwrap(),
                )
                .body(bytes)
                .send()
                .await
                .map_err(|e| {
                    tracing::warn!("Failed to send request: {:?}", e);
                    RunTaskError::RetryableError
                })?;

            let status = response.status();
            if !status.is_success() {
                if status.is_client_error() {
                    let body = response.text().await;
                    panic!(
                        "Fatal error encountered while attempting write to GCS. Status code: {}, Response body: {:?}",
                        status,
                        body
                    )
                } else {
                    tracing::warn!(
                        "Transient error encountered while attempting write to GCS. Status code: {}",
                        status,
                    );
                    Err(RunTaskError::RetryableError)
                }
            } else {
                tracing::info!(
                    "Finished writing file to GCS bucket: {}, file name: {}",
                    bucket_str,
                    object_name
                );
                tracing::info!("Length of bytes successfully written: {}", bytes_len);
                Ok(message)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::testutils::make_raw_routed_msg;

    use super::*;

    #[test]
    fn test_to_bytes() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let arroyo_msg = make_raw_routed_msg(py, b"hello".to_vec(), "source1", vec![]);
            assert_eq!(
                pybytes_to_bytes(arroyo_msg.payload().payload.unwrap_payload(), py).unwrap(),
                b"hello".to_vec()
            );
        });
    }
}
