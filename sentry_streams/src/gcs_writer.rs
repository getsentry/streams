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
use reqwest::StatusCode;
use sentry_arroyo::processing::strategies::run_task_in_threads::RunTaskError;
use sentry_arroyo::processing::strategies::run_task_in_threads::RunTaskFunc;
use sentry_arroyo::processing::strategies::run_task_in_threads::TaskRunner;
use sentry_arroyo::types::Message;

use gcp_auth::{provider, TokenProvider};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

const METRIC_SINK_GCS_WRITER_BYTES: &str = "streams.pipeline.sink.gcs_writer.bytes";

/// Max attempts for a single GCS upload (initial try + retries).
const GCS_UPLOAD_MAX_ATTEMPTS: u32 = 5;
const GCS_UPLOAD_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const GCS_UPLOAD_MAX_BACKOFF: Duration = Duration::from_secs(30);

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
            let py_bytes: &Bound<PyBytes> = payload_content.cast().unwrap();
            Ok(py_bytes.as_bytes().to_vec())
        }
    }
}

/// Exponential backoff for attempt `n` (0-based failure index), capped at max.
fn gcs_upload_backoff(failure_index: u32) -> Duration {
    let shift = failure_index.min(16);
    let backoff = GCS_UPLOAD_INITIAL_BACKOFF.saturating_mul(1u32 << shift);
    backoff.min(GCS_UPLOAD_MAX_BACKOFF)
}

/// Client (4xx) errors are permanent. Everything else is treated as transient.
fn is_permanent_http_status(status: StatusCode) -> bool {
    status.is_client_error()
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

        let pybytes_start = std::time::Instant::now();
        let bytes: Vec<u8> = match message.payload().payload {
            RoutedValuePayload::PyStreamingMessage(ref py_message) => {
                traced_with_gil!(|py| pybytes_to_bytes(py_message, py)).unwrap()
            }
            RoutedValuePayload::WatermarkMessage(..) => {
                return Box::pin(async move { Ok(message) });
            }
        };
        let pybytes_ms = pybytes_start.elapsed().as_millis();

        let bytes_len = bytes.len();
        let route_source = self.route.source.clone();

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

            let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
            let mut last_error: Option<String> = None;

            for attempt in 1..=GCS_UPLOAD_MAX_ATTEMPTS {
                // Get a fresh token (gcp_auth caches and only refreshes when expired).
                // Token fetch failures are transient and retried with backoff.
                let token_start = std::time::Instant::now();
                let token = match auth_provider.token(scopes).await {
                    Ok(token) => token,
                    Err(e) => {
                        let err = format!("Failed to obtain token: {:?}", e);
                        tracing::warn!(
                            "{}, attempt {}/{}",
                            err,
                            attempt,
                            GCS_UPLOAD_MAX_ATTEMPTS
                        );
                        last_error = Some(err);
                        if attempt == GCS_UPLOAD_MAX_ATTEMPTS {
                            break;
                        }
                        tokio::time::sleep(gcs_upload_backoff(attempt - 1)).await;
                        continue;
                    }
                };
                let token_ms = token_start.elapsed().as_millis();

                let request_start = std::time::Instant::now();
                let response = match client
                    .post(&url)
                    .header(
                        AUTHORIZATION,
                        HeaderValue::from_str(&format!("Bearer {}", token.as_str())).unwrap(),
                    )
                    .body(bytes.clone())
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(e) => {
                        let err = format!("Failed to send request: {:?}", e);
                        tracing::warn!(
                            "{}, attempt {}/{}",
                            err,
                            attempt,
                            GCS_UPLOAD_MAX_ATTEMPTS
                        );
                        last_error = Some(err);
                        if attempt == GCS_UPLOAD_MAX_ATTEMPTS {
                            break;
                        }
                        tokio::time::sleep(gcs_upload_backoff(attempt - 1)).await;
                        continue;
                    }
                };
                let request_ms = request_start.elapsed().as_millis();

                let status = response.status();
                if status.is_success() {
                    tracing::info!(
                        "Finished writing file to GCS bucket: {}, file name: {}",
                        bucket_str,
                        object_name
                    );
                    tracing::info!(
                        "Length of bytes successfully written: {} (pybytes_to_bytes_ms={}, token_ms={}, request_ms={}, attempts={})",
                        bytes_len,
                        pybytes_ms,
                        token_ms,
                        request_ms,
                        attempt
                    );
                    let gcs_labels = vec![("source".to_string(), route_source.clone())];
                    metrics::histogram!(METRIC_SINK_GCS_WRITER_BYTES, &gcs_labels)
                        .record(bytes_len as f64);
                    return Ok(message);
                }

                if is_permanent_http_status(status) {
                    let body = response.text().await;
                    // Permanent client errors must crash the consumer so offsets are not committed.
                    return Err(RunTaskError::Other(anyhow::anyhow!(
                        "Fatal error encountered while attempting write to GCS. Status code: {}, Response body: {:?}",
                        status,
                        body
                    )));
                }

                let err = format!(
                    "Transient error encountered while attempting write to GCS. Status code: {}",
                    status
                );
                tracing::warn!("{}, attempt {}/{}", err, attempt, GCS_UPLOAD_MAX_ATTEMPTS);
                last_error = Some(err);
                if attempt == GCS_UPLOAD_MAX_ATTEMPTS {
                    break;
                }
                tokio::time::sleep(gcs_upload_backoff(attempt - 1)).await;
            }

            // Exhausted retries: permanent error so arroyo crashes the consumer and does not
            // commit offsets past the failed parquet batch.
            Err(RunTaskError::Other(anyhow::anyhow!(
                "GCS write failed after {} attempts for bucket {}, object {}: {}",
                GCS_UPLOAD_MAX_ATTEMPTS,
                bucket_str,
                object_name,
                last_error.unwrap_or_else(|| "unknown error".to_string())
            )))
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

    #[test]
    fn test_gcs_upload_backoff_doubles_and_caps() {
        assert_eq!(gcs_upload_backoff(0), Duration::from_millis(500));
        assert_eq!(gcs_upload_backoff(1), Duration::from_secs(1));
        assert_eq!(gcs_upload_backoff(2), Duration::from_secs(2));
        assert_eq!(gcs_upload_backoff(3), Duration::from_secs(4));
        assert_eq!(gcs_upload_backoff(10), GCS_UPLOAD_MAX_BACKOFF);
        assert_eq!(gcs_upload_backoff(100), GCS_UPLOAD_MAX_BACKOFF);
    }

    #[test]
    fn test_permanent_http_status_classification() {
        assert!(is_permanent_http_status(StatusCode::BAD_REQUEST));
        assert!(is_permanent_http_status(StatusCode::UNAUTHORIZED));
        assert!(is_permanent_http_status(StatusCode::FORBIDDEN));
        assert!(is_permanent_http_status(StatusCode::NOT_FOUND));
        assert!(is_permanent_http_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_permanent_http_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!is_permanent_http_status(StatusCode::BAD_GATEWAY));
        assert!(!is_permanent_http_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_permanent_http_status(StatusCode::GATEWAY_TIMEOUT));
    }
}
