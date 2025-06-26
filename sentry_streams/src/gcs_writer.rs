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
pub struct GCSWriter {
    client: Client,
    bucket: String,
    route: Route,
    object_generator: Py<PyAny>,
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
        let client = ClientBuilder::new();

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
        let bucket = bucket.to_string();

        GCSWriter {
            client,
            bucket,
            route,
            object_generator,
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

        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.bucket.clone(),
            object
        );

        let route = message.payload().route.clone();
        let actual_route = self.route.clone();

        let bytes = match message.payload().payload {
            RoutedValuePayload::PyStreamingMessage(ref py_message) => {
                traced_with_gil!(|py| pybytes_to_bytes(py_message, py)).unwrap()
            }
            RoutedValuePayload::WatermarkMessage(..) => {
                return Box::pin(async move { Ok(message) })
            }
        };

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
