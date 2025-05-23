use anyhow::anyhow;
use pyo3::types::PyAnyMethods;
use pyo3::types::PyBytes;
use pyo3::Python;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client;
use sentry_arroyo::processing::strategies::run_task_in_threads::{
    RunTaskError, RunTaskFunc, TaskRunner,
};

use crate::routes::RoutedValue;
use pyo3::prelude::*;
use sentry_arroyo::types::Message;
pub struct GCSWriter {}

impl GCSWriter {
    pub fn new() {}
}

fn to_bytes(payload: &RoutedValue) -> Vec<u8> {
    let payload = Python::with_gil(|py| {
        let payload = payload.payload.clone_ref(py);
        // if payload.is_none(py) {
        // prolly need to handle this case

        let py_bytes: &Bound<PyBytes> = payload.bind(py).downcast().unwrap();
        py_bytes.as_bytes().to_vec()
    });
    payload
}

// start by supporting bytes only
impl TaskRunner<RoutedValue, (), anyhow::Error> for GCSWriter {
    fn get_task(&self, message: Message<RoutedValue>) -> RunTaskFunc<(), anyhow::Error> {
        Box::pin(async move {
            let bucket = "arroyo-artifacts";
            let object = "uploaded-file.txt";

            // Read the access token
            let access_token = std::env::var("GCP_ACCESS_TOKEN")
                .expect("Set GCP_ACCESS_TOKEN env variable with GCP authorization token");

            let bytes = to_bytes(message.payload());

            let client = Client::new();
            let url = format!(
                "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                bucket, object
            );

            client
                .post(&url)
                .header(AUTHORIZATION, format!("Bearer {}", access_token))
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(bytes)
                .send()
                .await
                .map_err(|e| anyhow!(e))
                .map_err(RunTaskError::Other)?;

            Ok(message.replace(()))
        })
    }
}
