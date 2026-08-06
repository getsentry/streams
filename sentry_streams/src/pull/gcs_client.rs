use std::sync::Arc;

use gcp_auth::{provider, TokenProvider};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, ClientBuilder};
use tokio::sync::OnceCell;

/// Client for uploading bytes to Google Cloud Storage via the JSON API.
///
/// Handles authentication (lazily initialized via gcp_auth), token refresh,
/// and HTTP upload. Inject this into handlers/stages that need GCS access.
pub struct GcsClient {
    client: Client,
    bucket: String,
    auth_provider: Arc<OnceCell<Arc<dyn TokenProvider>>>,
}

impl GcsClient {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            auth_provider: Arc::new(OnceCell::new()),
        }
    }

    /// Create a client with default reqwest settings.
    pub fn with_defaults(bucket: impl Into<String>) -> Self {
        let mut headers = HeaderMap::with_capacity(1);
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let client = ClientBuilder::new()
            .default_headers(headers)
            .build()
            .expect("Failed to build reqwest client");

        Self::new(client, bucket)
    }

    /// Upload bytes to GCS as the given object name.
    pub async fn upload(
        &self,
        object_name: &str,
        bytes: &[u8],
    ) -> Result<(), GcsError> {
        let auth_provider = self
            .auth_provider
            .get_or_init(|| async {
                provider().await.expect("Failed to get gcp_auth provider")
            })
            .await;

        let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
        let token = auth_provider
            .token(scopes)
            .await
            .map_err(|e| GcsError::Auth(format!("{e}")))?;

        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.bucket, object_name,
        );

        let response = self
            .client
            .post(&url)
            .header(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token.as_str())).unwrap(),
            )
            .body(bytes.to_vec())
            .send()
            .await
            .map_err(|e| GcsError::Network(format!("{e}")))?;

        let status = response.status();
        if status.is_success() {
            tracing::info!(
                "GCS upload complete: bucket={}, object={}, bytes={}",
                self.bucket,
                object_name,
                bytes.len(),
            );
            metrics::histogram!("streams.pipeline.sink.gcs_writer.bytes")
                .record(bytes.len() as f64);
            Ok(())
        } else if status.is_client_error() {
            let body = response.text().await.unwrap_or_default();
            Err(GcsError::ClientError(format!(
                "status={status}, body={body}"
            )))
        } else {
            Err(GcsError::ServerError(format!("status={status}")))
        }
    }
}

#[derive(Debug)]
pub enum GcsError {
    /// Failed to obtain auth token.
    Auth(String),
    /// Network/transport error (retryable).
    Network(String),
    /// 4xx — fatal, bad request.
    ClientError(String),
    /// 5xx — retryable server error.
    ServerError(String),
}

impl std::fmt::Display for GcsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcsError::Auth(msg) => write!(f, "GCS auth error: {msg}"),
            GcsError::Network(msg) => write!(f, "GCS network error: {msg}"),
            GcsError::ClientError(msg) => write!(f, "GCS client error: {msg}"),
            GcsError::ServerError(msg) => write!(f, "GCS server error: {msg}"),
        }
    }
}

impl std::error::Error for GcsError {}
