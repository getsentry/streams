use reqwest::blocking::ClientBuilder;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

// Just a way to test out writes to GCS
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = "arroyo-artifacts";
    let object = "uploaded-file.txt";

    let client = ClientBuilder::new();
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket, object
    );

    let access_token = std::env::var("MY_NEW_TOKEN")
        .expect("Set MY_NEW_TOKEN env variable with GCP authorization token");

    let mut headers = HeaderMap::with_capacity(2);
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", access_token)).unwrap(),
    );
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/octet-stream").unwrap(),
    );

    // got client
    let client = client.default_headers(headers).build().unwrap();

    let bytes = String::from("Hello world").into_bytes();

    let res = client
        .post(&url)
        .header(AUTHORIZATION, format!("Bearer {}", access_token))
        .header(CONTENT_TYPE, "application/octet-stream")
        .body(bytes)
        .send()?;

    println!("Status: {}", res.status());
    println!("Response: {}", res.text()?);

    Ok(())
}
