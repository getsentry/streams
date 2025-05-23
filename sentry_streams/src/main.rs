use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = "arroyo-artifacts";
    let object = "uploaded-file.txt";

    // Read the access token
    let access_token =
        std::env::var("MY_NEW_TOKEN").expect("Set MY_NEW_TOKEN env variable with your OAuth token");

    let s = String::from("Hello, world!");
    let bytes: Vec<u8> = s.into_bytes();

    let client = Client::new();
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket, object
    );

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
