use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightServiceClient::connect("http://localhost:50051").await?;

    // Define a flight request (example: fetching data by ticket)
    let request = tonic::Request::new(Ticket {
        ticket: tonic::codegen::Bytes::from("example_ticket"),
    });

    // Send the request and receive the response stream
    let mut response_stream = client.do_get(request).await?.into_inner();

    let ctx = SessionContext::new();

    // Process incoming Flight data
    while let Some(response) = response_stream.message().await? {
        let schema = Schema::new(vec![
            Field::new("string_column", DataType::Utf8, false),
            Field::new("int_column", DataType::Int32, false),
        ]);
        let sch_ref = Arc::new(schema);
        let ret = flight_data_to_arrow_batch(&response, sch_ref.clone(), &HashMap::new());

        if ret.is_err() {
            print!("Error {}", ret.unwrap_err());
            continue;
        }
        let table = MemTable::try_new(sch_ref.clone(), vec![vec![ret.unwrap()]])?;
        ctx.register_table("people", Arc::new(table))?;
        let df = ctx
            .table("people")
            .await?
            .filter(col("int_column").gt(lit(1)))?; // Filtering rows where id > 2

        // Collect and display the results
        let results = df.collect().await?;
        for batch in results {
            println!("{:?}", batch);
        }
    }

    Ok(())
}
