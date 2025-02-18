use arrow::array::as_string_array;
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::Ticket;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::Volatility;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature};
use datafusion::prelude::*;
use serde_json;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Parser {
    signature: Signature,
}

impl Parser {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Parser {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "parser"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        assert_eq!(args.len(), 1);
        let value = &args[0];
        assert_eq!(value.data_type(), DataType::Utf8);

        let args = ColumnarValue::values_to_arrays(args)?;
        let value = as_string_array(&args[0]);

        let mut ids = Vec::new();
        let mut names = Vec::new();
        let mut values = Vec::new();

        for i in 0..value.len() {
            if value.is_null(i) {
                continue;
            }

            let json_str = value.value(i);
            if let Ok(parsed) = serde_json::from_str::<Value>(json_str) {
                ids.push(parsed.get("id").and_then(|v| v.as_i64()));
                names.push(
                    parsed
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                );
                values.push(parsed.get("value").and_then(|v| v.as_f64()));
            }
        }

        let id_array = Arc::new(Int64Array::from(ids)) as ArrayRef;
        let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
        let value_array = Arc::new(Int64Array::from(values)) as ArrayRef;

        let struct_fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ];

        Ok(ColumnarValue::from(Arc::new(StructArray::from(vec![
            (struct_fields[0].clone(), id_array),
            (struct_fields[1].clone(), name_array),
            (struct_fields[2].clone(), value_array),
        ])) as ArrayRef))
    }

    fn output_ordering(
        &self,
        input: &[ExprProperties],
    ) -> datafusion::error::Result<SortProperties> {
        Ok(input[0].sort_properties)
    }
}

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

    let parse_udf = ScalarUDF::from(Parser::new());

    // Process incoming Flight data
    while let Some(response) = response_stream.message().await? {
        let schema = Schema::new(vec![
            Field::new("offset", DataType::Int32, false),
            Field::new("message", DataType::Utf8, false),
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
            .filter(col("offset").gt(lit(5)))?; // Filtering rows where id > 2

        // Collect and display the results
        let results = df.collect().await?;
        for batch in results {
            println!("{:?}", batch);
        }
    }

    Ok(())
}
