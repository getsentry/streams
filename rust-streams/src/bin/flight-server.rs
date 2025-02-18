use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream;
use futures::stream::BoxStream;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

fn build_batch() -> RecordBatch {
    let messages = vec![
        Some("{\"id\":1,\"name\":\"Item1\",\"value\":10}"),
        Some("{\"id\":2,\"name\":\"Item2\",\"value\":20}"),
        Some("{\"id\":3,\"name\":\"Item3\",\"value\":30}"),
        Some("{\"id\":4,\"name\":\"Item4\",\"value\":40}"),
        Some("{\"id\":5,\"name\":\"Item5\",\"value\":50}"),
        Some("{\"id\":6,\"name\":\"Item6\",\"value\":60}"),
        Some("{\"id\":7,\"name\":\"Item7\",\"value\":70}"),
        Some("{\"id\":8,\"name\":\"Item8\",\"value\":80}"),
        Some("{\"id\":9,\"name\":\"Item9\",\"value\":90}"),
    ];

    let int_values = vec![
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
    ];

    let string_array = StringArray::from(messages);
    let int_array = Int32Array::from(int_values);

    let schema = Schema::new(vec![
        Field::new("offset", DataType::Int32, false),
        Field::new("message", DataType::Utf8, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(int_array) as Arc<dyn arrow::array::Array>,
            Arc::new(string_array) as Arc<dyn arrow::array::Array>,
        ],
    )
    .unwrap()
}

type FlightStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

#[derive(Clone)]
pub struct FlightServiceImpl {}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        println!("Received request: {:?}", _request.get_ref());

        let table = build_batch();
        let flight_data: Vec<Result<FlightData, Status>> =
            batches_to_flight_data(table.schema().as_ref(), vec![table])
                .unwrap()
                .into_iter()
                .map(Ok)
                .collect();
        println!("Number of flight data items: {}", flight_data.len());
        let stream = stream::iter(flight_data);
        Ok(Response::new(Box::pin(stream) as FlightStream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    println!("Starting server on address: {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
