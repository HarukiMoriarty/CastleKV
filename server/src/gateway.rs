use crate::executor::{ExecutorMessage, ExecutorSender};
use rpc::gateway::db_server::Db;
use rpc::gateway::{Command, CommandResult};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

/// Gateway service that forwards client requests to the executor
pub struct GatewayService {
    /// Channel to send messages to the executor
    executor_tx: ExecutorSender,
}

impl GatewayService {
    /// Create a new GatewayService
    pub fn new(executor_tx: ExecutorSender) -> Self {
        Self { executor_tx }
    }
}

#[tonic::async_trait]
impl Db for GatewayService {
    /// Stream type for responses to client commands
    type ConnectExecutorStream = Pin<Box<dyn Stream<Item = Result<CommandResult, Status>> + Send>>;

    /// Connects a client to the executor service
    ///
    /// # Arguments
    ///
    /// * `request` - Contains a stream of commands from the client
    ///
    /// # Returns
    ///
    /// * Stream of command results back to the client
    async fn connect_executor(
        &self,
        request: Request<Streaming<Command>>,
    ) -> Result<Response<Self::ConnectExecutorStream>, Status> {
        // Extract the incoming command stream
        let stream = request.into_inner();

        // Create a channel for sending results back to the client
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        // Forward the incoming stream to executor
        self.executor_tx
            .send(ExecutorMessage::NewClient { stream, result_tx })
            .map_err(|_| Status::internal("Executor service not available"))?;

        debug!("Gateway: forwarded new client connection to executor");

        // Return the result stream to the client
        Ok(Response::new(Box::pin(UnboundedReceiverStream::new(
            result_rx,
        ))))
    }
}
