use crate::executor::{ExecutorMessage, ExecutorSender};
use rpc::gateway::db_server::Db;
use rpc::gateway::{Command, CommandResult};
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

pub struct GatewayService {
    executor_tx: ExecutorSender,
}

impl GatewayService {
    pub fn new(executor_tx: ExecutorSender) -> Self {
        Self { executor_tx }
    }
}

#[tonic::async_trait]
impl Db for GatewayService {
    type ConnectExecutorStream = Pin<Box<dyn Stream<Item = Result<CommandResult, Status>> + Send>>;

    async fn connect_executor(
        &self,
        request: Request<Streaming<Command>>,
    ) -> Result<Response<Self::ConnectExecutorStream>, Status> {
        let stream = request.into_inner();
        let (result_tx, result_rx) = mpsc::channel(100);

        // Forward the incoming stream to executor.
        self.executor_tx
            .send(ExecutorMessage::NewClient { stream, result_tx })
            .map_err(|_| Status::internal("executor not available"))?;

        debug!("Gateway: forwarded new client to executor");
        Ok(Response::new(Box::pin(ReceiverStream::new(result_rx))))
    }
}
