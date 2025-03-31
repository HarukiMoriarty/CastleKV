use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::Stream;
use tonic::codec::Streaming;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use rpc::raft::{
    raft_server::Raft, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};

/// Events from RPC server to LogManager
#[derive(Debug)]
pub enum RaftRequest {
    /// Received AppendEntries request
    AppendEntriesRequest {
        request: AppendEntriesRequest,
        oneshot_tx: oneshot::Sender<AppendEntriesResponse>,
    },
    /// Received RequestVote request
    RequestVoteRequest {
        request: RequestVoteRequest,
        oneshot_tx: oneshot::Sender<RequestVoteResponse>,
    },
}

pub struct RaftService {
    /// Channel to forward raft request to log manager
    raft_request_tx: mpsc::UnboundedSender<RaftRequest>,
}

impl RaftService {
    pub fn new(raft_request_tx: mpsc::UnboundedSender<RaftRequest>) -> Self {
        Self { raft_request_tx }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    type AppendEntriesStream =
        Pin<Box<dyn Stream<Item = Result<AppendEntriesResponse, Status>> + Send>>;

    type RequestVoteStream =
        Pin<Box<dyn Stream<Item = Result<RequestVoteResponse, Status>> + Send>>;

    async fn append_entries(
        &self,
        request: Request<Streaming<AppendEntriesRequest>>,
    ) -> Result<Response<Self::AppendEntriesStream>, Status> {
        let mut stream = request.into_inner();
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let raft_request_tx = self.raft_request_tx.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                let (oneshot_tx, oneshot_rx) = oneshot::channel();

                // Forward to LogManager
                if let Err(e) = raft_request_tx.send(RaftRequest::AppendEntriesRequest {
                    request: req,
                    oneshot_tx,
                }) {
                    error!("Failed to forward AppendEntries request: {}", e);
                    break;
                }

                // Wait for response from LogManager
                match oneshot_rx.await {
                    Ok(response) => {
                        if let Err(e) = resp_tx.send(Ok(response)) {
                            error!("Failed to send AppendEntries response: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to receive response from LogManager: {}", e);
                        let _ = resp_tx.send(Err(Status::internal("Internal error")));
                        break;
                    }
                }
            }

            info!("Raft AppendEntries service done");
        });

        // Return the response stream
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(resp_rx),
        )))
    }

    async fn request_vote(
        &self,
        request: Request<Streaming<RequestVoteRequest>>,
    ) -> Result<Response<Self::RequestVoteStream>, Status> {
        let mut stream = request.into_inner();
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let raft_request_tx = self.raft_request_tx.clone();

        tokio::spawn(async move {
            while let Ok(Some(req)) = stream.message().await {
                // Create channel for getting a response
                let (oneshot_tx, oneshot_rx) = oneshot::channel();

                // Forward to LogManager
                if let Err(e) = raft_request_tx.send(RaftRequest::RequestVoteRequest {
                    request: req,
                    oneshot_tx,
                }) {
                    error!("Failed to forward RequestVote request: {}", e);
                    break;
                }

                // Wait for response from LogManager
                match oneshot_rx.await {
                    Ok(response) => {
                        if let Err(e) = resp_tx.send(Ok(response)) {
                            error!("Failed to send RequestVote response: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to receive response from LogManager: {}", e);
                        let _ = resp_tx.send(Err(Status::internal("Internal error")));
                        break;
                    }
                }
            }

            info!("Raft RequestVote service done");
        });

        // Return the response stream
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::UnboundedReceiverStream::new(resp_rx),
        )))
    }
}
