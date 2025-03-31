use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Status};
use tracing::{debug, error, info};

use rpc::raft::{
    raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};

/// Manages RPC client connections to other Raft nodes
pub struct RaftSession {
    /// Map of peer nodes (node_id -> address)
    peer_addresses: HashMap<u32, String>,
    /// Map of connected clients
    sessions: HashMap<u32, RaftClient<tonic::transport::Channel>>,
}

impl RaftSession {
    /// Create a new RaftSession and connect to all peers
    pub async fn new(peer_addresses: HashMap<u32, String>) -> Self {
        let mut raft_session = Self {
            peer_addresses: peer_addresses.clone(),
            sessions: HashMap::new(),
        };

        // Try connecting to all peers during initialization
        for (peer_id, addr) in peer_addresses {
            if let Err(e) = raft_session.connect_to_peer(peer_id, addr).await {
                error!("Initial connection to peer {} failed: {}", peer_id, e);
            }
        }

        raft_session
    }

    /// Establish connection to a specific peer
    async fn connect_to_peer(
        &mut self,
        peer_id: u32,
        addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to peer {} at {}", peer_id, addr);

        let addr = if !addr.starts_with("http") {
            format!("http://{}", addr)
        } else {
            addr
        };

        match RaftClient::connect(addr).await {
            Ok(client) => {
                info!("Connected to peer {}", peer_id);
                self.sessions.insert(peer_id, client);
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to peer {}: {}", peer_id, e);
                Err(Box::new(e))
            }
        }
    }

    /// Send AppendEntries RPC to a single peer
    pub async fn send_append_entries(
        &mut self,
        peer_id: u32,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Status> {
        if let Some(session) = self.sessions.get_mut(&peer_id) {
            debug!("Sending AppendEntries to peer {}", peer_id);

            let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);
            let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);

            match session.append_entries(Request::new(req_stream)).await {
                Ok(response) => {
                    let mut stream = response.into_inner();

                    if let Err(e) = req_tx.send(request).await {
                        error!("Failed to send AppendEntries request: {}", e);
                        self.sessions.remove(&peer_id);
                        return Err(Status::internal("Failed to send request"));
                    }

                    match stream.message().await {
                        Ok(Some(resp)) => Ok(resp),
                        Ok(None) => Err(Status::internal("Empty response")),
                        Err(e) => {
                            error!("Error receiving AppendEntries response: {}", e);
                            self.sessions.remove(&peer_id);
                            Err(e)
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to start AppendEntries stream: {}", e);
                    self.sessions.remove(&peer_id);
                    Err(e)
                }
            }
        } else {
            Err(Status::unavailable("Peer not connected"))
        }
    }

    /// Broadcast AppendEntries to all peers and wait for majority success
    pub async fn broadcast_append_entries(
        session: Arc<Mutex<RaftSession>>,
        request: AppendEntriesRequest,
    ) -> Result<Vec<(u32, AppendEntriesResponse)>, Status> {
        let peer_ids = {
            let s = session.lock().await;
            s.peer_addresses.keys().copied().collect::<Vec<_>>()
        };

        let majority = peer_ids.len() / 2 + 1;
        let mut futures = FuturesUnordered::new();

        for peer_id in peer_ids {
            let req = request.clone();
            let session = Arc::clone(&session);

            futures.push(tokio::spawn(async move {
                let mut s = session.lock().await;
                let result = s.send_append_entries(peer_id, req).await;
                (peer_id, result)
            }));
        }

        let mut responses = Vec::new();
        let mut success_count = 0;

        while let Some(result) = futures.next().await {
            if let Ok((peer_id, Ok(resp))) = result {
                responses.push((peer_id, resp));
                success_count += 1;

                if success_count >= majority {
                    break;
                }
            } else if let Ok((peer_id, Err(e))) = result {
                error!("AppendEntries failed to peer {}: {}", peer_id, e);
            }
        }

        if success_count >= majority {
            Ok(responses)
        } else {
            Err(Status::internal(
                "Failed to reach majority in AppendEntries",
            ))
        }
    }

    /// Send RequestVote RPC to a single peer
    async fn send_request_vote(
        &mut self,
        peer_id: u32,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Status> {
        if let Some(session) = self.sessions.get_mut(&peer_id) {
            debug!("Sending RequestVote to peer {}", peer_id);

            let (req_tx, req_rx) = tokio::sync::mpsc::channel(1);
            let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);

            match session.request_vote(Request::new(req_stream)).await {
                Ok(response) => {
                    let mut stream = response.into_inner();

                    if let Err(e) = req_tx.send(request).await {
                        error!("Failed to send RequestVote request: {}", e);
                        self.sessions.remove(&peer_id);
                        return Err(Status::internal("Failed to send request"));
                    }

                    match stream.message().await {
                        Ok(Some(resp)) => Ok(resp),
                        Ok(None) => Err(Status::internal("Empty response")),
                        Err(e) => {
                            error!("Error receiving RequestVote response: {}", e);
                            self.sessions.remove(&peer_id);
                            Err(e)
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to start RequestVote stream: {}", e);
                    self.sessions.remove(&peer_id);
                    Err(e)
                }
            }
        } else {
            Err(Status::unavailable("Peer not connected"))
        }
    }

    /// Broadcast RequestVote to all peers and wait for majority success
    pub async fn broadcast_request_vote(
        session: Arc<Mutex<RaftSession>>,
        request: RequestVoteRequest,
    ) -> Result<Vec<(u32, RequestVoteResponse)>, Status> {
        let peer_ids = {
            let s = session.lock().await;
            s.peer_addresses.keys().copied().collect::<Vec<_>>()
        };

        let majority = peer_ids.len() / 2 + 1;
        let mut futures = FuturesUnordered::new();

        for peer_id in peer_ids {
            let req = request.clone();
            let session = Arc::clone(&session);

            futures.push(tokio::spawn(async move {
                let mut s = session.lock().await;
                let result = s.send_request_vote(peer_id, req).await;
                (peer_id, result)
            }));
        }

        let mut responses = Vec::new();
        let mut success_count = 0;

        while let Some(result) = futures.next().await {
            if let Ok((peer_id, Ok(resp))) = result {
                responses.push((peer_id, resp));
                success_count += 1;

                if success_count >= majority {
                    break;
                }
            } else if let Ok((peer_id, Err(e))) = result {
                error!("RequestVote failed to peer {}: {}", peer_id, e);
            }
        }

        if success_count >= majority {
            Ok(responses)
        } else {
            Err(Status::internal("Failed to reach majority in RequestVote"))
        }
    }
}
