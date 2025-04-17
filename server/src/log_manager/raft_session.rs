use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::Request;
use tracing::debug;
use tracing::trace;
use tracing::warn;
use tracing::{error, info};

use rpc::raft::{
    raft_client::RaftClient, AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest,
    RequestVoteResponse,
};

type AppendEntriesResponseStream = tonic::Streaming<AppendEntriesResponse>;
type RequestVoteResponseStream = tonic::Streaming<RequestVoteResponse>;

/// Connection state for a peer node
struct PeerConnection {
    /// Peer's node ID
    peer_id: u32,
    /// Peer's address
    addr: String,
    /// Sender for AppendEntries requests
    append_entries_tx: mpsc::UnboundedSender<AppendEntriesRequest>,
    /// Receiver for AppendEntries responses
    append_entries_responses: AppendEntriesResponseStream,
    /// Sender for RequestVote requests
    request_vote_tx: mpsc::UnboundedSender<RequestVoteRequest>,
    /// Receiver for RequestVote responses
    request_vote_responses: RequestVoteResponseStream,
}

impl PeerConnection {
    /// Create a new connection to a peer
    pub async fn new(peer_id: u32, addr: String) -> anyhow::Result<Self> {
        let formatted_addr = if !addr.starts_with("http") {
            format!("http://{}", addr)
        } else {
            addr.clone()
        };

        info!("Connecting to peer {} at {}", peer_id, formatted_addr);

        match RaftClient::connect(formatted_addr.clone()).await {
            Ok(mut client) => {
                // Set up AppendEntries stream
                let (append_tx, append_rx) = mpsc::unbounded_channel();
                let append_req_stream =
                    tokio_stream::wrappers::UnboundedReceiverStream::new(append_rx);

                let append_response = client
                    .append_entries(Request::new(append_req_stream))
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to establish AppendEntries stream with peer {}: {}",
                            peer_id, e
                        );
                        anyhow::anyhow!("Failed to establish AppendEntries stream: {}", e)
                    })?;

                // Set up RequestVote stream
                let (vote_tx, vote_rx) = mpsc::unbounded_channel();
                let vote_req_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(vote_rx);

                let vote_response = client
                    .request_vote(Request::new(vote_req_stream))
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to establish RequestVote stream with peer {}: {}",
                            peer_id, e
                        );
                        anyhow::anyhow!("Failed to establish RequestVote stream: {}", e)
                    })?;

                // Create peer connection with both streams established
                Ok(Self {
                    peer_id,
                    addr,
                    append_entries_tx: append_tx,
                    append_entries_responses: append_response.into_inner(),
                    request_vote_tx: vote_tx,
                    request_vote_responses: vote_response.into_inner(),
                })
            }
            Err(e) => {
                error!("Failed to connect to peer {}: {}", peer_id, e);
                Err(anyhow::anyhow!("Failed to connect to peer: {}", e))
            }
        }
    }

    /// Reconnect to the peer
    pub async fn reconnect(&mut self) -> anyhow::Result<()> {
        debug!(
            "Attempting to reconnect to peer {} at {}",
            self.peer_id, self.addr
        );

        let formatted_addr = if !self.addr.starts_with("http") {
            format!("http://{}", self.addr)
        } else {
            self.addr.clone()
        };

        match RaftClient::connect(formatted_addr).await {
            Ok(mut client) => {
                // Set up AppendEntries stream
                let (append_tx, append_rx) = mpsc::unbounded_channel();
                let append_req_stream =
                    tokio_stream::wrappers::UnboundedReceiverStream::new(append_rx);

                let append_response = client
                    .append_entries(Request::new(append_req_stream))
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to re-establish AppendEntries stream with peer {}: {}",
                            self.peer_id, e
                        );
                        anyhow::anyhow!("Failed to re-establish AppendEntries stream: {}", e)
                    })?;

                // Set up RequestVote stream
                let (vote_tx, vote_rx) = mpsc::unbounded_channel();
                let vote_req_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(vote_rx);

                let vote_response = client
                    .request_vote(Request::new(vote_req_stream))
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to re-establish RequestVote stream with peer {}: {}",
                            self.peer_id, e
                        );
                        anyhow::anyhow!("Failed to re-establish RequestVote stream: {}", e)
                    })?;

                // Update the connection objects
                self.append_entries_tx = append_tx;
                self.append_entries_responses = append_response.into_inner();
                self.request_vote_tx = vote_tx;
                self.request_vote_responses = vote_response.into_inner();

                debug!("Successfully reconnected to peer {}", self.peer_id);
                Ok(())
            }
            Err(e) => {
                error!("Failed to reconnect to peer {}: {}", self.peer_id, e);
                Err(anyhow::anyhow!("Failed to reconnect to peer: {}", e))
            }
        }
    }

    /// Send AppendEntries RPC to this peer with auto-reconnect
    pub async fn send_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse> {
        trace!("Sending AppendEntries to peer {}", self.peer_id);

        // Try to send the request
        if let Err(e) = self.append_entries_tx.send(request.clone()) {
            warn!(
                "Failed to send AppendEntries request to peer {}: {}",
                self.peer_id, e
            );

            // Try to reconnect once
            self.reconnect().await?;

            // Try again with the new connection
            self.append_entries_tx
                .send(request.clone())
                .map_err(|e| anyhow::anyhow!("Failed to send request after reconnect: {}", e))?;
        }

        // Try to receive the response
        match self.append_entries_responses.message().await {
            Ok(Some(resp)) => {
                trace!("Received AppendEntries response from peer {}", self.peer_id);
                Ok(resp)
            }
            Ok(None) => {
                warn!("Empty AppendEntries response from peer {}", self.peer_id);
                Err(anyhow::anyhow!(
                    "Empty AppendEntries response from peer {}",
                    self.peer_id
                ))
            }
            Err(e) => {
                warn!(
                    "Error receiving AppendEntries response from peer {}: {}",
                    self.peer_id, e
                );
                Err(anyhow::anyhow!(
                    "Error receiving AppendEntries response from peer {}: {}",
                    self.peer_id,
                    e
                ))
            }
        }
    }

    /// Send RequestVote RPC to this peer with auto-reconnect
    pub async fn send_request_vote(
        &mut self,
        request: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse> {
        trace!("Sending RequestVote to peer {}", self.peer_id);

        // Try to send the request
        if let Err(e) = self.request_vote_tx.send(request) {
            warn!(
                "Failed to send RequestVote request to peer {}: {}",
                self.peer_id, e
            );

            // Try to reconnect once
            self.reconnect().await?;

            // Try again with the new connection
            self.request_vote_tx
                .send(request)
                .map_err(|e| anyhow::anyhow!("Failed to send request after reconnect: {}", e))?;
        }

        // Try to receive the response
        match self.request_vote_responses.message().await {
            Ok(Some(resp)) => {
                trace!("Received RequestVote response from peer {}", self.peer_id);
                Ok(resp)
            }
            Ok(None) => {
                warn!("Empty RequestVote response from peer {}", self.peer_id);
                Err(anyhow::anyhow!(
                    "Empty RequestVote response from peer {}",
                    self.peer_id
                ))
            }
            Err(e) => {
                warn!(
                    "Error receiving RequestVote response from peer {}: {}",
                    self.peer_id, e
                );
                Err(anyhow::anyhow!(
                    "Error receiving RequestVote response from peer {}: {}",
                    self.peer_id,
                    e
                ))
            }
        }
    }
}

/// Manages RPC client connections to other Raft nodes
pub struct RaftSession {
    /// Map of connected clients
    peer_connections: HashMap<u32, PeerConnection>,
}

impl RaftSession {
    /// Create a new RaftSession and connect to all peers
    pub async fn new(peer_addresses: HashMap<u32, String>) -> Self {
        let mut peer_connections = HashMap::new();

        // Connect to all peers with retries during initialization
        for (peer_id, addr) in &peer_addresses {
            let peer_id = *peer_id;
            let addr = addr.clone();

            // Keep trying until connected
            loop {
                match PeerConnection::new(peer_id, addr.clone()).await {
                    Ok(connection) => {
                        peer_connections.insert(peer_id, connection);
                        break;
                    }
                    Err(e) => {
                        error!(
                            "Failed to connect to peer {}: {}. Retrying in 1 second...",
                            peer_id, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }

        Self { peer_connections }
    }

    /// Send AppendEntries RPC to a specific peer
    pub async fn send_append_entries(
        &mut self,
        peer_id: u32,
        request: AppendEntriesRequest,
    ) -> anyhow::Result<AppendEntriesResponse> {
        match self.peer_connections.get_mut(&peer_id) {
            Some(connection) => connection.send_append_entries(request).await,
            None => Err(anyhow::anyhow!("No connection found for peer {}", peer_id)),
        }
    }

    /// Broadcast AppendEntries to all peers and wait for majority success
    pub async fn broadcast_append_entries(
        session: Arc<Mutex<RaftSession>>,
        request: AppendEntriesRequest,
    ) -> anyhow::Result<Vec<(u32, AppendEntriesResponse)>> {
        let peer_ids = {
            let s = session.lock().await;
            s.peer_connections.keys().copied().collect::<Vec<_>>()
        };

        let majority = if peer_ids.is_empty() {
            0
        } else {
            peer_ids.len() / 2 + 1
        };
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
            Err(anyhow::anyhow!("Failed to reach majority in AppendEntries"))
        }
    }

    /// Send RequestVote RPC to a specific peer
    pub async fn send_request_vote(
        &mut self,
        peer_id: u32,
        request: RequestVoteRequest,
    ) -> anyhow::Result<RequestVoteResponse> {
        match self.peer_connections.get_mut(&peer_id) {
            Some(connection) => connection.send_request_vote(request).await,
            None => Err(anyhow::anyhow!("No connection found for peer {}", peer_id)),
        }
    }

    /// Broadcast RequestVote to all peers and wait for majority success
    pub async fn broadcast_request_vote(
        session: Arc<Mutex<RaftSession>>,
        request: RequestVoteRequest,
    ) -> anyhow::Result<Vec<(u32, RequestVoteResponse)>> {
        let peer_ids = {
            let s = session.lock().await;
            s.peer_connections.keys().copied().collect::<Vec<_>>()
        };

        let majority = if peer_ids.is_empty() {
            0
        } else {
            peer_ids.len() / 2 + 1
        };
        let mut futures = FuturesUnordered::new();

        for peer_id in peer_ids {
            let session = Arc::clone(&session);

            futures.push(tokio::spawn(async move {
                let mut s = session.lock().await;
                let result = s.send_request_vote(peer_id, request).await;
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
            Err(anyhow::anyhow!("Failed to reach majority in RequestVote"))
        }
    }
}
