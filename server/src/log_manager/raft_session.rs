use futures::stream::{FuturesUnordered, StreamExt};
use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
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

use super::AppendLogResult;

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
    peer_connections: HashMap<u32, Arc<Mutex<PeerConnection>>>,
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
                        peer_connections.insert(peer_id, Arc::new(Mutex::new(connection)));
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

    /// Process incoming messages from channels
    pub async fn run(
        &mut self,
        mut append_entries_req_rx: mpsc::UnboundedReceiver<(
            AppendEntriesRequest,
            Option<Sender<AppendLogResult>>,
        )>,
        mut catchup_append_req_rx: mpsc::UnboundedReceiver<(u32, AppendEntriesRequest)>,
        mut vote_req_rx: mpsc::UnboundedReceiver<RequestVoteRequest>,
        append_resp_tx: mpsc::UnboundedSender<(
            bool,
            Vec<(u32, AppendEntriesResponse)>,
            AppendEntriesRequest,
            Option<Sender<AppendLogResult>>,
        )>,
        catchup_resp_tx: mpsc::UnboundedSender<(u32, AppendEntriesResponse)>,
        vote_resp_tx: mpsc::UnboundedSender<(bool, u64)>,
    ) -> anyhow::Result<()> {
        info!("Starting message processing loop");

        loop {
            tokio::select! {
                Some((request, sender)) = append_entries_req_rx.recv() => {
                    // Process broadcast AppendEntries
                    debug!("Processing broadcast AppendEntries");

                    // Get all peer IDs and connections
                    let peer_ids: Vec<u32> = self.peer_connections.keys().copied().collect();
                    let majority = if peer_ids.is_empty() { 0 } else { peer_ids.len() / 2 + 1 };

                    let mut futures = FuturesUnordered::new();

                    // Send to all peers in parallel
                    for peer_id in peer_ids {
                        if let Some(connection) = self.peer_connections.get(&peer_id) {
                            let connection = Arc::clone(connection);
                            let req = request.clone();

                            futures.push(tokio::spawn(async move {
                                let mut conn = connection.lock().await;
                                let result = conn.send_append_entries(req).await;
                                (peer_id, result)
                            }));
                        }
                    }

                    // Collect responses
                    let mut responses = Vec::new();
                    let mut success_count = 0;

                    while let Some(result) = futures.next().await {
                        if let Ok((peer_id, Ok(resp))) = result {
                            responses.push((peer_id, resp));
                            // Append success, check if we reach majority
                            if resp.success {
                                success_count += 1;
                                if success_count >= majority {
                                    break;
                                }
                            }
                        } else if let Ok((peer_id, Err(e))) = result {
                            error!("AppendEntries failed to peer {}: {}", peer_id, e);
                        }
                    }

                    if success_count >= majority {
                        if let Err(e) = append_resp_tx.send((true, responses, request, sender)) {
                            error!("Failed to send append responses: {}", e);
                        }
                    } else if let Err(e) = append_resp_tx.send((false, responses, request, sender)) {
                        error!("Failed to send append responses: {}", e);
                    }

                },

                Some((peer_id, request)) = catchup_append_req_rx.recv() => {
                    // Process catchup AppendEntries to a specific peer
                    debug!("Processing catchup AppendEntries to peer {}", peer_id);

                    let result = if let Some(connection) = self.peer_connections.get(&peer_id) {
                        let mut conn = connection.lock().await;
                        conn.send_append_entries(request).await
                    } else {
                        Err(anyhow::anyhow!("No connection found for peer {}", peer_id))
                    };

                    if let Ok(resp) = result {
                        if let Err(e) = catchup_resp_tx.send((peer_id, resp)) {
                            error!("Failed to send catchup response: {}", e);
                        }
                    }
                },

                Some(request) = vote_req_rx.recv() => {
                    // Process broadcast RequestVote
                    debug!("Processing broadcast RequestVote");

                    // Get all peer IDs and connections
                    let peer_ids: Vec<u32> = self.peer_connections.keys().copied().collect();
                    let majority = if peer_ids.is_empty() { 0 } else { peer_ids.len() / 2 };

                    let mut futures = FuturesUnordered::new();

                    // Send to all peers in parallel
                    for peer_id in peer_ids {
                        if let Some(connection) = self.peer_connections.get(&peer_id) {
                            let connection = Arc::clone(connection);

                            futures.push(tokio::spawn(async move {
                                let mut conn = connection.lock().await;
                                let result = conn.send_request_vote(request).await;
                                (peer_id, result)
                            }));
                        }
                    }

                    // Collect responses
                    let mut responses = Vec::new();
                    let mut success_count = 0;
                    let mut term = 0;

                    while let Some(result) = futures.next().await {
                        if let Ok((peer_id, Ok(resp))) = result {
                            debug!("received vote result from node {peer_id}");
                            term = max(term, resp.term);
                            if resp.vote_granted {
                                responses.push((peer_id, resp));
                                success_count += 1;
                                    if success_count >= majority {
                                    break;
                                }
                            }
                        } else if let Ok((peer_id, Err(e))) = result {
                            error!("RequestVote failed to peer {}: {}", peer_id, e);
                        }
                    }

                    if majority == 0 {
                        term = request.term;
                    }

                    if success_count >= majority {
                        if let Err(e) = vote_resp_tx.send((true, term)) {
                            error!("Failed to send vote responses: {}", e);
                        }
                    } else {
                        assert_ne!(term, 0);
                        if let Err(e) = vote_resp_tx.send((false, term)) {
                            error!("Failed to send vote responses: {}", e);
                        }
                    }
                },

                else => {
                    // All channels closed
                    info!("All message channels closed, terminating message loop");
                    break;
                }
            }
        }

        Ok(())
    }
}
