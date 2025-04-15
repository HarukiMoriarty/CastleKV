use common::NodeId;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;
use crate::database::KeyValueDb;
use crate::log_manager::comm::{ElectionResult, LogManagerMessage};
use crate::log_manager::persistent_states::PersistentStateManager;
use crate::log_manager::raft_service::{RaftRequest, RaftService};
use crate::log_manager::raft_session::RaftSession;
use crate::log_manager::storage::RaftLog;
use crate::log_manager::RaftRequestIncomingReceiver;
use crate::plan::Plan;
use rpc::gateway::Command;
use rpc::raft::{
    raft_server::RaftServer, AppendEntriesRequest, AppendEntriesResponse, LogEntry,
    RequestVoteRequest, RequestVoteResponse,
};

/// Represents the possible states of a node in the Raft
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Follower state - receives log entries from leader and votes in elections
    Follower {
        /// Current leader's node ID
        leader_id: u32,
        /// Current leader's network address
        leader_addr: String,
    },
    /// Candidate state - requests votes from peers during election
    Candidate,
    /// Leader state - handles client requests and replicates log entries to followers
    Leader {
        /// For each peer, index of the next log entry to send
        next_index: HashMap<String, u64>,
        /// For each peer, index of highest log entry known to be replicated
        match_index: HashMap<String, u64>,
    },
}

/// Manager for Raft log operations and consensus
pub struct LogManager {
    /// Underlying Raft log implementation
    log: RaftLog,
    /// Receiver for log manager messages from clients
    executor_rx: mpsc::UnboundedReceiver<LogManagerMessage>,
    /// Reference to the key-value database
    db: Arc<KeyValueDb>,
    /// Raft session for managing peer connections
    raft_session: Arc<Mutex<RaftSession>>,
    /// Unique identifier for this node
    node_id: NodeId,
    /// Map of peer nodes (node_id -> network address)
    peers: HashMap<u32, String>,

    /// Current state in the Raft consensus algorithm (Leader/Follower/Candidate)
    current_state: NodeState,

    /// Highest log entry known to be committed
    commit_index: u64,
    /// Highest log entry applied to state machine
    last_applied: u64,

    /// Timestamp of last received heartbeat
    last_heartbeat: Instant,
    /// Randomized election timeout duration
    election_timeout: Duration,

    /// Receiver for incoming raft request messages from peers
    raft_request_incoming_rx: RaftRequestIncomingReceiver,

    /// Election result - shared between the main loop and election task
    election_result: Arc<Mutex<Option<ElectionResult>>>,

    /// Manager for persistent Raft state
    persistent_state: PersistentStateManager,
}

impl LogManager {
    /// Create a new log manager
    ///
    /// # Arguments
    ///
    /// * `log_path` - Optional path to store log files
    /// * `rx` - Channel receiver for log manager messages
    /// * `db` - Reference to the key-value database
    /// * `max_segment_size` - Maximum size of each log segment in bytes
    pub async fn new(
        config: &ServerConfig,
        executor_rx: mpsc::UnboundedReceiver<LogManagerMessage>,
        db: Arc<KeyValueDb>,
    ) -> Self {
        let path_str = config
            .log_path
            .as_ref()
            .map(|p| p.to_str().unwrap_or("raft"))
            .unwrap_or("raft");

        // RaftLog::open handles directory creation/recovery
        let mut log = match RaftLog::open(path_str, config.log_seg_entry_size) {
            Ok(log) => {
                info!("Successfully opened log at {}", path_str);
                log
            }
            Err(e) => {
                error!("Failed to open log: {}, retrying...", e);
                RaftLog::open(path_str, config.log_seg_entry_size)
                    .expect("Failed to open log on retry")
            }
        };

        // Load all log entries into in-memory DB and storage
        let entries = match log.load_all_entries() {
            Ok(entries) => {
                info!("Loaded {} log entries", entries.len());
                entries
            }
            Err(e) => {
                error!("Failed to load log entries: {}", e);
                Vec::new()
            }
        };

        // Apply all the log entries to bring DB up to date
        for entry in entries {
            debug!("Applying log entry: {:?}", entry);

            match Plan::from_log_command(&entry.command.unwrap()) {
                Ok(plan) => {
                    db.execute(&plan);
                }
                Err(e) => {
                    error!("Failed to create plan from log entry: {}", e);
                }
            }
        }

        // Start raft server
        let (raft_request_incoming_tx, raft_request_incoming_rx) =
            mpsc::unbounded_channel::<RaftRequest>();
        let raft_service = RaftService::new(raft_request_incoming_tx);
        let peer_listen_addr = config.peer_listen_addr.clone();
        tokio::spawn(async move {
            info!("Starting Raft server at {}", peer_listen_addr);
            tonic::transport::Server::builder()
                .add_service(RaftServer::new(raft_service))
                .serve(peer_listen_addr.parse().unwrap())
                .await
                .unwrap();
        });

        // Initialize with no election result
        let election_result = Arc::new(Mutex::new(None));

        // Initialize persistent state manager
        let persistent_state = match PersistentStateManager::new(config.persistent_state_path.as_ref().map_or_else(|| Path::new("raft_state"), |p| p.as_path())) {
            Ok(state_manager) => {
                let state = state_manager.get_state();
                info!("Loaded persistent state: term={}, voted_for={:?}", 
                      state.current_term, state.voted_for);
                state_manager
            },
            Err(e) => {
                error!("Failed to initialize persistent state manager: {}", e);
                PersistentStateManager::default()
            }
        };

        LogManager {
            log,
            executor_rx,
            db,
            raft_session: Arc::new(Mutex::new(
                RaftSession::new(config.peer_replica_addr.clone()).await,
            )),
            node_id: config.node_id,
            peers: config.peer_replica_addr.clone(),
            current_state: NodeState::Follower {
                leader_id: 0,
                leader_addr: "TODO".to_string(),
            },
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
            election_timeout: Duration::from_millis(300 + rand::random::<u64>() % 200),
            raft_request_incoming_rx,
            election_result,
            persistent_state,
        }
    }

    /// Run the log manager service
    pub async fn run(mut self) {
        info!("Log manager started");

        let mut heartbeat_interval = interval(Duration::from_millis(150));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut election_deadline = self.last_heartbeat + self.election_timeout;

        loop {
            match &mut self.current_state {
                NodeState::Leader {
                    next_index,
                    match_index,
                } => {
                    tokio::select! {
                        Some(msg) = self.executor_rx.recv() => {
                            self.handle_client_request(msg).await;
                        },
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            // unimplemented!();
                            println!("raft_msg: {:?}", raft_msg);
                        },
                        _ = heartbeat_interval.tick() => {
                            self.send_heartbeats().await;
                            self.last_heartbeat = Instant::now();
                        },
                    }
                }
                NodeState::Follower {
                    leader_id,
                    leader_addr,
                } => {
                    tokio::select! {
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            match raft_msg {
                                RaftRequest::AppendEntriesRequest {
                                    request,
                                    oneshot_tx,
                                } => {
                                    if request.entries.len() == 0 {
                                        // Heartbeat
                                        self.follower_handle_heartbeat(request, oneshot_tx).await;
                                    } else {
                                        // TODO: Handle append entries request
                                        // unimplemented!();
                                        println!("append entries request: {:?}", request);
                                    }
                                    election_deadline = self.last_heartbeat + self.election_timeout;
                                }
                                RaftRequest::RequestVoteRequest {
                                    request,
                                    oneshot_tx,
                                } => {
                                    self.handle_request_vote(request, oneshot_tx).await;
                                }
                            }
                        },
                        _ = tokio::time::sleep_until(election_deadline.into()) => {
                            info!("Election timeout reached. Becoming candidate.");
                            self.start_election().await;
                            election_deadline = self.last_heartbeat + self.election_timeout;
                        }
                    }
                }
                NodeState::Candidate => {
                    tokio::select! {
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            match raft_msg {
                                RaftRequest::AppendEntriesRequest { request, oneshot_tx } => {
                                    // If we receive an AppendEntries with term >= our term,
                                    // we should revert to follower
                                    if request.term >= self.persistent_state.get_state().current_term {
                                        info!("Received AppendEntries from leader with term >= our term. Reverting to follower.");
                                        if let Err(e) = self.persistent_state.update_term_and_vote(request.term, None) {
                                            error!("Failed to persist term and vote update: {}", e);
                                        }
                                        self.current_state = NodeState::Follower {
                                            leader_id: request.leader_id,
                                            leader_addr: self.peers.get(&request.leader_id)
                                                .cloned()
                                                .unwrap_or_else(|| "unknown".to_string()),
                                        };

                                        // Handle the request as a follower would
                                        self.follower_handle_heartbeat(request, oneshot_tx).await;
                                    } else {
                                        // Reject AppendEntries from outdated leader
                                        let response = AppendEntriesResponse {
                                            term: self.persistent_state.get_state().current_term,
                                            success: false,
                                        };
                                        if oneshot_tx.send(response).is_err() {
                                            error!("Failed to send AppendEntries response");
                                        }
                                    }
                                },
                                RaftRequest::RequestVoteRequest { request, oneshot_tx } => {
                                    self.handle_request_vote(request, oneshot_tx).await;
                                },
                            }
                        },
                        _ = tokio::time::sleep_until(election_deadline.into()) => {
                            info!("Election retry timeout. Starting new election.");
                            self.start_election().await;
                            election_deadline = self.last_heartbeat + self.election_timeout;
                        },
                        _ = tokio::time::sleep(Duration::from_millis(10)) => {
                            // Periodically check if we've won the election
                            if let Some(vote_result) = self.check_election_result() {
                                if vote_result {
                                    info!("Won election for term {}. Becoming leader.", self.persistent_state.get_state().current_term);
                                    self.become_leader().await;
                                }
                            }
                        }
                    }
                }
            }
        }

        info!("Log manager stopped");
    }

    async fn handle_client_request(&mut self, msg: LogManagerMessage) {
        const MAX_RETRIES: usize = 3;
        match msg {
            LogManagerMessage::AppendEntry {
                cmd_id,
                ops,
                resp_tx,
            } => {
                // Only leader should handle this
                if !matches!(self.current_state, NodeState::Leader { .. }) {
                    warn!("Received client request but not leader");
                    let _ = resp_tx.send(0); // or use Result<u64, Err>
                    return;
                }
                info!("Received client request {:?}", cmd_id);

                // Generate log entry
                let term = self.persistent_state.get_state().current_term;
                let last_index = self.log.get_last_index();
                let index = last_index + 1;

                let entry = LogEntry {
                    term,
                    index,
                    command: Some(Command {
                        cmd_id: cmd_id.into(),
                        ops,
                    }),
                };

                debug!("Leader appending client entry at index {}", index);

                // Try to append the log entry with retries if it fails
                let mut success = false;
                let mut retry_count = 0;

                while !success && retry_count <= MAX_RETRIES {
                    match self.log.append(entry.clone()) {
                        Ok(_) => {
                            success = true;

                            if retry_count > 0 {
                                info!(
                                    "Successfully appended log entry after {} retries",
                                    retry_count
                                );
                            }
                        }
                        Err(e) => {
                            retry_count += 1;

                            if retry_count <= MAX_RETRIES {
                                warn!(
                                    "Failed to append log entry (attempt {}/{}): {}",
                                    retry_count,
                                    MAX_RETRIES + 1,
                                    e
                                );
                            } else {
                                error!(
                                    "Failed to append log entry after {} attempts: {}",
                                    MAX_RETRIES + 1,
                                    e
                                );
                            }
                        }
                    }
                }

                if !success {
                    let _ = resp_tx.send(0);
                    return;
                }

                // Replicate to followers
                if let NodeState::Leader { next_index, match_index } = &mut self.current_state {
                    // Create AppendEntries request
                    let prev_log_index = index - 1;
                    let prev_log_term = if prev_log_index > 0 {
                        match self.log.get_entry(prev_log_index) {
                            Some(entry) => entry.term,
                            None => 0,
                        }
                    } else {
                        0
                    };

                    let append_request = AppendEntriesRequest {
                        term: self.persistent_state.get_state().current_term,
                        leader_id: self.node_id.0,
                        prev_log_index,
                        prev_log_term,
                        entries: vec![entry.clone()],
                        leader_commit: self.commit_index,
                    };

                    // Clone the session for the spawned task
                    let raft_session = Arc::clone(&self.raft_session);
                    let entry_index = index;

                    // Spawn a task to handle replication
                    tokio::spawn(async move {
                        match RaftSession::broadcast_append_entries(raft_session, append_request).await {
                            Ok(responses) => {
                                info!("Successfully replicated entry {} to majority", entry_index);
                                // Send the response with the log index
                                if resp_tx.send(entry_index).is_err() {
                                    warn!("Failed to send log append response - receiver dropped");
                                }
                            }
                            Err(e) => {
                                error!("Failed to replicate entry to majority: {}", e);
                                // Send failure response
                                if resp_tx.send(0).is_err() {
                                    warn!("Failed to send log append failure response - receiver dropped");
                                }
                            }
                        }
                    });

                    // Update next_index and match_index for followers
                    // This is optimistic - we'll adjust if AppendEntries fails
                    for (peer_id, _) in &self.peers {
                        if let Some(peer_addr) = self.peers.get(peer_id) {
                            next_index.insert(peer_addr.clone(), index + 1);
                            // We don't update match_index yet - that happens when we get successful responses
                        }
                    }
                } else {
                    // This shouldn't happen due to the earlier check, but just in case
                    let _ = resp_tx.send(0);
                }
            }
        }
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self) {
        debug!("Sending heartbeat to followers");

        // For each follower, prepare and send AppendEntries
        let last_log_index = self.log.get_last_index();
        let last_log_term = self.log.get_last_term();

        // Create a heartbeat request (empty AppendEntries)
        let heartbeat_request = AppendEntriesRequest {
            leader_id: self.node_id.0,
            term: self.persistent_state.get_state().current_term,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![], // Empty for heartbeat
            leader_commit: self.commit_index,
        };

        // Clone the session for the spawned task
        let raft_session = Arc::clone(&self.raft_session);

        tokio::spawn(async move {
            match RaftSession::broadcast_append_entries(raft_session, heartbeat_request).await {
                Ok(responses) => {
                    debug!(
                        "Received {} successful heartbeat responses",
                        responses.len()
                    );
                }
                Err(e) => {
                    warn!("Failed to reach majority with heartbeat: {}", e);
                }
            }
        });
    }

    async fn follower_handle_heartbeat(
        &mut self,
        request: AppendEntriesRequest,
        oneshot_tx: oneshot::Sender<AppendEntriesResponse>,
    ) {
        debug!("Received heartbeat from leader {}", request.leader_id);
        let mut success = true;

        // 1. Term Verification and Update
        if request.term < self.persistent_state.get_state().current_term {
            // Reject heartbeat from outdated leader
            debug!(
                "Rejecting heartbeat from outdated leader (term {} < our term {})",
                request.term, self.persistent_state.get_state().current_term
            );
            success = false;
        } else if request.term > self.persistent_state.get_state().current_term {
            // Update follower's term if leader's term is higher
            debug!(
                "Updating term from {} to {}",
                self.persistent_state.get_state().current_term, request.term
            );
            if let Err(e) = self.persistent_state.update_term_and_vote(request.term, None) {
                error!("Failed to persist term and vote update: {}", e);
            }
        }

        // 2. Log Consistency Check
        if success {
            // Check if follower's log contains an entry at prevLogIndex with matching term
            if request.prev_log_index > 0 {
                match self.log.get_entry(request.prev_log_index) {
                    Some(entry) => {
                        if entry.term != request.prev_log_term {
                            debug!("Log inconsistency: entry at index {} has term {}, but leader expects term {}",
                                   request.prev_log_index, entry.term, request.prev_log_term);
                            success = false;
                        }
                    }
                    None => {
                        if request.prev_log_index > 0 {
                            debug!(
                                "Log inconsistency: missing entry at index {}",
                                request.prev_log_index
                            );
                            success = false;
                        }
                    }
                }
            }
        }

        // 3. Update Leader Information
        if success {
            // Update leader information
            if let NodeState::Follower {
                leader_id,
                leader_addr,
            } = &mut self.current_state
            {
                *leader_id = request.leader_id;
                *leader_addr = self
                    .peers
                    .get(&request.leader_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
            } else {
                // If we're not already a follower, become one
                self.current_state = NodeState::Follower {
                    leader_id: request.leader_id,
                    leader_addr: self
                        .peers
                        .get(&request.leader_id)
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string()),
                };
            }

            // 4. Update Commit Index (if needed)
            if request.leader_commit > self.commit_index {
                let last_log_index = self.log.get_last_index();
                self.commit_index = request.leader_commit.min(last_log_index);
                debug!("Updated commit index to {}", self.commit_index);

                // Apply newly committed entries to state machine
                self.apply_committed_entries().await;
            }
        }

        // 5. Reset Election Timer
        self.last_heartbeat = Instant::now();

        // 6. Send Response
        let response = AppendEntriesResponse {
            term: self.persistent_state.get_state().current_term,
            success,
        };

        if oneshot_tx.send(response).is_err() {
            error!("Failed to send heartbeat response");
        }
    }

    // Helper method to apply committed entries to the state machine
    async fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;

            if let Some(entry) = self.log.get_entry(self.last_applied) {
                if let Some(command) = &entry.command {
                    debug!("Applying log entry {} to state machine", self.last_applied);

                    match Plan::from_log_command(command) {
                        Ok(plan) => {
                            self.db.execute(&plan);
                        }
                        Err(e) => {
                            error!("Failed to create plan from log entry: {}", e);
                        }
                    }
                }
            } else {
                error!("Missing log entry at index {}", self.last_applied);
                break;
            }
        }
    }

    async fn start_election(&mut self) {
        info!(
            "[Node {}] Starting election for term {}",
            self.node_id.0,
            self.persistent_state.get_state().current_term + 1
        );

        // 1. Increment current term and vote for self atomically
        let new_term = self.persistent_state.get_state().current_term + 1;
        if let Err(e) = self.persistent_state.update_term_and_vote(new_term, Some(self.node_id.0)) {
            error!("Failed to persist term and vote update: {}", e);
        }

        // 2. Transition to candidate state
        self.current_state = NodeState::Candidate;

        // 3. Reset election timer with randomization
        self.election_timeout = Duration::from_millis(300 + rand::random::<u64>() % 200);
        self.last_heartbeat = Instant::now();

        // 4. Prepare RequestVote RPC
        let last_log_index = self.log.get_last_index();
        let last_log_term = self.log.get_last_term();

        let request = RequestVoteRequest {
            term: self.persistent_state.get_state().current_term,
            candidate_id: self.node_id.0,
            last_log_index,
            last_log_term,
        };

        // 5. Send RequestVote RPCs to all other servers
        let raft_session = Arc::clone(&self.raft_session);
        let current_term = self.persistent_state.get_state().current_term;
        let node_id = self.node_id.0;
        let election_result = Arc::clone(&self.election_result);

        // Reset election result
        {
            let mut result = self.election_result.lock().await;
            *result = None;
        }

        // Spawn a task to collect votes
        tokio::spawn(async move {
            match RaftSession::broadcast_request_vote(raft_session, request).await {
                Ok(_) => {
                    info!("Node {} won election for term {}", node_id, current_term);

                    // Set the election result
                    let mut result = election_result.lock().await;
                    *result = Some(ElectionResult {
                        won: true,
                        term: current_term,
                    });
                }
                Err(e) => {
                    warn!("Failed to collect majority votes: {}", e);

                    // Set negative result
                    let mut result = election_result.lock().await;
                    *result = Some(ElectionResult {
                        won: false,
                        term: current_term,
                    });
                }
            }
        });
    }

    async fn handle_request_vote(
        &mut self,
        request: RequestVoteRequest,
        oneshot_tx: oneshot::Sender<RequestVoteResponse>,
    ) {
        let mut vote_granted = false;

        // If the candidate's term is greater than ours, update our term
        if request.term > self.persistent_state.get_state().current_term {
            if let Err(e) = self.persistent_state.update_term_and_vote(request.term, None) {
                error!("Failed to persist term and vote update: {}", e);
            }

            // If we were a candidate or leader, revert to follower
            if !matches!(self.current_state, NodeState::Follower { .. }) {
                self.current_state = NodeState::Follower {
                    leader_id: 0, // Unknown leader
                    leader_addr: "unknown".to_string(),
                };
            }
        }

        // Decide whether to grant vote
        if request.term < self.persistent_state.get_state().current_term {
            // Reject vote if candidate's term is outdated
            vote_granted = false;
        } else if self.persistent_state.get_state().voted_for.is_none() || self.persistent_state.get_state().voted_for == Some(request.candidate_id) {
            // Check if candidate's log is at least as up-to-date as ours
            let last_log_index = self.log.get_last_index();
            let last_log_term = self.log.get_last_term();

            if request.last_log_term > last_log_term
                || (request.last_log_term == last_log_term
                    && request.last_log_index >= last_log_index)
            {
                // Grant vote
                vote_granted = true;
                if let Err(e) = self.persistent_state.update_vote(Some(request.candidate_id)) {
                    error!("Failed to persist vote update: {}", e);
                }
                self.last_heartbeat = Instant::now(); // Reset election timer
            }
        }

        // Send response
        let response = RequestVoteResponse {
            term: self.persistent_state.get_state().current_term,
            vote_granted,
        };

        if oneshot_tx.send(response).is_err() {
            error!("Failed to send RequestVote response");
        }
    }

    async fn become_leader(&mut self) {
        // Initialize leader state
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        // Initialize nextIndex for each server to last log index + 1
        let last_log_index = self.log.get_last_index();

        for peer_addr in self.peers.values() {
            next_index.insert(peer_addr.clone(), last_log_index + 1);
            match_index.insert(peer_addr.clone(), 0);
        }

        // Transition to leader state
        self.current_state = NodeState::Leader {
            next_index,
            match_index,
        };

        // Send initial empty AppendEntries RPCs (heartbeats) to establish authority
        self.send_heartbeats().await;
    }

    fn check_election_result(&mut self) -> Option<bool> {
        // Get the current election result
        if let Ok(guard) = self.election_result.try_lock() {
            if let Some(result) = *guard {
                // Only accept results for the current term
                if result.term == self.persistent_state.get_state().current_term {
                    return Some(result.won);
                }
            }
        }

        None
    }
}
