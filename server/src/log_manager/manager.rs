use common::NodeId;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, sleep_until, Instant, Interval};
use tracing::{debug, error, info, trace, warn};

use crate::config::ServerConfig;
use crate::database::KeyValueDb;
use crate::log_manager::comm::LogManagerMessage;
use crate::log_manager::raft_persistent::PersistentStateManager;
use crate::log_manager::raft_service::{RaftRequest, RaftService};
use crate::log_manager::raft_session::RaftSession;
use crate::log_manager::storage::RaftLog;
use crate::log_manager::{AppendLogResult, RaftRequestIncomingReceiver};
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
        /// Follower election timeout
        follower_election_timeout: Instant,
    },
    /// Candidate state - requests votes from peers during election
    Candidate {
        /// Candidate election timeout
        candidate_election_timeout: Instant,
    },
    /// Leader state - handles client requests and replicates log entries to followers
    Leader {
        /// For each peer, index of the next log entry to send
        next_index: HashMap<String, u64>,
        /// For each peer, index of highest log entry known to be replicated
        match_index: HashMap<String, u64>,
    },
}

/// Custom type to simplify complex nested tuple types
pub type AppendEntriesResponseData = (
    bool,
    Vec<(u32, AppendEntriesResponse)>,
    AppendEntriesRequest,
    Option<oneshot::Sender<AppendLogResult>>,
);

/// Manager for Raft log operations and consensus
pub struct LogManager {
    /// Underlying Raft log implementation
    log: RaftLog,
    /// Receiver for log manager messages from clients
    executor_rx: mpsc::UnboundedReceiver<LogManagerMessage>,
    /// Reference to the key-value database
    db: Arc<KeyValueDb>,

    /// Following field for raft
    /// Unique identifier for this node
    replica_id: NodeId,
    /// Map of peer nodes (node_id -> network address)
    peers: HashMap<u32, String>,
    /// Current state in the Raft consensus algorithm (Leader/Follower/Candidate)
    current_state: NodeState,
    /// Highest log entry known to be committed
    commit_index: u64,
    /// Highest log entry applied to state machine
    last_applied: u64,
    /// Randomized election timeout duration
    election_timeout: Duration,
    /// Receiver for incoming raft request messages from peers
    raft_request_incoming_rx: RaftRequestIncomingReceiver,
    /// Manager for persistent Raft state
    persistent_state: PersistentStateManager,

    /// All channels
    /// Broadcast append log entry
    append_entries_req_tx: mpsc::UnboundedSender<(
        AppendEntriesRequest,
        Option<oneshot::Sender<AppendLogResult>>,
    )>,
    append_entries_resp_rx: mpsc::UnboundedReceiver<AppendEntriesResponseData>,

    /// Catchup append log entry
    catchup_append_req_tx: mpsc::UnboundedSender<(u32, AppendEntriesRequest)>,
    catchup_append_resp_rx: mpsc::UnboundedReceiver<(u32, AppendEntriesResponse)>,

    /// Vote
    vote_req_tx: mpsc::UnboundedSender<RequestVoteRequest>,
    vote_resp_rx: mpsc::UnboundedReceiver<(bool, u64)>,
}

impl LogManager {
    /// Create a new log manager
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing log path and other settings
    /// * `executor_rx` - Channel receiver for log manager messages
    /// * `db` - Reference to the key-value database
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

        // Broadcast append entry
        let (append_entries_req_tx, append_entries_req_rx) = mpsc::unbounded_channel::<(
            AppendEntriesRequest,
            Option<oneshot::Sender<AppendLogResult>>,
        )>();
        let (append_entries_resp_tx, append_entries_resp_rx) =
            mpsc::unbounded_channel::<AppendEntriesResponseData>();

        // Catch up append entry
        let (catchup_append_req_tx, catchup_append_req_rx) =
            mpsc::unbounded_channel::<(u32, AppendEntriesRequest)>();
        let (catchup_append_resp_tx, catchup_append_resp_rx) =
            mpsc::unbounded_channel::<(u32, AppendEntriesResponse)>();

        // Vote
        let (vote_req_tx, vote_req_rx) = mpsc::unbounded_channel::<RequestVoteRequest>();
        let (vote_resp_tx, vote_resp_rx) = mpsc::unbounded_channel::<(bool, u64)>();

        let raft_service = RaftService::new(raft_request_incoming_tx);
        let peer_listen_addr = config.peer_listen_addr.clone();

        // Start session
        tokio::spawn(async move {
            info!("Starting Raft server at {}", peer_listen_addr);
            tonic::transport::Server::builder()
                .add_service(RaftServer::new(raft_service))
                .serve(peer_listen_addr.parse().unwrap())
                .await
                .unwrap();
        });

        // Initialize persistent state manager
        let persistent_state = match PersistentStateManager::new(
            config
                .persistent_state_path
                .as_ref()
                .map_or_else(|| Path::new("raft_state"), |p| p.as_path()),
        ) {
            Ok(state_manager) => {
                let state = state_manager.get_state();
                info!(
                    "Loaded persistent state: term={}, voted_for={:?}",
                    state.current_term, state.voted_for
                );
                state_manager
            }
            Err(e) => {
                error!("Failed to initialize persistent state manager: {}", e);
                PersistentStateManager::default()
            }
        };

        // Calculate randomized election timeout
        let election_timeout = Duration::from_millis(300 + rand::random::<u64>() % 200);

        // Initialize as follower with election timeout
        let current_state = NodeState::Follower {
            leader_id: 0,
            leader_addr: "TODO".to_string(),
            follower_election_timeout: Instant::now() + election_timeout,
        };

        let peer_replica_addr = config.peer_replica_addr.clone();
        tokio::spawn(async move {
            let mut raft_session = RaftSession::new(peer_replica_addr).await;
            if let Err(e) = raft_session
                .run(
                    append_entries_req_rx,
                    catchup_append_req_rx,
                    vote_req_rx,
                    append_entries_resp_tx,
                    catchup_append_resp_tx,
                    vote_resp_tx,
                )
                .await
            {
                error!("RaftSession run error: {}", e);
            }
        });

        LogManager {
            log,
            executor_rx,
            db,
            replica_id: config.replica_id,
            peers: config.peer_replica_addr.clone(),
            current_state,
            commit_index: 0,
            last_applied: 0,
            election_timeout,
            raft_request_incoming_rx,
            persistent_state,
            append_entries_req_tx,
            append_entries_resp_rx,
            catchup_append_req_tx,
            catchup_append_resp_rx,
            vote_req_tx,
            vote_resp_rx,
        }
    }

    /// Run the log manager service
    pub async fn run(mut self) {
        info!("Log manager started");

        // Create the heartbeat interval for leader state, initially None
        let mut leader_heartbeat_interval: Option<Interval> = None;

        loop {
            match &mut self.current_state {
                NodeState::Leader { .. } => {
                    // Create the interval if we don't already have one
                    if leader_heartbeat_interval.is_none() {
                        debug!("Creating new heartbeat interval for leader");
                        let mut interval = interval(Duration::from_millis(150));
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        leader_heartbeat_interval = Some(interval);
                    }

                    // Unwrap is safe here because we just initialized it if it was None
                    let heartbeat_interval = leader_heartbeat_interval.as_mut().unwrap();

                    tokio::select! {
                        Some(msg) = self.executor_rx.recv() => {
                            debug!("Leader handle client message {:?}.", msg);
                            self.handle_client_request(msg).await;
                        },
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            debug!("Leader handle raft request {:?}.", raft_msg);
                            match raft_msg {
                                RaftRequest::AppendEntriesRequest { request, oneshot_tx } => {
                                    self.handle_append_entries(request, oneshot_tx).await;
                                },
                                RaftRequest::RequestVoteRequest { request, oneshot_tx } => {
                                    self.handle_request_vote(request, oneshot_tx).await;
                                },
                            }
                        },
                        Some(responses) = self.append_entries_resp_rx.recv() => {
                            self.handle_append_entries_responses(responses).await;
                        },
                        Some(catchup_response) = self.catchup_append_resp_rx.recv() => {
                            self.handle_catchup_append_response(catchup_response);
                        }
                        _ = heartbeat_interval.tick() => {
                            trace!("Leader triggered hearbeat.");
                            self.send_heartbeats().await;
                        },
                    }
                }
                NodeState::Follower {
                    follower_election_timeout,
                    ..
                } => {
                    // If we were previously a leader, clean up the interval
                    if leader_heartbeat_interval.is_some() {
                        debug!("Dropping heartbeat interval as follower");
                        leader_heartbeat_interval = None;
                    }

                    tokio::select! {
                        Some(msg) = self.executor_rx.recv() => {
                            debug!("Follower handle client message {:?}.", msg);
                            self.handle_client_request(msg).await;
                        },
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            trace!("Follower handle raft request {:?}.", raft_msg);
                            match raft_msg {
                                RaftRequest::AppendEntriesRequest {
                                    request,
                                    oneshot_tx,
                                } => {
                                    self.handle_append_entries(request, oneshot_tx).await;
                                }
                                RaftRequest::RequestVoteRequest {
                                    request,
                                    oneshot_tx,
                                } => {
                                    self.handle_request_vote(request, oneshot_tx).await;
                                }
                            }
                        },
                        _ = sleep_until(*follower_election_timeout) => {
                            debug!("Follower triggered election timeout.");
                            self.start_election().await;
                        }
                    }
                }
                NodeState::Candidate {
                    candidate_election_timeout,
                } => {
                    // It is impossible for leader directly come into candidate, it
                    // is safe not to reset heartbeat interval

                    tokio::select! {
                        Some(msg) = self.executor_rx.recv() => {
                            debug!("Candidate handle client message {:?}.", msg);
                            self.handle_client_request(msg).await;
                        },
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            debug!("Candidate handle raft request {:?}.", raft_msg);
                            match raft_msg {
                                RaftRequest::AppendEntriesRequest { request, oneshot_tx } => {
                                    self.handle_append_entries(request, oneshot_tx).await;
                                },
                                RaftRequest::RequestVoteRequest { request, oneshot_tx } => {
                                    self.handle_request_vote(request, oneshot_tx).await;
                                },
                            }
                        },
                        _ = sleep_until(*candidate_election_timeout) => {
                            debug!("Candidate triggered election retry timeout.");
                            self.start_election().await;
                        },
                        Some((success, term)) = self.vote_resp_rx.recv() => {
                            if let Some(vote_result) = self.check_election_result(success, term) {
                                if vote_result {
                                    debug!("Candidate won election for term {}.", self.persistent_state.get_state().current_term);
                                    self.transition_to_leader();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Helper method to reset follower election timeout
    fn reset_follower_election_timeout(&mut self) -> Instant {
        let new_timeout = Instant::now() + self.election_timeout;

        if let NodeState::Follower {
            follower_election_timeout,
            ..
        } = &mut self.current_state
        {
            *follower_election_timeout = new_timeout;
        }

        new_timeout
    }

    /// Helper method to transition to follower state
    fn transition_to_follower(&mut self, leader_id: u32, leader_addr: String) {
        debug!("Transitioning to follower, leader_id={}", leader_id);

        let follower_election_timeout = Instant::now() + self.election_timeout;

        self.current_state = NodeState::Follower {
            leader_id,
            leader_addr,
            follower_election_timeout,
        };
    }

    /// Helper method to transition to candidate state
    fn transition_to_candidate(&mut self) {
        debug!("Transitioning to candidate");

        let candidate_election_timeout = Instant::now() + self.election_timeout;

        self.current_state = NodeState::Candidate {
            candidate_election_timeout,
        };
    }

    /// Helper method to transition to leader state
    fn transition_to_leader(&mut self) {
        info!(
            "Transitioning to leader for term {}",
            self.persistent_state.get_state().current_term
        );

        // Initialize leader state
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        // Initialize nextIndex for each server to last log index + 1
        let last_log_index = self.log.get_last_index();

        for peer_addr in self.peers.values() {
            next_index.insert(peer_addr.clone(), last_log_index + 1);
            match_index.insert(peer_addr.clone(), 0);
        }

        self.current_state = NodeState::Leader {
            next_index,
            match_index,
        };
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

                    // Check the specific non-leader state and respond accordingly
                    match &self.current_state {
                        NodeState::Follower { leader_id, .. } => {
                            // For followers, return leader's address
                            let _ = resp_tx.send(AppendLogResult::LeaderSwitch(*leader_id));
                        }
                        NodeState::Candidate { .. } => {
                            // For candidates, return leader unselected
                            let _ = resp_tx.send(AppendLogResult::LeaderUnSelected);
                        }
                        _ => unreachable!(), // This should not happen given the earlier check
                    }
                    return;
                }
                debug!("Received client request {:?}", cmd_id);

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
                                debug!(
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
                    let _ = resp_tx.send(AppendLogResult::Failure);
                    return;
                }

                // Replicate to followers
                if let NodeState::Leader { next_index, .. } = &mut self.current_state {
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
                        leader_id: self.replica_id.into(),
                        prev_log_index,
                        prev_log_term,
                        entries: vec![entry.clone()],
                        leader_commit: self.commit_index,
                    };

                    let _ = self
                        .append_entries_req_tx
                        .send((append_request, Some(resp_tx)));

                    // Update next_index and match_index for followers
                    // This is optimistic - we'll adjust if AppendEntries fails
                    for peer_id in self.peers.keys() {
                        if let Some(peer_addr) = self.peers.get(peer_id) {
                            next_index.insert(peer_addr.clone(), index + 1);
                            // We don't update match_index yet - that happens when we get successful responses
                        }
                    }
                }
            }
        }
    }

    /// Send heartbeats to all followers
    async fn send_heartbeats(&self) {
        trace!("Sending heartbeat to followers");

        // For each follower, prepare and send AppendEntries
        let last_log_index = self.log.get_last_index();
        let last_log_term = self.log.get_last_term();

        // Create a heartbeat request (empty AppendEntries)
        let heartbeat_request = AppendEntriesRequest {
            leader_id: self.replica_id.into(),
            term: self.persistent_state.get_state().current_term,
            prev_log_index: last_log_index,
            prev_log_term: last_log_term,
            entries: vec![], // Empty for heartbeat
            leader_commit: self.commit_index,
        };

        let _ = self.append_entries_req_tx.send((heartbeat_request, None));
    }

    async fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
        oneshot_tx: oneshot::Sender<AppendEntriesResponse>,
    ) {
        trace!(
            "Received append entries from node {}: {} entries",
            request.leader_id,
            request.entries.len()
        );

        let mut success = true;
        let current_term = self.persistent_state.get_state().current_term;

        // 1. Term Verification Term Update
        match request.term.cmp(&current_term) {
            std::cmp::Ordering::Less => {
                // Reject request from outdated leader
                debug!(
                    "Rejecting append entries from outdated leader (term {} < our term {})",
                    request.term, current_term
                );
                let response = AppendEntriesResponse {
                    term: current_term,
                    success: false,
                };
                if oneshot_tx.send(response).is_err() {
                    error!("Failed to send AppendEntries response");
                }
                return;
            }
            std::cmp::Ordering::Greater => {
                debug!("Updating term from {} to {}", current_term, request.term);
                if let Err(e) = self
                    .persistent_state
                    .update_term_and_vote(request.term, None)
                {
                    error!("Failed to persist term and vote update: {}", e);
                }
            }
            std::cmp::Ordering::Equal => {
                if let NodeState::Leader { .. } = self.current_state {
                    panic!("Impossible for leader received same term append log entry request");
                }
            }
        }

        // 2. State Transition - if we're not a follower or we have a different leader
        match self.current_state {
            NodeState::Leader { .. } => {
                // If we're a leader but received AppendEntries with equal or higher term,
                // we should step down
                debug!(
                    "Stepping down from leader to follower due to AppendEntries from node {}",
                    request.leader_id
                );

                let leader_addr = self
                    .peers
                    .get(&request.leader_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());

                self.transition_to_follower(request.leader_id, leader_addr);
            }
            NodeState::Candidate { .. } => {
                // If we're a candidate but received AppendEntries with equal or higher term,
                // we should revert to follower
                debug!(
                    "Reverting from candidate to follower due to AppendEntries from node {}",
                    request.leader_id
                );

                let leader_addr = self
                    .peers
                    .get(&request.leader_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());

                self.transition_to_follower(request.leader_id, leader_addr);
            }
            NodeState::Follower { leader_id, .. } if leader_id != request.leader_id => {
                // If we're a follower but with a different leader, update leader info
                debug!(
                    "Changing leader from {} to {}",
                    leader_id, request.leader_id
                );

                let leader_addr = self
                    .peers
                    .get(&request.leader_id)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());

                self.transition_to_follower(request.leader_id, leader_addr);
            }
            NodeState::Follower { .. } => {
                // Already a follower with the correct leader, just reset the election timeout
                self.reset_follower_election_timeout();
                debug!("Reset election timer due to heartbeat from leader");
            }
        }

        // 3. Log Consistency Check
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
                    debug!(
                        "Log inconsistency: missing entry at index {}",
                        request.prev_log_index
                    );
                    success = false;
                }
            }
        }

        // 4. If consistency check passes and there are entries to append, process them
        let mut last_new_entry_index = request.prev_log_index;

        if success && !request.entries.is_empty() {
            // Find conflicting entries and truncate log if needed
            for entry in &request.entries {
                let entry_index = entry.index;

                // Check if we already have an entry at this index
                if let Some(existing_entry) = self.log.get_entry(entry_index) {
                    if existing_entry.term != entry.term {
                        // Conflict found - truncate log from this point
                        debug!(
                            "Truncating log from index {} due to term mismatch",
                            entry_index
                        );
                        if let Err(e) = self.log.truncate_from(entry_index) {
                            error!("Failed to truncate log: {}", e);
                            success = false;
                            break;
                        }

                        // Append this entry
                        if let Err(e) = self.log.append(entry.clone()) {
                            error!("Failed to append entry at index {}: {}", entry_index, e);
                            success = false;
                            break;
                        }
                        last_new_entry_index = entry_index;
                    }
                    // If terms match, we already have this entry, so skip it
                    else {
                        last_new_entry_index = entry_index;
                    }
                } else {
                    // Entry doesn't exist, append it
                    if let Err(e) = self.log.append(entry.clone()) {
                        error!("Failed to append entry at index {}: {}", entry_index, e);
                        success = false;
                        break;
                    }
                    last_new_entry_index = entry_index;
                }
            }

            // 5. Update commit index if needed
            if success && request.leader_commit > self.commit_index {
                self.commit_index = request.leader_commit.min(last_new_entry_index);
                debug!("Updated commit index to {}", self.commit_index);

                // Apply newly committed entries to state machine
                self.apply_committed_entries().await;
            }
        }

        // 6. Reset Election Timer - always reset if this is a valid AppendEntries
        if success && matches!(self.current_state, NodeState::Follower { .. }) {
            self.reset_follower_election_timeout();
            debug!("Reset election timer due to valid AppendEntries");
        }

        // 7. Send Response
        let response = AppendEntriesResponse {
            term: self.persistent_state.get_state().current_term,
            success,
        };

        if oneshot_tx.send(response).is_err() {
            error!("Failed to send AppendEntries response");
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
        debug!(
            "[{}] Starting election for term {}",
            self.replica_id,
            self.persistent_state.get_state().current_term + 1
        );

        // 1. Increment current term and vote for self atomically
        let new_term = self.persistent_state.get_state().current_term + 1;
        if let Err(e) = self
            .persistent_state
            .update_term_and_vote(new_term, Some(self.replica_id.into()))
        {
            error!("Failed to persist term and vote update: {}", e);
        }

        // 2. Transition to candidate state
        self.transition_to_candidate();

        // 3. Prepare RequestVote RPC
        let last_log_index = self.log.get_last_index();
        let last_log_term = self.log.get_last_term();

        let request = RequestVoteRequest {
            term: self.persistent_state.get_state().current_term,
            candidate_id: self.replica_id.into(),
            last_log_index,
            last_log_term,
        };

        // 4. Send RequestVote RPCs to all other servers
        let _ = self.vote_req_tx.send(request);
    }

    async fn handle_request_vote(
        &mut self,
        request: RequestVoteRequest,
        oneshot_tx: oneshot::Sender<RequestVoteResponse>,
    ) {
        let mut vote_granted = false;
        let was_leader_or_candidate = !matches!(self.current_state, NodeState::Follower { .. });

        // If the candidate's term is greater than ours, update our term
        if request.term > self.persistent_state.get_state().current_term {
            if let Err(e) = self
                .persistent_state
                .update_term_and_vote(request.term, None)
            {
                error!("Failed to persist term and vote update: {}", e);
            }

            // If we were a candidate or leader, revert to follower
            if !matches!(self.current_state, NodeState::Follower { .. }) {
                debug!("Stepping down to follower due to higher term in RequestVote");

                // Since we don't have a leader yet, use 0 as leader_id
                self.transition_to_follower(0, "unknown".to_string());
            }
        }

        // Decide whether to grant vote
        if request.term < self.persistent_state.get_state().current_term {
            // Reject vote if candidate's term is outdated
            vote_granted = false;
        } else if self.persistent_state.get_state().voted_for.is_none()
            || self.persistent_state.get_state().voted_for == Some(request.candidate_id)
        {
            // Check if candidate's log is at least as up-to-date as ours
            let last_log_index = self.log.get_last_index();
            let last_log_term = self.log.get_last_term();

            if request.last_log_term > last_log_term
                || (request.last_log_term == last_log_term
                    && request.last_log_index >= last_log_index)
            {
                // Grant vote
                vote_granted = true;
                debug!(
                    "Vote for node {} term {}",
                    request.candidate_id, request.term
                );
                if let Err(e) = self
                    .persistent_state
                    .update_vote(Some(request.candidate_id))
                {
                    error!("Failed to persist vote update: {}", e);
                }

                // Reset election timer ONLY if we granted the vote
                if let NodeState::Follower { .. } = &self.current_state {
                    self.reset_follower_election_timeout();
                    debug!("Reset election timer due to granting vote");
                }
            }
        }

        // Reset election timer if we transitioned from leader/candidate to follower
        if was_leader_or_candidate && matches!(self.current_state, NodeState::Follower { .. }) {
            self.reset_follower_election_timeout();
            debug!("Reset election timer due to state transition in RequestVote");
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

    fn check_election_result(&mut self, success: bool, term: u64) -> Option<bool> {
        // Only accept results for the current term
        if term == self.persistent_state.get_state().current_term {
            return Some(success);
        } else if term >= self.persistent_state.get_state().current_term {
            if let Err(e) = self.persistent_state.update_term_and_vote(term, None) {
                error!("Failed to persist term and vote update: {}", e);
            }
            // Default to leader 0 and unknown address
            self.transition_to_follower(0, "unknown".to_string());
        }
        None
    }

    async fn handle_append_entries_responses(&mut self, responses: AppendEntriesResponseData) {
        // Collect necessary data outside the borrow
        let mut peers_to_retry = Vec::new();

        if let NodeState::Leader {
            next_index,
            match_index,
        } = &mut self.current_state
        {
            let (success, per_server_responses, request, sender) = responses;

            // 1. Check if we are still in valid term
            // Find maximum term in all responses
            let max_term = per_server_responses
                .iter()
                .map(|(_, resp)| resp.term)
                .max()
                .unwrap_or(0);

            // If max term is higher than ours, step down
            if max_term > self.persistent_state.get_state().current_term {
                debug!(
                    "Received higher term ({} > {}) in AppendEntries responses. Stepping down as leader.",
                    max_term, self.persistent_state.get_state().current_term
                );

                // Update our term
                if let Err(e) = self.persistent_state.update_term_and_vote(max_term, None) {
                    error!("Failed to persist term and vote update: {}", e);
                }

                // Revert to follower state
                // TODO: we now just set leader to be 0 and unknow address, waiting for future append
                self.transition_to_follower(0, "unknown".to_string());
                return;
            }

            // 2. Check if append success, if not do nothing
            if !success {
                return;
            }

            if let Some(sender) = sender {
                if sender
                    .send(AppendLogResult::Success(request.prev_log_index + 1))
                    .is_err()
                {
                    warn!("Failed to send log append response - receiver dropped");
                }
            };

            // 3. Process each response individually
            for (peer_id, response) in per_server_responses {
                let peer_addr = match self.peers.get(&peer_id) {
                    Some(addr) => addr.clone(),
                    None => {
                        error!("Unknown peer_id {} in response", peer_id);
                        continue;
                    }
                };

                if response.success {
                    // Success - update nextIndex and matchIndex for this follower
                    let last_entry_index = if !request.entries.is_empty() {
                        request.entries.last().unwrap().index
                    } else {
                        request.prev_log_index
                    };

                    // Update nextIndex to be one past the last entry we sent
                    next_index.insert(peer_addr.clone(), last_entry_index + 1);

                    // Update matchIndex to the index of the last entry we know is replicated
                    match_index.insert(peer_addr.clone(), last_entry_index);

                    debug!(
                        "Updated nextIndex for {} to {} and matchIndex to {}",
                        peer_addr,
                        last_entry_index + 1,
                        last_entry_index
                    );
                } else {
                    // Failure: we should sure in this case is some node too old
                    // decrement nextIndex and retry
                    let current_next_index = next_index.get(&peer_addr).cloned().unwrap_or(1);

                    // Decrement nextIndex, but ensure it doesn't go below 1
                    let new_next_index = std::cmp::max(current_next_index - 1, 1);
                    next_index.insert(peer_addr.clone(), new_next_index);

                    debug!(
                        "AppendEntries failed for {}. Decremented nextIndex to {}",
                        peer_addr, new_next_index
                    );

                    peers_to_retry.push(peer_addr);
                }
            }

            // Check if we can advance the commit index after processing all responses
            self.update_commit_index().await;
        }

        // Now perform the retries outside of the borrow
        for peer_addr in peers_to_retry {
            self.retry_append_entries(peer_addr);
        }
    }

    fn handle_catchup_append_response(
        &mut self,
        catchup_append_response: (u32, AppendEntriesResponse),
    ) {
        if let NodeState::Leader { next_index, .. } = &mut self.current_state {
            let (peer_id, response) = catchup_append_response;
            if !response.success {
                let peer_addr = match self.peers.get(&peer_id) {
                    Some(addr) => addr.clone(),
                    None => {
                        panic!("Unknown peer_id {} in response", peer_id);
                    }
                };
                // Failure: we should sure in this case is some node too old
                // decrement nextIndex and retry
                let current_next_index = next_index.get(&peer_addr).cloned().unwrap_or(1);

                // Decrement nextIndex, but ensure it doesn't go below 1
                let new_next_index = std::cmp::max(current_next_index - 1, 1);
                next_index.insert(peer_addr.clone(), new_next_index);

                debug!(
                    "AppendEntries failed for {}. Decremented nextIndex to {}",
                    peer_addr, new_next_index
                );
                self.retry_append_entries(peer_addr);
            }
        }
    }

    fn retry_append_entries(&mut self, peer_addr: String) {
        if let NodeState::Leader { next_index, .. } = &self.current_state {
            let next_idx = next_index.get(&peer_addr).cloned().unwrap_or(1);

            // Get the previous log entry's index and term
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index > 0 {
                match self.log.get_entry(prev_log_index) {
                    Some(entry) => entry.term,
                    None => 0,
                }
            } else {
                0
            };

            // Get entries to send (from nextIndex onwards)
            let mut entries = Vec::new();
            let last_log_index = self.log.get_last_index();

            for idx in next_idx..=last_log_index {
                if let Some(entry) = self.log.get_entry(idx) {
                    entries.push(entry.clone());
                }
            }

            // Create AppendEntries request
            let request = AppendEntriesRequest {
                term: self.persistent_state.get_state().current_term,
                leader_id: self.replica_id.into(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };

            // Find the peer ID for this address
            let mut peer_id = None;
            for (id, addr) in &self.peers {
                if addr == &peer_addr {
                    peer_id = Some(*id);
                    break;
                }
            }

            // Send append entry request
            if let Some(id) = peer_id {
                let _ = self.catchup_append_req_tx.send((id, request));
            }
        }
    }

    async fn update_commit_index(&mut self) {
        if let NodeState::Leader { match_index, .. } = &self.current_state {
            // Get all matchIndex values
            let mut match_indices: Vec<u64> = match_index.values().cloned().collect();
            // Add our own last log index
            match_indices.push(self.log.get_last_index());

            // Sort in ascending order
            match_indices.sort_unstable();

            // Find the median (majority) index
            let majority_idx = match_indices.len() / 2;
            let potential_commit_index = match_indices[majority_idx];

            // Only update if the entry at the potential commit index has the current term
            // and it's greater than our current commit index
            if potential_commit_index > self.commit_index {
                if let Some(entry) = self.log.get_entry(potential_commit_index) {
                    if entry.term == self.persistent_state.get_state().current_term {
                        debug!(
                            "Advancing commit index from {} to {}",
                            self.commit_index, potential_commit_index
                        );
                        self.commit_index = potential_commit_index;

                        // Apply newly committed entries
                        self.apply_committed_entries().await;
                    }
                }
            }
        }
    }
}
