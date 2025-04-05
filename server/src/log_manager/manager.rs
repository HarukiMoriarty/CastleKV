use common::NodeId;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;
use crate::database::KeyValueDb;
use crate::log_manager::comm::LogManagerMessage;
use crate::log_manager::raft_service::{RaftRequest, RaftService};
use crate::log_manager::storage::RaftLog;
use crate::log_manager::RaftRequestIncomingReceiver;
use crate::plan::Plan;
use rpc::gateway::Command;
use rpc::raft::{raft_server::RaftServer, LogEntry};

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
    /// Unique identifier for this node
    node_id: NodeId,
    /// Map of peer nodes (node_id -> network address)
    peers: HashMap<u32, String>,

    /// Current state in the Raft consensus algorithm (Leader/Follower/Candidate)
    current_state: NodeState,
    /// Current term number (monotonically increasing)
    current_term: u64,
    /// Node ID this node voted for in the current term (if any)
    voted_for: Option<u32>,

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
    pub fn new(
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

        LogManager {
            log,
            executor_rx,
            db,
            node_id: config.node_id,
            peers: config.peer_replica_addr.clone(),
            current_state: NodeState::Follower {
                leader_id: 0,
                leader_addr: "TODO".to_string(),
            },
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
            election_timeout: Duration::from_millis(300 + rand::random::<u64>() % 200),
            raft_request_incoming_rx,
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
                            unimplemented!();
                        },
                        _ = heartbeat_interval.tick() => {
                            unimplemented!();
                        },
                    }
                }
                NodeState::Follower {
                    leader_id,
                    leader_addr,
                } => {
                    tokio::select! {
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            unimplemented!();
                            election_deadline = self.last_heartbeat + self.election_timeout;
                        },
                        _ = tokio::time::sleep_until(election_deadline.into()) => {
                            info!("Election timeout reached. Becoming candidate.");
                        }
                    }
                }
                NodeState::Candidate => {
                    tokio::select! {
                        Some(raft_msg) = self.raft_request_incoming_rx.recv() => {
                            unimplemented!();
                        },
                        _ = tokio::time::sleep_until(election_deadline.into()) => {
                            info!("Election retry timeout. Starting new election.");
                        },
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

                // Generate log entry
                let term = 0;
                let index = 0;

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

                // Send the response with the log index
                if resp_tx.send(index).is_err() {
                    warn!("Failed to send log append response - receiver dropped");
                }
            }
        }
    }
}
