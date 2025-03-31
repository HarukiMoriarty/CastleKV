use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, error, info, warn};

use crate::database::KeyValueDb;
use crate::log_manager::comm::LogManagerMessage;
use crate::log_manager::storage::RaftLog;
use crate::plan::Plan;
use rpc::gateway::Command;
use rpc::raft::LogEntry;

/// Represents the possible states of a node in the Raft
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Follower state - receives log entries from leader and votes in elections
    Follower,
    /// Candidate state - requests votes from peers during election
    Candidate,
    /// Leader state - handles client requests and replicates log entries to followers
    Leader,
}

/// Manager for Raft log operations and consensus
pub struct LogManager {
    /// Underlying Raft log implementation
    log: RaftLog,
    /// Receiver for log manager messages from clients
    rx: UnboundedReceiver<LogManagerMessage>,
    /// Reference to the key-value database
    db: Arc<KeyValueDb>,
    // /// Unique identifier for this node
    // node_id: u32,
    // /// Map of peer nodes (node_id -> network address)
    // peers: HashMap<u32, String>,

    // /// Current state in the Raft consensus algorithm (Leader/Follower/Candidate)
    // current_state: NodeState,
    // /// Current term number (monotonically increasing)
    // current_term: u64,
    // /// Node ID this node voted for in the current term (if any)
    // voted_for: Option<u32>,

    // /// Current leader's node ID (if known)
    // leader_id: Option<u32>,
    // /// Current leader's network address (if known)
    // leader_addr: Option<String>,

    // /// Highest log entry known to be committed
    // commit_index: u64,
    // /// Highest log entry applied to state machine
    // last_applied: u64,

    // /// For each peer, index of the next log entry to send (leader only)
    // next_index: HashMap<String, u64>,
    // /// For each peer, index of highest log entry known to be replicated (leader only)
    // match_index: HashMap<String, u64>,

    // /// Timestamp of last received heartbeat
    // last_heartbeat: Instant,
    // /// Randomized election timeout duration
    // election_timeout: Duration,

    // /// Map of channels to send RPC messages to peers (node_id -> sender)
    // rpc_tx: HashMap<String, UnboundedSender<RaftRpcMessage>>,
    // /// Receiver for incoming RPC messages from peers
    // rpc_rx: UnboundedReceiver<RaftRpcMessage>,
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
        log_path: Option<impl AsRef<Path>>,
        rx: UnboundedReceiver<LogManagerMessage>,
        db: Arc<KeyValueDb>,
        max_segment_size: usize,
    ) -> Self {
        let path_str = log_path
            .as_ref()
            .map(|p| p.as_ref().to_str().unwrap_or("raft"))
            .unwrap_or("raft");

        // RaftLog::open handles directory creation/recovery
        let mut log = match RaftLog::open(path_str, max_segment_size) {
            Ok(log) => {
                info!("Successfully opened log at {}", path_str);
                log
            }
            Err(e) => {
                error!("Failed to open log: {}, retrying...", e);
                RaftLog::open(path_str, max_segment_size).expect("Failed to open log on retry")
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

        LogManager { log, rx, db }
    }

    /// Run the log manager service
    pub async fn run(mut self) {
        info!("Log manager started");

        const MAX_RETRIES: usize = 3;

        while let Some(msg) = self.rx.recv().await {
            match msg {
                LogManagerMessage::AppendEntry {
                    ops,
                    cmd_id,
                    resp_tx,
                } => {
                    // TODO: use raft to determine term and log index
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
                    debug!("Appending log entry: {:?}", entry);

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

        info!("Log manager stopped");
    }
}
