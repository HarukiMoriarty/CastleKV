use common::CommandId;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// Sender channel type for LogManager communications
pub type LogManagerSender = UnboundedSender<LogManagerMessage>;

/// Log command entry representing a writing command (PUT / SWAP / DELETE)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct LogCommand {
    /// Command identifier
    pub(crate) cmd_id: CommandId,
    /// Operations as key-value pairs
    pub(crate) ops: Vec<(String, String)>,
}

/// A complete log entry in the Raft log
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct LogEntry {
    /// Raft term number
    pub(crate) term: u64,
    /// Log entry index
    pub(crate) index: u64,
    /// Command to be executed
    pub(crate) command: LogCommand,
}

/// Messages that can be sent to the log manager
pub enum LogManagerMessage {
    /// Append a new entry to the log
    AppendEntry {
        /// Command identifier
        cmd_id: CommandId,
        /// Operations as key-value pairs
        ops: Vec<(String, String)>,
        /// Response channel
        resp_tx: oneshot::Sender<u64>,
    },
}
