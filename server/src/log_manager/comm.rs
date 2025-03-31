use common::CommandId;
use rpc::gateway::Operation;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// Sender channel type for LogManager communications
pub type LogManagerSender = UnboundedSender<LogManagerMessage>;

/// Messages that can be sent to the log manager
pub enum LogManagerMessage {
    /// Append a new entry to the log
    AppendEntry {
        /// Command identifier
        cmd_id: CommandId,
        /// Operations as key-value pairs
        ops: Vec<Operation>,
        /// Response channel
        resp_tx: oneshot::Sender<u64>,
    },
}
