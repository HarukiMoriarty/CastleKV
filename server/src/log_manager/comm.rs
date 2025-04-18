use common::CommandId;
use rpc::gateway::Operation;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// Sender channel type for LogManager communications
pub type LogManagerSender = UnboundedSender<LogManagerMessage>;

#[derive(Debug)]
/// Messages that can be sent to the log manager
pub enum LogManagerMessage {
    /// Append a new entry to the log
    AppendEntry {
        /// Command identifier
        cmd_id: CommandId,
        /// Operations as key-value pairs
        ops: Vec<Operation>,
        /// Response channel
        resp_tx: oneshot::Sender<AppendLogResult>,
    },
}

#[derive(Debug, Clone)]
pub enum AppendLogResult {
    Success(u64),      // Successfully logged, with log index
    Failure,           // Failed, but still on leader
    LeaderSwitch(u32), // Need to redirect to a different leader
    LeaderUnSelected,  // Need to wait for leader election
    NoOp,              // No log operation needed
}
