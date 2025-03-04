use common::CommandId;
use tokio::sync::oneshot;

// Communication between in-memory database and disk storage.
pub enum StorageMessage {
    Put {
        cmd_id: CommandId,
        op_id: u32,
        key: String,
        value: String,
    },
    Delete {
        cmd_id: CommandId,
        op_id: u32,
        key: String,
    },
    Flush {
        reply_tx: oneshot::Sender<()>,
    },
}
