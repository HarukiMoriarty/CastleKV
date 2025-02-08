use common::TransactionId;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

pub type LockManagerSender = mpsc::UnboundedSender<LockManagerMessage>;

pub enum LockManagerMessage {
    AcquireLock {
        txn_id: TransactionId,
        key: String,
        mode: LockMode,
        resp_tx: oneshot::Sender<bool>,
    },
    ReleaseLocks {
        txn_id: TransactionId,
    },
}

struct WoundWaitLock {
    mode: LockMode,
    owners: Vec<u64>,
    waiters: VecDeque<(u64, LockMode)>,
}

pub struct LockManager {
    locks: HashMap<String, WoundWaitLock>,
    txn_channels: HashMap<u64, Vec<oneshot::Sender<bool>>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            txn_channels: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<LockManagerMessage>) {
        info!("Lock manager started");

        while let Some(msg) = rx.recv().await {
            match msg {
                LockManagerMessage::AcquireLock {
                    txn_id,
                    key,
                    mode,
                    resp_tx,
                } => {
                    debug!(
                        "Lock request: txn {:?} requesting {:?} lock on {:?}",
                        txn_id, mode, key
                    );

                    let success = self.try_acquire_lock(txn_id, &key, mode);
                    let _ = resp_tx.send(success);
                }

                LockManagerMessage::ReleaseLocks { txn_id } => {
                    debug!("Releasing all locks for txn {}", txn_id);
                    self.release_locks(txn_id);
                }
            }
        }
    }

    fn try_acquire_lock(&mut self, txn_id: TransactionId, key: &str, mode: LockMode) -> bool {
        // Implementation
        true
    }

    fn release_locks(&mut self, txn_id: TransactionId) {
        // Implementation
        todo!()
    }
}
