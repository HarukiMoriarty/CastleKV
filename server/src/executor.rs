use rpc::gateway::{Command, CommandResult, Operation, OperationResult, Status};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{debug, warn};

use crate::database::KeyValueDb;
use crate::lock_manager::{LockManagerMessage, LockManagerSender, LockMode};
use common::{CommandId, NodeId};

pub type ExecutorSender = mpsc::UnboundedSender<ExecutorMessage>;
type ExecutorReceiver = mpsc::UnboundedReceiver<ExecutorMessage>;

pub enum ExecutorMessage {
    NewClient {
        stream: Streaming<Command>,
        result_tx: mpsc::Sender<Result<CommandResult, tonic::Status>>,
    },
}

pub struct Executor {
    node_id: NodeId,
    rx: ExecutorReceiver,
    lock_manager_tx: LockManagerSender,
    cmd_cnt: Arc<AtomicU32>,
    db: Arc<KeyValueDb>,
}

impl Executor {
    pub fn new(
        node_id: NodeId,
        rx: ExecutorReceiver,
        lock_manager_tx: LockManagerSender,
        db: Arc<KeyValueDb>,
    ) -> Self {
        Self {
            node_id,
            rx,
            lock_manager_tx,
            cmd_cnt: Arc::new(AtomicU32::new(1)),
            db,
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ExecutorMessage::NewClient { stream, result_tx } => {
                    debug!("Executor: handling new client");

                    let lock_manager_tx = self.lock_manager_tx.clone();
                    let cmd_cnt = Arc::clone(&self.cmd_cnt);
                    let db = Arc::clone(&self.db);

                    tokio::spawn(async move {
                        let mut stream = stream;
                        while let Some(command) = stream.next().await {
                            match command {
                                Ok(cmd) => {
                                    debug!("Command details: {:?}", cmd);

                                    let (read_set, write_set) = Self::calculate_rw_set(&cmd.ops);
                                    let mut lock_requests = Vec::new();
                                    for key in read_set {
                                        lock_requests.push((key, LockMode::Shared));
                                    }
                                    for key in write_set {
                                        lock_requests.push((key, LockMode::Exclusive));
                                    }

                                    let (lock_resp_tx, lock_resp_rx) = oneshot::channel();
                                    let cmd_id = CommandId::new(
                                        self.node_id,
                                        cmd_cnt.fetch_add(1, Ordering::SeqCst),
                                    );

                                    // Request locks
                                    if lock_manager_tx
                                        .send(LockManagerMessage::AcquireLocks {
                                            cmd_id,
                                            lock_requests,
                                            resp_tx: lock_resp_tx,
                                        })
                                        .is_err()
                                    {
                                        warn!("Lock manager disconnected");
                                        break;
                                    }

                                    // Wait for lock response
                                    let result = match lock_resp_rx.await {
                                        Ok(true) => {
                                            // Locks acquired successfully
                                            let mut ops_results = Vec::new();

                                            for op in cmd.ops.iter() {
                                                // Execute operation on the database
                                                let op_result = db.execute(op, cmd_id);

                                                ops_results.push(OperationResult {
                                                    id: op.id,
                                                    content: op_result,
                                                    has_err: false,
                                                });
                                            }

                                            // Release locks after operation
                                            let _ = lock_manager_tx
                                                .send(LockManagerMessage::ReleaseLocks { cmd_id });

                                            CommandResult {
                                                cmd_id: cmd_id.into(),
                                                ops: ops_results,
                                                status: Status::Committed as i32,
                                                content: "Operation successful".to_string(),
                                                has_err: false,
                                            }
                                        }
                                        Ok(false) => {
                                            // Command was aborted due to conflict
                                            // Send release message to cleanup any partial locks
                                            let _ = lock_manager_tx
                                                .send(LockManagerMessage::ReleaseLocks { cmd_id });

                                            CommandResult {
                                                cmd_id: cmd_id.into(),
                                                ops: vec![],
                                                status: Status::Aborted as i32,
                                                content: "Command aborted due to conflict"
                                                    .to_string(),
                                                has_err: false,
                                            }
                                        }
                                        Err(_) => {
                                            warn!("Lock manager response channel closed");
                                            // Also release locks in case of error
                                            let _ = lock_manager_tx
                                                .send(LockManagerMessage::ReleaseLocks { cmd_id });
                                            break;
                                        }
                                    };

                                    if result_tx.send(Ok(result)).await.is_err() {
                                        warn!("Client disconnected");
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("Error receiving command: {}", e);
                                    break;
                                }
                            }
                        }
                    });
                }
            }
        }
    }

    fn calculate_rw_set(ops: &[Operation]) -> (HashSet<String>, HashSet<String>) {
        let mut read_set = HashSet::new();
        let mut write_set = HashSet::new();

        for op in ops {
            match op.name.to_uppercase().as_str() {
                "PUT" | "SWAP" | "DELETE" => {
                    if op.args.len() >= 2 {
                        if read_set.contains(&op.args[0]) {
                            read_set.remove(&op.args[0]);
                        }
                        write_set.insert(op.args[0].clone());
                    }
                }
                "GET" => {
                    if op.args.len() >= 2 && !write_set.contains(&op.args[0]) {
                        read_set.insert(op.args[0].clone());
                    }
                }
                "SCAN" => {
                    if op.args.len() >= 2 {
                        if let (Some(start_num), Some(end_num)) = (
                            Self::extract_key_number(&op.args[0]),
                            Self::extract_key_number(&op.args[1]),
                        ) {
                            for num in start_num..=end_num {
                                if !write_set.contains(&format!("usertable_user{}", num)) {
                                    read_set.insert(format!("usertable_user{}", num));
                                }
                            }
                        }
                    }
                }
                _ => panic!("Unsupported operation: {}", op.name),
            }
        }

        (read_set, write_set)
    }

    fn extract_key_number(key: &str) -> Option<u32> {
        key.strip_prefix("usertable_user")
            .and_then(|num_str| num_str.parse().ok())
    }
}
