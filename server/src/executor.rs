use rpc::gateway::{Command, CommandResult, Status};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{debug, warn};

use crate::lock_manager::{LockManagerMessage, LockManagerSender, LockMode};
use common::CommandId;

pub type ExecutorSender = mpsc::UnboundedSender<ExecutorMessage>;
type ExecutorReceiver = mpsc::UnboundedReceiver<ExecutorMessage>;

pub enum ExecutorMessage {
    NewClient {
        stream: Streaming<Command>,
        result_tx: mpsc::Sender<Result<CommandResult, tonic::Status>>,
    },
}

pub struct Executor {
    rx: ExecutorReceiver,
    lock_manager_tx: LockManagerSender,
    cmd_cnt: Arc<AtomicU64>,
}

impl Executor {
    pub fn new(rx: ExecutorReceiver, lock_manager_tx: LockManagerSender) -> Self {
        Self {
            rx,
            lock_manager_tx,
            cmd_cnt: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ExecutorMessage::NewClient { stream, result_tx } => {
                    debug!("Executor: handling new client");

                    let lock_manager_tx = self.lock_manager_tx.clone();
                    let cmd_cnt = Arc::clone(&self.cmd_cnt);

                    tokio::spawn(async move {
                        let mut stream = stream;
                        while let Some(command) = stream.next().await {
                            match command {
                                Ok(cmd) => {
                                    debug!("Command details: {:?}", cmd);

                                    let lock_requests = cmd
                                        .ops
                                        .iter()
                                        .map(|op| {
                                            let mode = LockMode::Exclusive;
                                            (op.name.clone(), mode)
                                        })
                                        .collect::<Vec<_>>();

                                    let (lock_resp_tx, lock_resp_rx) = oneshot::channel();
                                    let cmd_id =
                                        CommandId::from(cmd_cnt.fetch_add(1, Ordering::SeqCst));

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
                                            let result = CommandResult {
                                                cmd_id: cmd_id.into(),
                                                ops: vec![],
                                                status: Status::Committed as i32,
                                                content: "Operation successful".to_string(),
                                                has_err: false,
                                            };

                                            // Release locks after operation
                                            let _ = lock_manager_tx
                                                .send(LockManagerMessage::ReleaseLocks { cmd_id });

                                            result
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
                                                has_err: true,
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
}
