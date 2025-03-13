use rpc::gateway::{Command, CommandResult, OperationResult, Status};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{debug, warn};

use crate::config::ServerConfig;
use crate::database::KeyValueDb;
use crate::lock_manager::{LockManagerMessage, LockManagerSender, LockMode};
use crate::log_manager::{LogManagerMessage, LogManagerSender};
use crate::plan::Plan;
use common::CommandId;

pub type ExecutorSender = mpsc::UnboundedSender<ExecutorMessage>;
type ExecutorReceiver = mpsc::UnboundedReceiver<ExecutorMessage>;

pub enum ExecutorMessage {
    NewClient {
        stream: Streaming<Command>,
        result_tx: mpsc::UnboundedSender<Result<CommandResult, tonic::Status>>,
    },
}

pub struct Executor {
    config: ServerConfig,
    rx: ExecutorReceiver,
    log_manager_tx: LogManagerSender,
    lock_manager_tx: LockManagerSender,
    cmd_cnt: Arc<AtomicU32>,
    db: Arc<KeyValueDb>,
}

impl Executor {
    pub fn new(
        config: ServerConfig,
        rx: ExecutorReceiver,
        log_manager_tx: LogManagerSender,
        lock_manager_tx: LockManagerSender,
        db: Arc<KeyValueDb>,
    ) -> Self {
        Self {
            config,
            rx,
            log_manager_tx,
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

                    let log_manager_tx = self.log_manager_tx.clone();
                    let lock_manager_tx = self.lock_manager_tx.clone();
                    let cmd_cnt = Arc::clone(&self.cmd_cnt);
                    let db = Arc::clone(&self.db);

                    // Seperate clone to avoid wrong ownership
                    let partition_info = self.config.partition_info.clone();

                    tokio::spawn(async move {
                        let mut stream = stream;
                        while let Some(command) = stream.next().await {
                            match command {
                                Ok(cmd) => {
                                    debug!("Command details: {:?}", cmd);

                                    // Generate unique monotonical increasing command id
                                    let cmd_id = CommandId::new(
                                        self.config.node_id,
                                        cmd_cnt.fetch_add(1, Ordering::SeqCst),
                                    );

                                    // Generate Plan (validation)
                                    let mut plan =
                                        match Plan::from_command(&cmd, cmd_id, &partition_info) {
                                            Err(err) => {
                                                debug!("Command validation failed: {}", err);

                                                let result = CommandResult {
                                                    cmd_id: cmd_id.into(),
                                                    ops: vec![OperationResult {
                                                        id: 0,
                                                        content: err.clone(),
                                                        has_err: true,
                                                    }],
                                                    status: Status::Aborted as i32,
                                                    content: format!(
                                                        "Command validation failed: {}",
                                                        err
                                                    ),
                                                    has_err: true,
                                                };

                                                if result_tx.send(Ok(result)).is_err() {
                                                    warn!("Client disconnected");
                                                    break;
                                                }
                                                continue;
                                            }
                                            Ok(plan) => plan,
                                        };

                                    let mut lock_requests = Vec::new();
                                    for key in &plan.rw_set.read_set {
                                        lock_requests.push((key.clone(), LockMode::Shared));
                                    }
                                    for key in &plan.rw_set.write_set {
                                        lock_requests.push((key.clone(), LockMode::Exclusive));
                                    }

                                    let (lock_resp_tx, lock_resp_rx) = oneshot::channel();

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
                                            // Append log entry
                                            Self::append_raft_log(&mut plan, &log_manager_tx).await;

                                            // Locks acquired successfully
                                            let ops_results = db.execute(&plan);

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

                                    if result_tx.send(Ok(result)).is_err() {
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

    async fn append_raft_log(plan: &mut Plan, log_manager_tx: &LogManagerSender) {
        let mut log_ops: Vec<(String, String)> = Vec::new();
        let (log_resp_tx, log_resp_rx) = oneshot::channel::<u64>();

        for op in plan.ops.iter() {
            let op_name = op.name.clone();
            let op_args = op.args.clone();

            if op_name == "PUT" || op_name == "SWAP" {
                let key = op_args[0].clone();
                let value = op_args[1].clone();
                log_ops.push((key, value));
            } else if op_name == "DELETE" {
                let key = op_args[0].clone();
                log_ops.push((key, "NULL".to_string()));
            }
            // Other operations like GET or SCAN don't need to be logged
        }

        if !log_ops.is_empty() {
            let log_req = LogManagerMessage::AppendEntry {
                cmd_id: plan.cmd_id,
                ops: log_ops,
                resp_tx: log_resp_tx,
            };

            log_manager_tx.send(log_req).unwrap();

            // Wait for log response sync to ensure log entry is persisted
            plan.log_entry_index = Some(log_resp_rx.await.unwrap());
        }
    }
}
