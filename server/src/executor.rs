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
use crate::log_manager::{AppendLogResult, LogManagerMessage, LogManagerSender};
use crate::metrics::COMMAND_DURATION;
use crate::plan::Plan;
use common::CommandId;
use rpc::gateway::Operation;

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
                                Ok(mut cmd) => {
                                    let _timer = COMMAND_DURATION
                                        .with_label_values(&[] as &[&str])
                                        .start_timer();

                                    debug!("Command details: {:?}", cmd);

                                    // Generate unique monotonical increasing command id
                                    let cmd_id = CommandId::new(
                                        self.config.replica_id,
                                        cmd_cnt.fetch_add(1, Ordering::SeqCst),
                                    );
                                    // Set cmd id
                                    cmd.cmd_id = cmd_id.into();

                                    // Generate Plan (validation)
                                    let mut plan =
                                        match Plan::from_client_command(&cmd, &partition_info) {
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
                                            let append_log_result =
                                                Self::append_raft_log(&mut plan, &log_manager_tx)
                                                    .await;

                                            let (status, content, ops_results) =
                                                match append_log_result {
                                                    AppendLogResult::Success(log_entry_index) => {
                                                        plan.log_entry_index =
                                                            Some(log_entry_index);
                                                        (
                                                            Status::Committed,
                                                            "Operation successful".to_string(),
                                                            db.execute(&plan),
                                                        )
                                                    }
                                                    AppendLogResult::Failure => (
                                                        Status::Aborted,
                                                        "Operation failed".to_string(),
                                                        vec![],
                                                    ),
                                                    AppendLogResult::LeaderSwitch(leader_id) => (
                                                        Status::Leaderswitch,
                                                        format!("{}", leader_id),
                                                        vec![],
                                                    ),
                                                    AppendLogResult::NoOp => (
                                                        Status::Committed,
                                                        "Operation successful".to_string(),
                                                        db.execute(&plan),
                                                    ),
                                                    AppendLogResult::LeaderUnSelected => {
                                                        unreachable!()
                                                    }
                                                };

                                            // Release locks after operation
                                            let _ = lock_manager_tx
                                                .send(LockManagerMessage::ReleaseLocks { cmd_id });

                                            CommandResult {
                                                cmd_id: cmd_id.into(),
                                                ops: ops_results,
                                                status: status.into(),
                                                content,
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

                                    debug!("Send result back to client");
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

    async fn append_raft_log(
        plan: &mut Plan,
        log_manager_tx: &LogManagerSender,
    ) -> AppendLogResult {
        let mut log_ops: Vec<Operation> = Vec::new();

        // Extract operations that need to be logged
        for op in plan.ops.iter() {
            let op_name = op.name.clone();
            if op_name == "PUT" || op_name == "SWAP" || op_name == "DELETE" {
                log_ops.push(op.clone());
            }
        }

        // If no operations need logging, return NoOp
        if log_ops.is_empty() {
            return AppendLogResult::NoOp;
        }

        // Try appending with retry for LeaderUnSelected
        let mut retry_count = 0;
        const MAX_RETRIES: u8 = 3; // Limit the number of retries

        loop {
            let (log_resp_tx, log_resp_rx) = oneshot::channel::<AppendLogResult>();

            let log_req = LogManagerMessage::AppendEntry {
                cmd_id: plan.cmd_id,
                ops: log_ops.clone(),
                resp_tx: log_resp_tx,
            };

            log_manager_tx.send(log_req).unwrap();

            // Wait for response
            match log_resp_rx.await.unwrap() {
                AppendLogResult::LeaderUnSelected => {
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        // Give up after MAX_RETRIES attempts
                        debug!(
                            "Giving up after {} retries for leader selection",
                            MAX_RETRIES
                        );
                        return AppendLogResult::Failure;
                    }

                    debug!(
                        "Leader unselected, retrying in 1 second (attempt {}/{})",
                        retry_count, MAX_RETRIES
                    );

                    // Sleep for 200 millseconds before retrying
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    continue;
                }
                result => return result, // For any other result, return immediately
            }
        }
    }
}
