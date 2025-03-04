use crate::{metadata, CommandId};
use anyhow::{bail, ensure, Context, Result};
use rpc::gateway::db_client::DbClient;
use rpc::gateway::{Command, CommandResult, Operation, Status};
use rpc::manager::manager_service_client::ManagerServiceClient;
use rpc::manager::{GetPartitionMapRequest, PartitionInfo};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};

enum Client {
    Remote {
        txs: HashMap<String, mpsc::Sender<Command>>,
        rxs: HashMap<String, Streaming<CommandResult>>,
    },
    DryRun,
}

/// A session that wraps a connection to the gateway.
///
/// The session provides methods to execute commands and transactions.
pub struct Session {
    client: Client,
    name: String,
    cmds: Option<HashMap<String, Command>>,
    partition_info: Vec<PartitionInfo>,
    next_op_id: Option<u32>,
}

impl Session {
    pub async fn remote(name: &str, manager_addr: String) -> Result<Self> {
        let mut manager_client = ManagerServiceClient::connect(manager_addr)
            .await
            .context("Failed to connect to manager")?;
        let response = manager_client
            .get_partition_map(GetPartitionMapRequest {})
            .await
            .context("Failed to get partition map")?;
        let partition_info = response.into_inner().partitions;

        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();

        for partition in &partition_info {
            let server_addr = format!("http://{}", partition.server_address);
            let mut db = DbClient::connect(server_addr).await.context(format!(
                "Failed to connect to server: {}",
                partition.server_address
            ))?;

            let (tx, rx) = mpsc::channel(100);

            let mut request = Request::new(ReceiverStream::new(rx));
            request
                .metadata_mut()
                .insert(metadata::SESSION_NAME, FromStr::from_str(name).unwrap());
            let rx = db
                .connect_executor(request)
                .await
                .context(format!(
                    "Failed to connect executor for server: {}",
                    partition.server_address
                ))?
                .into_inner();

            txs.insert(partition.server_address.clone(), tx);
            rxs.insert(partition.server_address.clone(), rx);
        }

        Ok(Self {
            client: Client::Remote { txs, rxs },
            cmds: None,
            name: name.to_owned(),
            partition_info,
            next_op_id: None,
        })
    }

    pub fn dry_run(name: &str) -> Self {
        Self {
            client: Client::DryRun,
            cmds: None,
            name: name.to_owned(),
            partition_info: Vec::new(),
            next_op_id: None,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn get_server_for_key(&self, key: &str) -> Option<&str> {
        let key_value = {
            if key.starts_with("key") {
                key.to_string()
            } else {
                return None;
            }
        };

        for partition in &self.partition_info {
            if key_value >= partition.start_key && key_value <= partition.end_key {
                return Some(&partition.server_address);
            }
        }
        None
    }

    /// Starts a new command with multiple operations.
    ///
    /// The command is not executed until [`finish_command`](Self::finish_command) is called. If a
    /// command is already in progress, return an error.
    pub fn new_command(&mut self) -> Result<&mut Self> {
        ensure!(
            self.cmds.is_none() && self.next_op_id.is_none(),
            "must finish previous command first"
        );
        self.cmds = Some(HashMap::new());
        self.next_op_id = Some(0);

        Ok(self)
    }

    /// Returns the id of the next operation.
    pub fn get_next_op_id(&mut self) -> Option<u32> {
        if let Some(next_op_id) = self.next_op_id {
            self.next_op_id = Some(next_op_id + 1);
            return Some(next_op_id);
        } else {
            return None;
        }
    }

    /// Adds an operation to the current command.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the operation.
    /// * `args` - The arguments of the operation.
    pub fn add_operation(&mut self, name: &str, args: &[String]) -> Result<&mut Self> {
        if self.cmds.is_none() {
            bail!("no command in progress");
        };

        if (name == "get" || name == "set" || name == "del" || name == "put" || name == "swap")
            && !args.is_empty()
        {
            // Single key operations
            let key = &args[0];
            let server_addr = self
                .get_server_for_key(key)
                .map(|s| s.to_string())
                .ok_or_else(|| anyhow::anyhow!("No partition server found for key: {}", key))?;

            let id = self.next_op_id.unwrap();
            self.next_op_id = Some(id + 1);

            let op = Operation {
                id,
                name: name.to_string(),
                args: args.to_vec(),
            };

            // Add the operation to the command for this specific server
            let cmd = self
                .cmds
                .as_mut()
                .unwrap()
                .entry(server_addr.to_string())
                .or_insert_with(|| Command {
                    cmd_id: CommandId::INVALID.into(),
                    ops: vec![],
                });
            cmd.ops.push(op);
        } else if name == "scan" && args.len() >= 2 {
            let start_key = &args[0];
            let end_key = &args[1];

            // Check if start and end are valid
            if start_key > end_key {
                bail!("Invalid scan range: start key must be <= end key");
            }

            // For each partition, create a scan operation if the range overlaps
            let mut added_to_any_server = false;
            let id = self.next_op_id.unwrap();
            self.next_op_id = Some(id + 1);

            for partition in &self.partition_info {
                if !(*end_key < partition.start_key || *start_key > partition.end_key) {
                    added_to_any_server = true;
                    let effective_start = if *start_key > partition.start_key {
                        start_key.clone()
                    } else {
                        partition.start_key.clone()
                    };

                    let effective_end = if *end_key < partition.end_key {
                        end_key.clone()
                    } else {
                        partition.end_key.clone()
                    };

                    // Create a new scan operation for this partition
                    let scan_args = vec![effective_start, effective_end];

                    let op = Operation {
                        id,
                        name: name.to_string(),
                        args: scan_args,
                    };

                    // Add the operation to this partition's server
                    let cmd = self
                        .cmds
                        .as_mut()
                        .unwrap()
                        .entry(partition.server_address.clone())
                        .or_insert_with(|| Command {
                            cmd_id: CommandId::INVALID.into(),
                            ops: vec![],
                        });
                    cmd.ops.push(op);
                }
            }

            if !added_to_any_server {
                bail!("Scan range does not overlap with any partition");
            }
        } else {
            bail!("Unknown operation");
        }

        Ok(self)
    }

    /// Finishes the current command and execute it.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    pub async fn finish_command(&mut self) -> Result<Vec<CommandResult>> {
        let Some(cmds) = self.cmds.take() else {
            bail!("no command in progress");
        };
        self.next_op_id = None;

        let mut results = Vec::new();

        for (server_addr, cmd) in cmds {
            if cmd.ops.is_empty() {
                continue;
            }

            let result = self.execute(&server_addr, cmd).await?;
            results.push(result);
        }

        Ok(results)
    }

    async fn execute(&mut self, server_addr: &str, cmd: Command) -> Result<CommandResult> {
        match &mut self.client {
            Client::Remote { txs, rxs } => {
                // Get the sender and receiver for this server
                let tx = txs
                    .get(server_addr)
                    .context(format!("No tx for server: {}", server_addr))?;
                let rx = rxs
                    .get_mut(server_addr)
                    .context(format!("No rx for server: {}", server_addr))?;

                // Send the command to the executor
                tx.send(cmd).await.context("Failed to send command")?;

                // Wait for the result
                let Some(cmd_result) = rx.next().await else {
                    bail!("connection to server {} closed", server_addr);
                };

                let cmd_result = cmd_result.context(format!(
                    "Failed to receive result from server: {}",
                    server_addr
                ))?;
                Ok(cmd_result)
            }
            Client::DryRun => {
                println!("[{}] Command on server {}", self.name, server_addr);
                for op in &cmd.ops {
                    println!("\t{} {:?}", op.name, op.args);
                }
                println!();
                Ok(CommandResult {
                    status: Status::Committed.into(),
                    content: format!("dry-run on server {}", server_addr),
                    ..Default::default()
                })
            }
        }
    }
}
