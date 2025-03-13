use anyhow::{bail, ensure, Context, Result};
use rpc::manager::manager_service_client::ManagerServiceClient;
use rpc::manager::{GetPartitionMapRequest, PartitionInfo};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};
use tracing::debug;

use super::{extract_key, form_key, metadata, CommandId};
use rpc::gateway::db_client::DbClient;
use rpc::gateway::{Command, CommandResult, Operation, Status};

/// Client connection state
enum Client {
    /// Connected to remote servers
    Remote {
        /// Map of server addresses to command sender channels
        txs: HashMap<String, mpsc::Sender<Command>>,
        /// Map of server addresses to command result receiver streams
        rxs: HashMap<String, Streaming<CommandResult>>,
    },
    /// Dry run mode for testing/debugging
    DryRun,
}

/// A session that wraps a connection to the gateway.
/// The session provides methods to execute commands
/// across multiple partition servers in a distributed KV store.
pub struct Session {
    /// The client connection state
    client: Client,
    /// Name of the session
    name: String,
    /// Map of server to commands during command building
    cmds: Option<HashMap<String, Command>>,
    /// Information about available partitions and their server locations
    partition_info: Vec<PartitionInfo>,
    /// ID counter for the next operation
    next_op_id: Option<u32>,
}

impl Session {
    /// Creates a new remote session connected to partitioned servers.
    pub async fn remote(name: &str, manager_addr: String) -> Result<Self> {
        // Connect to the manager service to get partition information
        let mut manager_client = connect_to_manager(&manager_addr).await?;

        let response = manager_client
            .get_partition_map(GetPartitionMapRequest {})
            .await
            .context("Failed to get partition map")?;

        let partition_info = response.into_inner().partitions;
        debug!("Got partition info: {partition_info:#?}");

        // Connect to each unique partition server
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        let mut unique_servers = HashMap::new();

        for partition in &partition_info {
            unique_servers.insert(partition.server_address.clone(), true);
        }

        for server_address in unique_servers.keys() {
            connect_to_server(server_address, name, &mut txs, &mut rxs)
                .await
                .with_context(|| format!("Failed to connect to server: {}", server_address))?;
        }

        Ok(Self {
            client: Client::Remote { txs, rxs },
            cmds: None,
            name: name.to_owned(),
            partition_info,
            next_op_id: None,
        })
    }

    /// Creates a new session in dry run mode that logs operations
    /// without executing them. For testing and debugging.
    pub fn dry_run(name: &str) -> Self {
        Self {
            client: Client::DryRun,
            cmds: None,
            name: name.to_owned(),
            partition_info: Vec::new(),
            next_op_id: None,
        }
    }

    /// Returns the name of the session
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Determines which server is responsible for a specific key in a specific table.
    fn get_server_for_key(&self, table_name: &str, key: u64) -> Option<&str> {
        for partition in &self.partition_info {
            if partition.table_name == table_name
                && key >= partition.start_key
                && key < partition.end_key
            {
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
            "Must finish previous command first"
        );

        self.cmds = Some(HashMap::new());
        self.next_op_id = Some(0);

        Ok(self)
    }

    /// Returns the id of the next operation and increments the counter.
    pub fn get_next_op_id(&mut self) -> Option<u32> {
        self.next_op_id
    }

    /// Adds an operation to the current command.
    ///
    /// Routes the operation to the appropriate server based on the keys involved.
    /// Handles both single (PUT, GET, DELETE, SWAP) operations and SCAN operations
    /// that may span multiple partitions.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    pub fn add_operation(&mut self, name: &str, args: &[String]) -> Result<&mut Self> {
        // Ensure a command is in progress
        if self.cmds.is_none() {
            bail!("No command in progress");
        }

        // Handle different operation types
        match name {
            // Single-key operations
            op @ ("GET" | "DELETE" | "PUT" | "SWAP") if !args.is_empty() => {
                self.add_single_key_operation(op, args)?;
            }

            // Scan operations
            "SCAN" if args.len() >= 2 => {
                self.add_scan_operation(args)?;
            }

            // Unknown or invalid operations
            _ => {
                bail!("Unknown operation or invalid arguments");
            }
        }

        Ok(self)
    }

    /// Helper method to add a single-key operation
    fn add_single_key_operation(&mut self, op_name: &str, args: &[String]) -> Result<()> {
        let (table_name, key) = extract_key(&args[0]).map_err(|e| anyhow::anyhow!(e))?;

        let server_addr = self
            .get_server_for_key(&table_name, key)
            .map(|s| s.to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No partition server found for table: {}, key: {}",
                    table_name,
                    key
                )
            })?;

        let id = self.next_op_id.unwrap();
        self.next_op_id = Some(id + 1);

        let op = Operation {
            id,
            name: op_name.to_string(),
            args: args.to_vec(),
        };

        // Add the operation to the command for this specific server
        let cmds = self.cmds.as_mut().unwrap();
        let cmd = cmds.entry(server_addr).or_insert_with(|| Command {
            cmd_id: CommandId::INVALID.into(),
            ops: vec![],
        });

        cmd.ops.push(op);

        Ok(())
    }

    /// Helper method to add a scan operation
    fn add_scan_operation(&mut self, args: &[String]) -> Result<()> {
        let (start_table, start_key) = extract_key(&args[0]).map_err(|e| anyhow::anyhow!(e))?;
        let (end_table, end_key) = extract_key(&args[1]).map_err(|e| anyhow::anyhow!(e))?;

        // Validate scan parameters
        if start_table != end_table {
            bail!("SCAN operation must operate within the same table");
        }

        if start_key > end_key {
            bail!("Invalid scan range: start key must be less than or equal to end key");
        }

        let table_name = start_table;
        let id = self.next_op_id.unwrap();
        self.next_op_id = Some(id + 1);

        // Track if we've added the operation to at least one server
        let mut added_to_any_server = false;
        let cmds = self.cmds.as_mut().unwrap();

        // Filter to only relevant partitions for this table
        let relevant_partitions: Vec<&PartitionInfo> = self
            .partition_info
            .iter()
            .filter(|p| p.table_name == table_name)
            .collect();

        for partition in relevant_partitions {
            // Check if scan range overlaps with this partition
            if !(end_key < partition.start_key || start_key >= partition.end_key) {
                added_to_any_server = true;

                // Calculate effective range for this partition (intersection of request and partition range)
                let effective_start = start_key.max(partition.start_key);
                let effective_end = end_key.min(partition.end_key - 1);

                // Create scan arguments for this partition
                let scan_args = vec![
                    form_key(&table_name, effective_start),
                    form_key(&table_name, effective_end),
                ];

                let op = Operation {
                    id,
                    name: "SCAN".to_string(),
                    args: scan_args,
                };

                // Add the operation to this partition's server
                let cmd = cmds
                    .entry(partition.server_address.clone())
                    .or_insert_with(|| Command {
                        cmd_id: CommandId::INVALID.into(),
                        ops: vec![],
                    });

                cmd.ops.push(op);
            }
        }

        if !added_to_any_server {
            bail!(
                "Scan range does not overlap with any partition for table: {}",
                table_name
            );
        }

        Ok(())
    }

    /// Finishes the current command and executes it on all relevant servers.
    ///
    /// This implementation optimizes command execution by sending all commands
    /// to their respective servers first, then collecting results.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    pub async fn finish_command(&mut self) -> Result<Vec<CommandResult>> {
        let Some(cmds) = self.cmds.take() else {
            bail!("No command in progress");
        };

        self.next_op_id = None;

        match &mut self.client {
            Client::Remote { txs, rxs } => {
                execute_remote_commands(cmds, &self.name, txs, rxs).await
            }

            Client::DryRun => execute_dry_run_commands(cmds, &self.name),
        }
    }
}

/// Execute commands on remote servers
async fn execute_remote_commands(
    cmds: HashMap<String, Command>,
    session_name: &str,
    txs: &mut HashMap<String, mpsc::Sender<Command>>,
    rxs: &mut HashMap<String, Streaming<CommandResult>>,
) -> Result<Vec<CommandResult>> {
    let mut results = Vec::new();

    for (server_addr, cmd) in cmds {
        // Skip empty commands
        if cmd.ops.is_empty() {
            continue;
        }

        loop {
            // Send command to server
            let tx = txs
                .get(&server_addr)
                .with_context(|| format!("No tx for server: {}", server_addr))?;

            if let Err(send_err) = tx.send(cmd.clone()).await {
                debug!(
                    "Failed to send command to server: {}, error: {}. Reconnecting...",
                    server_addr, send_err
                );

                // Connection broken, try to reconnect
                connect_to_server(&server_addr, session_name, txs, rxs)
                    .await
                    .with_context(|| format!("Failed to reconnect to server: {}", server_addr))?;
                continue;
            }

            // Receive the result
            let rx = rxs
                .get_mut(&server_addr)
                .with_context(|| format!("No rx for server: {}", server_addr))?;

            match rx.next().await {
                Some(Ok(result)) => {
                    results.push(result);
                    break;
                }
                Some(Err(e)) => {
                    debug!(
                        "Error receiving result from server: {}, error: {}. Reconnecting...",
                        server_addr, e
                    );

                    connect_to_server(&server_addr, session_name, txs, rxs)
                        .await
                        .with_context(|| {
                            format!("Failed to reconnect to server: {}", server_addr)
                        })?;
                }
                None => {
                    debug!(
                        "Connection to server {} closed. Reconnecting...",
                        server_addr
                    );

                    connect_to_server(&server_addr, session_name, txs, rxs)
                        .await
                        .with_context(|| {
                            format!("Failed to reconnect to server: {}", server_addr)
                        })?;
                }
            }
        }
    }

    Ok(results)
}

/// Execute commands in dry run mode
fn execute_dry_run_commands(
    cmds: HashMap<String, Command>,
    session_name: &str,
) -> Result<Vec<CommandResult>> {
    let mut results = Vec::new();

    for (server_addr, cmd) in cmds {
        if cmd.ops.is_empty() {
            continue;
        }

        println!("[{}] Command on server {}", session_name, server_addr);
        for op in &cmd.ops {
            println!("\t{} {:?}", op.name, op.args);
        }
        println!();

        results.push(CommandResult {
            status: Status::Committed.into(),
            content: format!("dry-run on server {}", server_addr),
            ..Default::default()
        });
    }

    Ok(results)
}

/// Connect to the manager service with retries
async fn connect_to_manager(
    manager_addr: &str,
) -> Result<ManagerServiceClient<tonic::transport::Channel>> {
    let formatted_addr = if !manager_addr.starts_with("http") {
        format!("http://{}", manager_addr)
    } else {
        manager_addr.to_string()
    };

    loop {
        match ManagerServiceClient::connect(formatted_addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                debug!(
                    "Failed to connect to manager: {}, error: {}. Retrying...",
                    manager_addr, e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Establishes a connection to a server and sets up the executor stream.
/// This function handles connection retries.
async fn connect_to_server(
    server_address: &str,
    name: &str,
    txs: &mut HashMap<String, mpsc::Sender<Command>>,
    rxs: &mut HashMap<String, Streaming<CommandResult>>,
) -> Result<()> {
    let server_addr = if !server_address.starts_with("http") {
        format!("http://{}", server_address)
    } else {
        server_address.to_string()
    };

    // Retry loop for the connection
    let mut db = loop {
        match DbClient::connect(server_addr.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                debug!(
                    "Failed to connect to server: {}, error: {}. Retrying...",
                    server_address, e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    };

    let (tx, rx) = mpsc::channel(100);

    let mut request = Request::new(ReceiverStream::new(rx));
    request
        .metadata_mut()
        .insert(metadata::SESSION_NAME, FromStr::from_str(name).unwrap());

    let rx = db
        .connect_executor(request)
        .await
        .with_context(|| format!("Failed to connect executor for server: {}", server_address))?
        .into_inner();

    txs.insert(server_address.to_string(), tx);
    rxs.insert(server_address.to_string(), rx);

    Ok(())
}
