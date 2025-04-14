use anyhow::{bail, ensure, Context, Result};
use rpc::manager::manager_service_client::ManagerServiceClient;
use rpc::manager::{GetPartitionMapRequest, PartitionInfo};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};
use tracing::{debug, info, trace, warn};

use super::{extract_key, form_key, metadata, CommandId};
use rpc::gateway::db_client::DbClient;
use rpc::gateway::{Command, CommandResult, Operation, Status};

/// Represents a partition replica group
struct ReplicaGroup {
    /// All server addresses in this replica group
    servers: Vec<String>,
    /// The currently connected server address
    connected_server: String,
    /// Sender for commands to the currently connected server
    tx: mpsc::Sender<Command>,
    /// Receiver for results from the currently connected server
    rx: Streaming<CommandResult>,
}

/// Client connection state
enum Client {
    /// Connected to remote servers
    Remote {
        /// Map of replica IDs to ReplicaGroup
        replicas: HashMap<usize, ReplicaGroup>,
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
    /// Map of replica ID to commands during command building
    cmds: Option<HashMap<usize, Command>>,
    /// Information about available partitions for key-to-replica lookup
    /// Structured as: table_name -> [(start_key, end_key, replica_id)]
    partition_map: HashMap<String, Vec<(u64, u64, usize)>>,
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

        // Organize partitions by ranges and extract unique replica IDs
        let mut partition_map: HashMap<String, Vec<(u64, u64, usize)>> = HashMap::new();
        let mut replicas_by_id: HashMap<usize, Vec<PartitionInfo>> = HashMap::new();
        let mut next_replica_id = 0;
        let mut key_ranges: HashMap<(String, u64, u64), usize> = HashMap::new();

        // First pass: identify unique key ranges and assign replica IDs
        for partition in &partition_info {
            let key = (
                partition.table_name.clone(),
                partition.start_key,
                partition.end_key,
            );

            // If this key range doesn't have a replica ID yet, assign one
            if !key_ranges.contains_key(&key) {
                key_ranges.insert(key.clone(), next_replica_id);
                next_replica_id += 1;
            }

            let replica_id = *key_ranges.get(&key).unwrap();

            // Add to replicas_by_id for connection setup
            replicas_by_id
                .entry(replica_id)
                .or_insert_with(Vec::new)
                .push(partition.clone());

            // Add to partition_map for lookup
            partition_map
                .entry(key.0.clone())
                .or_insert_with(Vec::new)
                .push((key.1, key.2, replica_id));
        }

        // For each replica group, connect to one server
        let mut replicas = HashMap::new();
        for (replica_id, partitions) in &replicas_by_id {
            if partitions.is_empty() {
                continue;
            }

            // Get all unique servers in this replica group
            let mut servers_set = std::collections::HashSet::new();
            for partition in partitions {
                servers_set.insert(partition.server_address.clone());
            }

            let servers: Vec<String> = servers_set.into_iter().collect();

            if servers.is_empty() {
                continue;
            }

            // Connect to the first server in the replica group
            let connected_server = servers[0].clone();
            let (tx, rx) = connect_to_server(&connected_server, name)
                .await
                .with_context(|| format!("Failed to connect to server: {}", connected_server))?;

            // Create replica group
            let replica_group = ReplicaGroup {
                servers,
                connected_server,
                tx,
                rx,
            };

            replicas.insert(*replica_id, replica_group);
        }

        Ok(Self {
            client: Client::Remote { replicas },
            cmds: None,
            name: name.to_owned(),
            partition_map,
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
            partition_map: HashMap::new(),
            next_op_id: None,
        }
    }

    /// Returns the name of the session
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Determines which replica is responsible for a specific key in a specific table.
    fn get_replica_for_key(&self, table_name: &str, key: u64) -> Option<usize> {
        if let Some(ranges) = self.partition_map.get(table_name) {
            for &(start_key, end_key, replica_id) in ranges {
                if key >= start_key && key < end_key {
                    return Some(replica_id);
                }
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
    /// Routes the operation to the appropriate replica based on the keys involved.
    /// Handles both single (PUT, GET, DELETE, SWAP) operations and SCAN operations
    /// that may span multiple partitions.
    ///
    /// If no existing command builder previously created by [`new_command`](Self::new_command)
    /// exists, returns an error.
    pub fn add_operation(
        &mut self,
        name: &str,
        args: &[String],
        key_len: Option<usize>,
    ) -> Result<&mut Self> {
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
                self.add_scan_operation(args, key_len)?;
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

        let replica_id = self.get_replica_for_key(&table_name, key).ok_or_else(|| {
            anyhow::anyhow!("No replica found for table: {}, key: {}", table_name, key)
        })?;

        let id = self.next_op_id.unwrap();
        self.next_op_id = Some(id + 1);

        let op = Operation {
            id,
            name: op_name.to_string(),
            args: args.to_vec(),
        };

        // Add the operation to the command for this specific replica
        let cmds = self.cmds.as_mut().unwrap();
        let cmd = cmds.entry(replica_id).or_insert_with(|| Command {
            cmd_id: CommandId::INVALID.into(),
            ops: vec![],
        });

        cmd.ops.push(op);

        Ok(())
    }

    /// Helper method to add a scan operation
    fn add_scan_operation(&mut self, args: &[String], key_len: Option<usize>) -> Result<()> {
        let (start_table, mut start_key) = extract_key(&args[0]).map_err(|e| anyhow::anyhow!(e))?;
        let (end_table, mut end_key) = extract_key(&args[1]).map_err(|e| anyhow::anyhow!(e))?;

        // Validate scan parameters
        if start_table != end_table {
            bail!("SCAN operation must operate within the same table");
        }

        if start_key > end_key {
            trace!("start_key > end_key, swapping");
            std::mem::swap(&mut start_key, &mut end_key);
        }

        let table_name = start_table;
        let id = self.next_op_id.unwrap();
        self.next_op_id = Some(id + 1);

        // Track if we've added the operation to at least one replica
        let mut added_to_any_replica = false;
        let cmds = self.cmds.as_mut().unwrap();

        // Get all ranges for this table
        if let Some(ranges) = self.partition_map.get(&table_name) {
            // For each key range in this table
            for &(range_start, range_end, replica_id) in ranges {
                // Check if scan range overlaps with this range
                if !(end_key < range_start || start_key >= range_end) {
                    // Calculate effective range for this partition (intersection of request and partition range)
                    let effective_start = start_key.max(range_start);
                    let effective_end = end_key.min(range_end - 1);

                    // Create scan arguments for this partition
                    let scan_args = vec![
                        form_key(&table_name, effective_start, key_len),
                        form_key(&table_name, effective_end, key_len),
                    ];

                    let op = Operation {
                        id,
                        name: "SCAN".to_string(),
                        args: scan_args,
                    };

                    // Add the operation to this replica's command
                    let cmd = cmds.entry(replica_id).or_insert_with(|| Command {
                        cmd_id: CommandId::INVALID.into(),
                        ops: vec![],
                    });

                    cmd.ops.push(op);
                    added_to_any_replica = true;
                }
            }
        }

        if !added_to_any_replica {
            bail!(
                "Scan range does not overlap with any partition for table: {}",
                table_name
            );
        }

        Ok(())
    }

    /// Finishes the current command and executes it on all relevant replicas.
    pub async fn finish_command(&mut self) -> Result<Vec<CommandResult>> {
        let Some(cmds) = self.cmds.take() else {
            bail!("No command in progress");
        };

        self.next_op_id = None;

        match &mut self.client {
            Client::Remote { .. } => self.execute_remote_commands(cmds).await,
            Client::DryRun => Self::execute_dry_run_commands(cmds, &self.name),
        }
    }

    /// Execute commands on remote servers with transparent leader switching
    async fn execute_remote_commands(
        &mut self,
        cmds: HashMap<usize, Command>,
    ) -> Result<Vec<CommandResult>> {
        let mut results = Vec::new();

        // Process each command for each replica
        for (replica_id, cmd) in cmds {
            // Skip empty commands
            if cmd.ops.is_empty() {
                continue;
            }

            results.extend(self.execute_command_on_replica(replica_id, cmd).await?);
        }

        Ok(results)
    }

    /// Execute a single command on a specific replica with retries
    async fn execute_command_on_replica(
        &mut self,
        replica_id: usize,
        cmd: Command,
    ) -> Result<Vec<CommandResult>> {
        let mut retry_count = 0;
        const MAX_RETRIES: usize = 3;

        // Loop until we succeed or exhaust retries
        loop {
            // Get server information needed for this attempt
            let (tx, server_name) = self.get_replica_tx_and_server(replica_id)?;

            // Send command to the server
            if let Err(send_err) = tx.send(cmd.clone()).await {
                warn!(
                    "Failed to send command to server: {}, error: {}. Attempting reconnect...",
                    server_name, send_err
                );

                // Try switching to another server
                self.try_next_server_in_replica(replica_id).await?;
                continue;
            }

            // Get the replica's receiver to receive results
            let rx_opt = self.get_replica_rx(replica_id)?;

            // Wait for result
            match rx_opt.next().await {
                Some(Ok(result)) => {
                    // Check if we need to switch to a different leader
                    if result.status == Status::Leaderswitch.into() {
                        if retry_count >= MAX_RETRIES {
                            bail!("Max retries exceeded for leader switch");
                        }
                        retry_count += 1;

                        // Get the new leader address from result content
                        if !result.content.is_empty() {
                            // Try to switch to the specified leader
                            let new_server = result.content.clone();
                            match self.try_connect_to_server(replica_id, &new_server).await {
                                Ok(()) => {
                                    info!(
                                        "Switched to new leader: {} for replica {}",
                                        new_server, replica_id
                                    );
                                    continue; // Try again with new leader
                                }
                                Err(e) => {
                                    warn!("Failed to switch to leader {}: {}", new_server, e);
                                    // Try another server in the replica
                                    self.try_next_server_in_replica(replica_id).await?;
                                    continue;
                                }
                            }
                        } else {
                            // No leader specified, try another server
                            self.try_next_server_in_replica(replica_id).await?;
                            continue;
                        }
                    }

                    // Success!
                    return Ok(vec![result]);
                }
                Some(Err(e)) => {
                    warn!(
                        "Error receiving result from server: {}, error: {}. Reconnecting...",
                        server_name, e
                    );

                    self.try_next_server_in_replica(replica_id).await?;
                    continue;
                }
                None => {
                    warn!(
                        "Connection to server {} closed. Reconnecting...",
                        server_name
                    );

                    self.try_next_server_in_replica(replica_id).await?;
                    continue;
                }
            }
        }
    }

    /// Get the tx sender for a replica
    fn get_replica_tx_and_server(
        &self,
        replica_id: usize,
    ) -> Result<(mpsc::Sender<Command>, String)> {
        match &self.client {
            Client::Remote { replicas } => {
                if let Some(replica) = replicas.get(&replica_id) {
                    Ok((replica.tx.clone(), replica.connected_server.clone()))
                } else {
                    bail!("No replica found with ID: {}", replica_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        }
    }

    /// Get the rx receiver for a replica
    fn get_replica_rx(&mut self, replica_id: usize) -> Result<&mut Streaming<CommandResult>> {
        match &mut self.client {
            Client::Remote { replicas } => {
                if let Some(replica) = replicas.get_mut(&replica_id) {
                    Ok(&mut replica.rx)
                } else {
                    bail!("No replica found with ID: {}", replica_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        }
    }

    /// Try to connect to a specific server in the replica group
    async fn try_connect_to_server(&mut self, replica_id: usize, server_addr: &str) -> Result<()> {
        // First get servers list without holding the borrow
        let servers = match &self.client {
            Client::Remote { replicas } => {
                if let Some(replica) = replicas.get(&replica_id) {
                    replica.servers.clone()
                } else {
                    bail!("No replica found with ID: {}", replica_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        };

        // Check if the server is in this replica group
        if !servers.contains(&server_addr.to_string()) {
            bail!(
                "Server {} is not in replica group {}",
                server_addr,
                replica_id
            );
        }

        // Check if we're already connected to this server
        let already_connected = match &self.client {
            Client::Remote { replicas } => {
                if let Some(replica) = replicas.get(&replica_id) {
                    replica.connected_server == server_addr
                } else {
                    false
                }
            }
            Client::DryRun => false,
        };

        if already_connected {
            return Ok(());
        }

        // Connect to the new server
        let (tx, rx) = connect_to_server(server_addr, &self.name)
            .await
            .with_context(|| format!("Failed to connect to server: {}", server_addr))?;

        // Update the replica with the new connection
        match &mut self.client {
            Client::Remote { replicas } => {
                if let Some(replica) = replicas.get_mut(&replica_id) {
                    replica.connected_server = server_addr.to_string();
                    replica.tx = tx;
                    replica.rx = rx;

                    info!(
                        "Switched connection for replica {} to server {}",
                        replica_id, server_addr
                    );
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        };

        Ok(())
    }

    /// Try the next server in the replica group
    async fn try_next_server_in_replica(&mut self, replica_id: usize) -> Result<()> {
        // Get the next server to try without holding the borrow
        let next_server = {
            match &self.client {
                Client::Remote { replicas } => {
                    if let Some(replica) = replicas.get(&replica_id) {
                        let current_idx = replica
                            .servers
                            .iter()
                            .position(|s| s == &replica.connected_server)
                            .unwrap_or(0);

                        let next_idx = (current_idx + 1) % replica.servers.len();
                        replica.servers[next_idx].clone()
                    } else {
                        bail!("No replica found with ID: {}", replica_id)
                    }
                }
                Client::DryRun => bail!("Not connected to remote servers"),
            }
        };

        // Try to switch to the next server
        self.try_connect_to_server(replica_id, &next_server).await
    }

    /// Execute commands in dry run mode
    fn execute_dry_run_commands(
        cmds: HashMap<usize, Command>,
        session_name: &str,
    ) -> Result<Vec<CommandResult>> {
        let mut results = Vec::new();

        for (replica_id, cmd) in cmds {
            if cmd.ops.is_empty() {
                continue;
            }

            println!("[{}] Command on replica {}", session_name, replica_id);
            for op in &cmd.ops {
                println!("\t{} {:?}", op.name, op.args);
            }
            println!();

            results.push(CommandResult {
                status: Status::Committed.into(),
                content: format!("dry-run on replica {}", replica_id),
                ..Default::default()
            });
        }

        Ok(results)
    }
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
/// Returns the command sender and result receiver.
async fn connect_to_server(
    server_address: &str,
    session_name: &str,
) -> Result<(mpsc::Sender<Command>, Streaming<CommandResult>)> {
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
    request.metadata_mut().insert(
        metadata::SESSION_NAME,
        FromStr::from_str(session_name).unwrap(),
    );

    let rx = db
        .connect_executor(request)
        .await
        .with_context(|| format!("Failed to connect executor for server: {}", server_address))?
        .into_inner();

    Ok((tx, rx))
}
