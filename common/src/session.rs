use anyhow::{bail, ensure, Context, Result};
use rpc::manager::manager_service_client::ManagerServiceClient;
use rpc::manager::{GetPartitionMapRequest, PartitionInfo};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Streaming};
use tracing::{debug, trace, warn};

use super::{extract_key, form_key, metadata, CommandId};
use rpc::gateway::db_client::DbClient;
use rpc::gateway::{Command, CommandResult, Operation, Status};

/// Represents a partition replica group
struct ReplicaGroup {
    /// Map of replica IDs to server addresses
    replicas: HashMap<u32, String>,
    /// The currently connected replica ID
    connected_replica_id: u32,
    /// The currently connected server address (cached for convenience)
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
        /// Map of partition IDs to ReplicaGroup
        partitions: HashMap<u32, ReplicaGroup>,
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
    cmds: Option<HashMap<u32, Command>>,
    /// Information about available partitions for key-to-replica lookup
    /// Structured as: table_name -> [(start_key, end_key, partition_id)]
    partition_map: HashMap<String, HashSet<(u64, u64, u32)>>,
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
        // Here partition == replica group
        let mut partition_map: HashMap<String, HashSet<(u64, u64, u32)>> = HashMap::new();
        let mut partition_by_id: HashMap<u32, Vec<PartitionInfo>> = HashMap::new();

        // First pass: identify unique key ranges by partition_id
        for partition_replica in &partition_info {
            let partition_id = partition_replica.partition_id;

            // Add to partition_by_id for connection setup
            // format: partition_id -> replica_group
            partition_by_id
                .entry(partition_id)
                .or_insert_with(Vec::new)
                .push(partition_replica.clone());

            // Add to partition_map for lookup
            partition_map
                .entry(partition_replica.table_name.clone())
                .or_insert_with(HashSet::new)
                .insert((
                    partition_replica.start_key,
                    partition_replica.end_key,
                    partition_id,
                ));
        }

        debug!("Created partition map {partition_map:?}");

        // For each replica group, connect to one server
        let mut partitions = HashMap::new();
        for (partition_id, partition_replicas) in &partition_by_id {
            if partition_replicas.is_empty() {
                continue;
            }

            // Map replica IDs to server addresses
            let mut replicas = HashMap::new();
            for replica in partition_replicas {
                replicas.insert(replica.replica_id, replica.server_address.clone());
            }

            if replicas.is_empty() {
                continue;
            }

            // Get first replica ID (preferably replica_id 0 which is the primary)
            let first_replica_id = replicas
                .keys()
                .min()
                .cloned()
                .unwrap_or_else(|| *replicas.keys().next().unwrap());

            let connected_server = replicas[&first_replica_id].clone();

            // Connect to the first server in the replica group
            let (tx, rx) = connect_to_server(&connected_server, name)
                .await
                .with_context(|| format!("Failed to connect to server: {}", connected_server))?;

            debug!("Connected to server: {connected_server} for partition: {partition_id}, replica: {first_replica_id}");

            // Create replica group
            let replica_group = ReplicaGroup {
                replicas,
                connected_replica_id: first_replica_id,
                connected_server,
                tx,
                rx,
            };

            partitions.insert(*partition_id, replica_group);
        }

        Ok(Self {
            client: Client::Remote { partitions },
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
    fn get_replica_for_key(&self, table_name: &str, key: u64) -> Option<u32> {
        if let Some(ranges) = self.partition_map.get(table_name) {
            for &(start_key, end_key, partition_id) in ranges {
                if key >= start_key && key < end_key {
                    return Some(partition_id);
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

        let partition_id = self.get_replica_for_key(&table_name, key).ok_or_else(|| {
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
        let cmd = cmds.entry(partition_id).or_insert_with(|| Command {
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
            for &(range_start, range_end, partition_id) in ranges {
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
                    let cmd = cmds.entry(partition_id).or_insert_with(|| Command {
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
        cmds: HashMap<u32, Command>,
    ) -> Result<Vec<CommandResult>> {
        let mut results = Vec::new();

        // Process each command for each replica
        for (partition_id, cmd) in cmds {
            // Skip empty commands
            if cmd.ops.is_empty() {
                continue;
            }

            results.extend(self.execute_command_on_partition(partition_id, cmd).await?);
        }

        Ok(results)
    }

    /// Execute a single command on a specific partition with retries
    async fn execute_command_on_partition(
        &mut self,
        partition_id: u32,
        cmd: Command,
    ) -> Result<Vec<CommandResult>> {
        let mut retry_count = 0;
        const MAX_RETRIES: usize = 10;

        // Loop until we succeed or exhaust retries
        loop {
            // Get server information needed for this attempt
            let (tx, server_name) = self.get_partition_tx_and_server(partition_id)?;

            // Send command to the server
            if let Err(send_err) = tx.send(cmd.clone()).await {
                warn!(
                    "Failed to send command to server: {}, error: {}. Attempting reconnect...",
                    server_name, send_err
                );

                // Try switching to another server
                self.try_next_replica_in_partition(partition_id).await?;
                continue;
            }

            // Get the partition's receiver to receive results
            let rx_opt = self.get_partition_rx(partition_id)?;

            // Wait for result
            match rx_opt.next().await {
                Some(Ok(result)) => {
                    // Check if we need to switch to a different leader
                    if result.status == Status::Leaderswitch.into() {
                        if retry_count >= MAX_RETRIES {
                            bail!("Max retries exceeded for leader switch");
                        }
                        retry_count += 1;

                        // Get the new leader replica ID from result content
                        if !result.content.is_empty() {
                            // Try to parse the content as u32 replica ID
                            match result.content.parse::<u32>() {
                                Ok(replica_id) => {
                                    // Try to switch to the specified replica
                                    match self
                                        .try_connect_to_replica(partition_id, replica_id)
                                        .await
                                    {
                                        Ok(()) => {
                                            debug!(
                                                "Switched to new leader: replica {} for partition {}",
                                                replica_id, partition_id
                                            );
                                            continue; // Try again with new leader
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to switch to replica {}: {}",
                                                replica_id, e
                                            );
                                            // Try another replica in the partition
                                            self.try_next_replica_in_partition(partition_id)
                                                .await?;
                                            continue;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to parse replica ID from {}: {}",
                                        result.content, e
                                    );
                                    // Try another replica in the partition
                                    self.try_next_replica_in_partition(partition_id).await?;
                                    continue;
                                }
                            }
                        } else {
                            // No leader specified, try another replica
                            self.try_next_replica_in_partition(partition_id).await?;
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

                    self.try_next_replica_in_partition(partition_id).await?;
                    continue;
                }
                None => {
                    warn!(
                        "Connection to server {} closed. Reconnecting...",
                        server_name
                    );

                    self.try_next_replica_in_partition(partition_id).await?;
                    continue;
                }
            }
        }
    }

    /// Get the tx sender for a partition
    fn get_partition_tx_and_server(
        &self,
        partition_id: u32,
    ) -> Result<(mpsc::Sender<Command>, String)> {
        match &self.client {
            Client::Remote { partitions } => {
                if let Some(partition) = partitions.get(&partition_id) {
                    Ok((partition.tx.clone(), partition.connected_server.clone()))
                } else {
                    bail!("No partition found with ID: {}", partition_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        }
    }

    /// Get the rx receiver for a partition
    fn get_partition_rx(&mut self, partition_id: u32) -> Result<&mut Streaming<CommandResult>> {
        match &mut self.client {
            Client::Remote { partitions } => {
                if let Some(partition) = partitions.get_mut(&partition_id) {
                    Ok(&mut partition.rx)
                } else {
                    bail!("No partition found with ID: {}", partition_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        }
    }

    /// Try to connect to a specific replica in the partition group
    async fn try_connect_to_replica(&mut self, partition_id: u32, replica_id: u32) -> Result<()> {
        // First get the server address for this replica without holding the borrow
        let server_addr = match &self.client {
            Client::Remote { partitions } => {
                if let Some(partition) = partitions.get(&partition_id) {
                    if let Some(server) = partition.replicas.get(&replica_id) {
                        server.clone()
                    } else {
                        bail!(
                            "Replica {} not found in partition {}",
                            replica_id,
                            partition_id
                        );
                    }
                } else {
                    bail!("No partition found with ID: {}", partition_id)
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        };

        // Check if we're already connected to this replica
        let already_connected = match &self.client {
            Client::Remote { partitions } => {
                if let Some(partition) = partitions.get(&partition_id) {
                    partition.connected_replica_id == replica_id
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
        let (tx, rx) = connect_to_server(&server_addr, &self.name)
            .await
            .with_context(|| format!("Failed to connect to server: {}", server_addr))?;

        // Update the replica with the new connection
        match &mut self.client {
            Client::Remote { partitions } => {
                if let Some(partition) = partitions.get_mut(&partition_id) {
                    partition.connected_replica_id = replica_id;
                    partition.connected_server = server_addr.clone();
                    partition.tx = tx;
                    partition.rx = rx;

                    debug!(
                        "Switched connection for partition {} to replica {} (server {})",
                        partition_id, replica_id, server_addr
                    );
                }
            }
            Client::DryRun => bail!("Not connected to remote servers"),
        };

        Ok(())
    }

    /// Try the next replica in the partition
    async fn try_next_replica_in_partition(&mut self, partition_id: u32) -> Result<()> {
        // Get the next replica to try without holding the borrow
        let (next_replica_id, ..) = {
            match &self.client {
                Client::Remote { partitions } => {
                    if let Some(partition) = partitions.get(&partition_id) {
                        // Get all replica IDs
                        let mut replica_ids: Vec<_> = partition.replicas.keys().cloned().collect();
                        replica_ids.sort();

                        // Find the current replica's position and get the next one (with wraparound)
                        let current_pos = replica_ids
                            .iter()
                            .position(|&id| id == partition.connected_replica_id)
                            .unwrap_or(0);
                        let next_pos = (current_pos + 1) % replica_ids.len();
                        let next_id = replica_ids[next_pos];

                        // Get the server address for the next replica
                        (next_id, partition.replicas[&next_id].clone())
                    } else {
                        bail!("No partition found with ID: {}", partition_id)
                    }
                }
                Client::DryRun => bail!("Not connected to remote servers"),
            }
        };

        // Try to switch to the next replica
        self.try_connect_to_replica(partition_id, next_replica_id)
            .await
    }

    /// Execute commands in dry run mode
    fn execute_dry_run_commands(
        cmds: HashMap<u32, Command>,
        session_name: &str,
    ) -> Result<Vec<CommandResult>> {
        let mut results = Vec::new();

        for (partition_id, cmd) in cmds {
            if cmd.ops.is_empty() {
                continue;
            }

            println!("[{}] Command on partition {}", session_name, partition_id);
            for op in &cmd.ops {
                println!("\t{} {:?}", op.name, op.args);
            }
            println!();

            results.push(CommandResult {
                status: Status::Committed.into(),
                content: format!("dry-run on partition {}", partition_id),
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
