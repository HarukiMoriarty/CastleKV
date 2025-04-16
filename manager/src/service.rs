use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info};

use rpc::manager::manager_service_server::{ManagerService, ManagerServiceServer};
use rpc::manager::{
    GetPartitionMapRequest, GetPartitionMapResponse, PartitionInfo, RegisterServerRequest,
    RegisterServerResponse,
};

// Import the storage module
use crate::storage::ManagerStorage;

/// Manager for handling server registration and partition assignment
pub struct Manager {
    /// List of server addresses
    server_addresses: Vec<String>,
    /// Partition assignments for each server
    partition_assignments: Arc<RwLock<HashMap<String, Vec<PartitionInfo>>>>,
    /// Server replication factor
    replication_factor: u32,
    /// Storage for persistence
    storage: Option<ManagerStorage>,
}

impl Manager {
    /// Create a new Manager instance
    ///
    /// # Arguments
    ///
    /// * `server_addresses` - List of server addresses
    /// * `tables` - Map of table names to key space sizes
    /// * `replication_factor` - The replication factor for partitions
    /// * `storage_path` - Optional path for persistence
    pub fn new(
        server_addresses: Vec<String>,
        tables: &HashMap<String, u64>,
        replication_factor: u32,
        storage_path: Option<PathBuf>,
    ) -> Self {
        // Initialize storage if path is provided
        let storage = match storage_path {
            Some(path) => match ManagerStorage::new(path.clone()) {
                Ok(storage) => {
                    info!("Storage initialized at {:?}", path);
                    Some(storage)
                }
                Err(err) => {
                    error!("Failed to initialize storage: {}", err);
                    None
                }
            },
            None => None,
        };

        let manager = Self {
            server_addresses: server_addresses.clone(),
            partition_assignments: Arc::new(RwLock::new(HashMap::new())),
            replication_factor,
            storage,
        };

        manager.initialize_partitions(tables);

        // Save state to storage if available
        if let Some(storage) = &manager.storage {
            let assignments = manager.partition_assignments.read().unwrap();
            if let Err(err) = storage.save_partition_assignments(&assignments) {
                error!("Failed to save partition assignments: {}", err);
            }

            if let Err(err) = storage.save_server_addresses(&server_addresses) {
                error!("Failed to save server addresses: {}", err);
            }

            if let Err(err) = storage.save_replication_factor(replication_factor) {
                error!("Failed to save replication factor: {}", err);
            }
        }

        // Log initialization info
        let tables_info: Vec<String> = tables
            .iter()
            .map(|(name, keys)| format!("{}={}", name, keys))
            .collect();

        info!(
            "Manager initialized with {} servers, replication factor: {}, tables: {}",
            server_addresses.len(),
            replication_factor,
            tables_info.join(", ")
        );

        manager
    }

    /// Initialize partition assignments for all tables
    ///
    /// # Arguments
    ///
    /// * `tables` - Map of table names to key space sizes
    fn initialize_partitions(&self, tables: &HashMap<String, u64>) {
        let server_count = self.server_addresses.len();
        if server_count == 0 {
            error!("Cannot initialize partitions: no servers available");
            return;
        }

        // Calculate number of partitions based on server count and replication factor
        let partitions_count = server_count / self.replication_factor as usize;
        if partitions_count == 0 {
            error!("Not enough servers for the specified replication factor");
            return;
        }

        let mut assignments = self.partition_assignments.write().unwrap();

        // Initialize empty partition list for each server
        for server_addr in &self.server_addresses {
            assignments.insert(server_addr.clone(), Vec::new());
        }

        // For each table, assign partitions across servers
        for (table_name, key_num) in tables {
            let keys_per_partition = *key_num / partitions_count as u64;

            for partition_idx in 0..partitions_count {
                let start_key = partition_idx as u64 * keys_per_partition;
                let end_key = if partition_idx == partitions_count - 1 {
                    *key_num // Last partition gets remainder
                } else {
                    (partition_idx as u64 + 1) * keys_per_partition
                };

                // Assign to consecutive servers for each replica
                let base_server_idx = partition_idx * self.replication_factor as usize;

                // For each replica of this partition
                for replica_idx in 0..self.replication_factor as usize {
                    let server_idx = base_server_idx + replica_idx;
                    if server_idx >= server_count {
                        error!(
                            "Not enough servers for partition {} replica {}",
                            partition_idx, replica_idx
                        );
                        continue;
                    }

                    let server_addr = &self.server_addresses[server_idx];

                    // Create partition info
                    let partition_info = PartitionInfo {
                        table_name: table_name.clone(),
                        partition_id: partition_idx as u32,
                        replica_id: replica_idx as u32,
                        server_address: server_addr.clone(),
                        start_key,
                        end_key,
                    };

                    // Add partition to server's assignments
                    if let Some(server_partitions) = assignments.get_mut(server_addr) {
                        server_partitions.push(partition_info);
                    }

                    info!(
                    "Server {} assigned table {} key range: {} to {} (partition {}, replica {})",
                    server_addr, table_name, start_key, end_key, partition_idx, replica_idx
                );
                }
            }
        }
    }
}

#[tonic::async_trait]
impl ManagerService for Manager {
    /// Handle server registration requests
    async fn register_server(
        &self,
        request: Request<RegisterServerRequest>,
    ) -> Result<Response<RegisterServerResponse>, Status> {
        let req_inner = request.into_inner();
        let server_address = req_inner.server_address;

        let assignments = self.partition_assignments.read().unwrap();

        match assignments.get(&server_address) {
            Some(server_partitions) => {
                let assigned_partitions = server_partitions.clone();

                for assigned_partition in &assigned_partitions {
                    if assigned_partition.partition_id != req_inner.partition_id
                        || assigned_partition.replica_id != req_inner.replica_id
                    {
                        error!(
                            "Server tried to register with wrong partition/replica id: {}",
                            server_address
                        );

                        return Ok(Response::new(RegisterServerResponse {
                            assigned_partitions: Vec::new(),
                            has_err: true,
                        }));
                    }
                }

                info!(
                    "Server {} registered with {} table partitions",
                    server_address,
                    assigned_partitions.len()
                );

                Ok(Response::new(RegisterServerResponse {
                    assigned_partitions,
                    has_err: false,
                }))
            }
            None => {
                error!("Unknown server tried to register: {}", server_address);

                Ok(Response::new(RegisterServerResponse {
                    assigned_partitions: Vec::new(),
                    has_err: true,
                }))
            }
        }
    }

    /// Handle requests for the complete partition map
    async fn get_partition_map(
        &self,
        _request: Request<GetPartitionMapRequest>,
    ) -> Result<Response<GetPartitionMapResponse>, Status> {
        let assignments = self.partition_assignments.read().unwrap();
        let mut partitions = Vec::new();

        // Collect all partitions from all servers
        for server_partitions in assignments.values() {
            partitions.extend(server_partitions.clone());
        }

        Ok(Response::new(GetPartitionMapResponse {
            partitions,
            has_err: false,
        }))
    }
}

/// Run the manager service
///
/// # Arguments
///
/// * `addr` - Address to listen on
/// * `manager` - Manager instance
pub async fn run_manager(addr: String, manager: Manager) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;

    info!("Starting manager service on {}", addr);

    Server::builder()
        .add_service(ManagerServiceServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}
