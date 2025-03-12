use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info};

use rpc::manager::manager_service_server::{ManagerService, ManagerServiceServer};
use rpc::manager::{
    GetPartitionMapRequest, GetPartitionMapResponse, PartitionInfo, RegisterServerRequest,
    RegisterServerResponse,
};

pub struct Manager {
    server_addresses: Vec<String>,
    partition_assignments: Arc<RwLock<HashMap<String, Vec<PartitionInfo>>>>,
}

impl Manager {
    pub fn new(server_addresses: Vec<String>, tables: &HashMap<String, u64>) -> Self {
        let manager = Self {
            server_addresses: server_addresses.clone(),
            partition_assignments: Arc::new(RwLock::new(HashMap::new())),
        };

        manager.initialize_partitions(tables);

        // Log initialization info
        let tables_info: Vec<String> = tables
            .iter()
            .map(|(name, keys)| format!("{}={}", name, keys))
            .collect();

        info!(
            "Manager initialized with {} servers, tables: {}",
            server_addresses.len(),
            tables_info.join(", ")
        );

        manager
    }

    fn initialize_partitions(&self, tables: &HashMap<String, u64>) {
        let server_count = self.server_addresses.len();
        let mut assignments = self.partition_assignments.write().unwrap();

        // Initialize empty partition list for each server
        for server_addr in &self.server_addresses {
            assignments.insert(server_addr.clone(), Vec::new());
        }

        // For each table, assign partitions across servers
        for (table_name, key_num) in tables {
            let partition_size = *key_num / server_count as u64;

            for i in 0..server_count {
                let start_key = i as u64 * partition_size;
                let end_key = if i == server_count - 1 {
                    *key_num // Last server gets remainder
                } else {
                    (i as u64 + 1) * partition_size
                };

                let server_addr = &self.server_addresses[i];

                // Create partition info
                let partition_info = PartitionInfo {
                    table_name: table_name.clone(),
                    server_address: server_addr.clone(),
                    start_key,
                    end_key,
                };

                // Add partition to server's assignments
                if let Some(server_partitions) = assignments.get_mut(server_addr) {
                    server_partitions.push(partition_info);
                }

                info!(
                    "Server {} assigned table {} partition range: {} to {}",
                    server_addr, table_name, start_key, end_key
                );
            }
        }
    }
}

#[tonic::async_trait]
impl ManagerService for Manager {
    async fn register_server(
        &self,
        request: Request<RegisterServerRequest>,
    ) -> Result<Response<RegisterServerResponse>, Status> {
        let req_inner = request.into_inner();
        let server_address = req_inner.server_address;

        let assignments = self.partition_assignments.read().unwrap();

        match assignments.get(&server_address) {
            Some(server_partitions) => {
                // Filter partitions to only include requested tables (if specified)
                let mut assigned_partitions = Vec::new();

                for partition in server_partitions {
                    assigned_partitions.push(partition.clone());
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

pub async fn run_manager(addr: String, manager: Manager) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;

    info!("Start manager on {}", addr);

    Server::builder()
        .add_service(ManagerServiceServer::new(manager))
        .serve(addr)
        .await?;

    Ok(())
}
