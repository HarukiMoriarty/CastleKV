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
    partition_assignments: Arc<RwLock<HashMap<String, (u64, u64)>>>,
    key_num: u64,
}

impl Manager {
    pub fn new(server_addresses: Vec<String>, key_num: u64) -> Self {
        let manager = Self {
            server_addresses: server_addresses.clone(),
            partition_assignments: Arc::new(RwLock::new(HashMap::new())),
            key_num,
        };

        manager.initialize_partitions();

        info!(
            "Manager initialized with {} servers, total keys: {}",
            server_addresses.len(),
            manager.key_num
        );

        manager
    }

    fn initialize_partitions(&self) {
        let server_count = self.server_addresses.len();
        let partition_size = self.key_num / server_count as u64;

        let mut assignments = self.partition_assignments.write().unwrap();

        for i in 0..server_count {
            let start_key = i as u64 * partition_size;
            let end_key = if i == server_count - 1 {
                self.key_num - 1 // Last server gets remainder
            } else {
                (i as u64 + 1) * partition_size - 1
            };

            let server_addr = &self.server_addresses[i];
            assignments.insert(server_addr.clone(), (start_key, end_key));
            info!(
                "Server {} assigned partition range: key{} to key{}",
                server_addr, start_key, end_key
            );
        }
    }
}

#[tonic::async_trait]
impl ManagerService for Manager {
    async fn register_server(
        &self,
        request: Request<RegisterServerRequest>,
    ) -> Result<Response<RegisterServerResponse>, Status> {
        let server_address = request.into_inner().server_address;

        let assignments = self.partition_assignments.read().unwrap();

        match assignments.get(&server_address) {
            Some((start_key, end_key)) => {
                info!(
                    "Server {} registered with partition range: {} to {}",
                    server_address, start_key, end_key
                );

                Ok(Response::new(RegisterServerResponse {
                    start_key: format!("key{}", start_key),
                    end_key: format!("key{}", end_key),
                    has_err: false,
                }))
            }
            None => {
                error!("Unknown server tried to register: {}", server_address);

                Ok(Response::new(RegisterServerResponse {
                    start_key: String::new(),
                    end_key: String::new(),
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

        for (server_addr, (start_key, end_key)) in assignments.iter() {
            partitions.push(PartitionInfo {
                server_address: server_addr.clone(),
                start_key: format!("key{}", start_key),
                end_key: format!("key{}", end_key),
            });
        }

        Ok(Response::new(GetPartitionMapResponse { partitions }))
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
