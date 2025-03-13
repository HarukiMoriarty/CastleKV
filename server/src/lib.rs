pub mod config;
pub mod database;
mod executor;
mod gateway;
mod lock_manager;
pub mod log_manager;
pub mod plan;
pub mod storage;

use anyhow::Context;
use config::ServerConfig;
use database::KeyValueDb;
use executor::Executor;
use gateway::GatewayService;
use lock_manager::LockManager;
use log_manager::LogManager;
use rpc::manager::{manager_service_client::ManagerServiceClient, RegisterServerRequest};
use std::sync::Arc;
use storage::Storage;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Connect to the manager and register this server
///
/// # Arguments
///
/// * `config` - Server configuration, will be updated with partition information
pub async fn connect_manager(config: &mut ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let manager_addr = format!("http://{}", config.manager_addr);

    // Attempt to connect to manager with retry
    let mut manager_client = loop {
        match ManagerServiceClient::connect(manager_addr.clone()).await {
            Ok(manager_client) => break manager_client,
            Err(e) => {
                error!(
                    "Failed to connect to manager: {}, error: {}. Retrying...",
                    config.manager_addr, e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    };

    // Create a request with server address
    let request = RegisterServerRequest {
        server_address: config.listen_addr.clone(),
    };

    // Register this server with the manager
    let response = manager_client
        .register_server(request)
        .await
        .context("Failed to register server")?
        .into_inner();

    if response.has_err {
        return Err(format!(
            "Server address '{}' registration failed",
            config.listen_addr
        )
        .into());
    }

    // Clear existing partition info and table names
    config.partition_info.clear();
    config.table_name.clear();

    // Process assigned partitions from the response
    for partition in response.assigned_partitions {
        // Add the table name to the set
        assert!(config.table_name.insert(partition.table_name.clone()));

        // Add the partition range to the map
        config.partition_info.insert(
            partition.table_name.clone(),
            (partition.start_key, partition.end_key),
        );

        info!(
            "Server assigned table '{}' partition range: {} to {}",
            partition.table_name, partition.start_key, partition.end_key
        );
    }

    if config.table_name.is_empty() {
        return Err("No tables were assigned to this server".into());
    }

    info!("Server registered with {} tables", config.table_name.len());
    Ok(())
}

/// Run the server with all its components
///
/// # Arguments
///
/// * `config` - Server configuration
pub async fn run_server(config: &ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Set up communication channels between components
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (lock_manager_tx, lock_manager_rx) = mpsc::unbounded_channel();
    let (log_manager_tx, log_manager_rx) = mpsc::unbounded_channel();

    // Set up storage channels if persistence is enabled
    let (storage_tx, storage_rx) = if config.persistence_enabled {
        let (tx, rx) = mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Initialize the database
    let db = Arc::new(KeyValueDb::new(
        config.db_path.clone(),
        storage_tx,
        &config.table_name,
    )?);

    // Start executor
    let executor = Executor::new(
        config.clone(),
        executor_rx,
        log_manager_tx,
        lock_manager_tx,
        db.clone(),
    );
    tokio::spawn(executor.run());
    info!("Started executor service");

    // Start storage service if persistence is enabled
    if config.persistence_enabled {
        let storage = Storage::new(config, storage_rx.unwrap())?;
        tokio::spawn(storage.run());
        info!("Started storage service");
    }

    // Start log manager
    let log_manager = LogManager::new(config.log_path.clone(), log_manager_rx, db.clone(), 1024);
    tokio::spawn(log_manager.run());
    info!("Started log manager service");

    // Start lock manager
    let lock_manager = LockManager::new(lock_manager_rx);
    tokio::spawn(lock_manager.run());
    info!("Started lock manager service");

    // Start gateway (gRPC server)
    let addr = config.listen_addr.parse()?;
    let gateway = GatewayService::new(executor_tx);

    info!("Starting gateway server on {}", addr);
    tonic::transport::Server::builder()
        .add_service(rpc::gateway::db_server::DbServer::new(gateway))
        .serve(addr)
        .await?;

    Ok(())
}
