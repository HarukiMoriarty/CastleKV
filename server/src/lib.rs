pub mod config;
pub mod database;
mod executor;
mod gateway;
mod lock_manager;
pub mod log_manager;
pub mod storage;

use config::ServerConfig;
use database::KeyValueDb;
use executor::Executor;
use gateway::GatewayService;
use lock_manager::LockManager;
use log_manager::LogManager;
use std::sync::Arc;
use storage::Storage;
use tokio::sync::mpsc;
use tracing::info;

pub async fn run_server(config: &ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (lock_mananger_tx, lock_manager_rx) = mpsc::unbounded_channel();
    let (storage_tx, storage_rx) = if config.persistence_enabled {
        let (storage_tx, storage_rx) = mpsc::unbounded_channel();
        (Some(storage_tx), Some(storage_rx))
    } else {
        (None, None)
    };
    let (log_manager_tx, log_manager_rx) = mpsc::unbounded_channel();

    // Start executor.
    let db = Arc::new(KeyValueDb::new(config.db_path.clone(), storage_tx.clone())?);
    let executor = Executor::new(
        config.node_id,
        executor_rx,
        log_manager_tx,
        lock_mananger_tx,
        db.clone(),
    );
    tokio::spawn(executor.run());
    info!("Start executor");

    if config.persistence_enabled {
        // Start storage.
        let storage = Storage::new(config, storage_rx.unwrap())?;
        tokio::spawn(storage.run());
        info!("Start storage service");
    }

    // Start log manager.
    let log_manager = LogManager::new(config.log_path.clone(), log_manager_rx, db.clone(), 1024);
    tokio::spawn(log_manager.run());
    info!("Start log manager");

    // Start lock manager.
    let lock_manager = LockManager::new(lock_manager_rx);
    tokio::spawn(lock_manager.run());
    info!("Start lock manager");

    // Start gateway.
    let addr = config.listen_addr.parse()?;
    let gateway = GatewayService::new(executor_tx);

    info!("Start gateway on {}", addr);
    tonic::transport::Server::builder()
        .add_service(rpc::gateway::db_server::DbServer::new(gateway))
        .serve(addr)
        .await?;

    Ok(())
}
