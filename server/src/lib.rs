mod comm;
mod database;
mod executor;
mod gateway;
mod lock_manager;
mod storage;

use database::KeyValueDb;
use executor::Executor;
use gateway::GatewayService;
use lock_manager::LockManager;
use storage::Storage;

use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;
use tracing::info;

pub async fn run_server(addr: String, db_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (lock_mananger_tx, lock_manager_rx) = mpsc::unbounded_channel();
    let (storage_tx, storage_rx) = mpsc::unbounded_channel();

    // Start executor.
    let db = Arc::new(KeyValueDb::new(&db_path, storage_tx)?);
    let executor = Executor::new(executor_rx, lock_mananger_tx, db);
    tokio::spawn(executor.run());
    info!("Started executor");

    // Start storage.
    let storage = Storage::new(&db_path, storage_rx)?;
    tokio::spawn(storage.run());
    info!("Started storage service");

    // Start lock manager.
    let lock_manager = LockManager::new(lock_manager_rx);
    tokio::spawn(lock_manager.run());
    info!("Started lock manager");

    // Start gateway.
    let addr = addr.parse()?;
    let gateway = GatewayService::new(executor_tx);

    info!("Starting gateway on {}", addr);
    tonic::transport::Server::builder()
        .add_service(rpc::gateway::db_server::DbServer::new(gateway))
        .serve(addr)
        .await?;

    Ok(())
}
