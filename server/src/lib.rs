mod database;
mod executor;
mod gateway;
mod lock_manager;

use database::KeyValueDb;
use executor::Executor;
use gateway::GatewayService;
use lock_manager::LockManager;

use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;
use tracing::info;

pub async fn run_server(addr: String, db_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (lock_mananger_tx, lock_manager_rx) = mpsc::unbounded_channel();

    // Start executor.
    let db = Arc::new(KeyValueDb::new(db_path)?);
    let executor = Executor::new(executor_rx, lock_mananger_tx, db);
    tokio::spawn(executor.run());
    info!("Started executor");

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
