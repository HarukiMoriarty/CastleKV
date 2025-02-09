mod executor;
mod gateway;
mod lock_manager;

use executor::Executor;
use gateway::GatewayService;
use lock_manager::LockManager;
use tokio::sync::mpsc;
use tracing::info;

pub async fn run_server(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();
    let (lock_mananger_tx, lock_manager_rx) = mpsc::unbounded_channel();

    // Start executor.
    let executor = Executor::new(executor_rx, lock_mananger_tx);
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
