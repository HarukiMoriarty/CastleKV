mod executor;
mod gateway;
mod lock_manager;

use executor::Executor;
use gateway::GatewayService;
use tokio::sync::mpsc;
use tracing::info;

pub async fn run_server(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let (executor_tx, executor_rx) = mpsc::unbounded_channel();

    // Start executor.
    let executor = Executor::new(executor_rx);
    tokio::spawn(executor.run());
    info!("Started executor");

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
