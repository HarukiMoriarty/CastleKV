use clap::Parser;
use std::path::PathBuf;
use tracing::info;

use common::{init_tracing, set_default_rust_log};
use server::config::ServerConfig;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "0", help = "Replica id of the current node")]
    replica_id: u32,

    #[arg(
        long,
        short,
        default_value = "0.0.0.0:23000",
        help = "The address to listen on from client"
    )]
    client_listen_addr: String,

    #[arg(
        long,
        short,
        default_value = "0.0.0.0:25000",
        help = "The address to listen on from replica peers"
    )]
    peer_listen_addr: String,

    #[arg(long, short, help = "Directory path for database files")]
    db_path: Option<PathBuf>,

    #[arg(long, help = "Directory path for log files")]
    log_path: Option<PathBuf>,

    #[arg(long, help = "Directory path for persistent state files")]
    persistent_state_path: Option<PathBuf>,

    #[arg(long, default_value = "0.0.0.0:24000", help = "Address of the manager")]
    manager_addr: String,

    #[arg(long, default_value = "", help = "Addresses of peer replicas")]
    peer_replica_addr: String,

    #[arg(long, default_value_t = false, help = "Enable database persistence")]
    persistence: bool,

    #[arg(long, help = "Batch size for write operations before flushing to disk")]
    batch_size: Option<usize>,

    #[arg(
        long,
        help = "Timeout in milliseconds before a batch is flushed even if not full"
    )]
    batch_timeout: Option<u64>,

    #[arg(long, default_value_t = 1024 * 1024, help = "Entry size of each log segment")]
    log_seg_entry_size: usize,
}

impl Cli {
    /// Validate command line arguments for consistency
    fn validate(&self) -> Result<(), String> {
        if self.persistence {
            // When persistence is enabled, certain parameters must be provided
            if self.db_path.is_none() {
                return Err("db_path must be specified when persistence is enabled".to_string());
            }
            if self.batch_size.is_none() {
                return Err("batch_size must be specified when persistence is enabled".to_string());
            }
            if self.batch_timeout.is_none() {
                return Err(
                    "batch_timeout must be specified when persistence is enabled".to_string(),
                );
            }
        } else {
            // When persistence is disabled, these parameters should not be provided
            if self.db_path.is_some() {
                return Err(
                    "db_path must not be specified when persistence is disabled".to_string()
                );
            }
            if self.batch_size.is_some() {
                return Err(
                    "batch_size must not be specified when persistence is disabled".to_string(),
                );
            }
            if self.batch_timeout.is_some() {
                return Err(
                    "batch_timeout must not be specified when persistence is disabled".to_string(),
                );
            }
        }
        Ok(())
    }
}

impl From<Cli> for ServerConfig {
    fn from(cli: Cli) -> Self {
        ServerConfig::builder()
            .node_id(cli.replica_id)
            .client_listen_addr(cli.client_listen_addr)
            .peer_listen_addr(cli.peer_listen_addr)
            .db_path(cli.db_path)
            .log_path(cli.log_path)
            .persistent_state_path(cli.persistent_state_path)
            .manager_addr(cli.manager_addr)
            .peer_replica_addr(cli.peer_replica_addr)
            .persistence_enabled(cli.persistence)
            .batch_size(cli.batch_size)
            .batch_timeout_ms(cli.batch_timeout)
            .log_seg_entry_size(cli.log_seg_entry_size)
            .build()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    set_default_rust_log("info");
    init_tracing();

    // Parse and validate command line arguments
    let cli = Cli::parse();
    if let Err(err) = cli.validate() {
        return Err(err.into());
    }

    // Convert to server configuration
    let mut config = ServerConfig::from(cli);
    info!("Server configuration: {:#?}", config);

    // Connect to manager to get partition information
    server::connect_manager(&mut config).await?;

    // Run the server
    server::run_server(&config).await
}
