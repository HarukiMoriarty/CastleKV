use clap::Parser;
use std::path::PathBuf;
use tracing::info;

use common::{init_tracing, set_default_rust_log};
use server::config::ServerConfig;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "0", help = "Id of the current node")]
    node_id: u32,

    #[arg(
        long,
        short,
        default_value = "0.0.0.0:23000",
        help = "The address to listen on"
    )]
    listen_addr: String,

    #[arg(long, short, help = "Directory path for database files")]
    db_path: Option<PathBuf>,

    #[arg(long, default_value = "0.0.0.0:24000", help = "Address of the manager")]
    manager_addr: String,

    #[arg(long, default_value_t = false, help = "Enable database persistence")]
    persistence: bool,

    #[arg(long, help = "Batch size for write operations before flushing to disk")]
    batch_size: Option<usize>,

    #[arg(
        long,
        help = "Timeout in milliseconds before a batch is flushed even if not full"
    )]
    batch_timeout: Option<u64>,
}

impl From<Cli> for ServerConfig {
    fn from(cli: Cli) -> Self {
        ServerConfig::builder()
            .node_id(cli.node_id)
            .listen_addr(cli.listen_addr)
            .db_path(cli.db_path)
            .manager_addr(cli.manager_addr)
            .persistence_enabled(cli.persistence)
            .batch_size(cli.batch_size)
            .batch_timeout_ms(cli.batch_timeout)
            .build()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();
    let config = ServerConfig::from(cli);
    info!("{:#?}", config);

    server::run_server(&config).await
}
