use clap::Parser;
use common::{init_tracing, set_default_rust_log};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, short, default_value = "0.0.0.0:23000")]
    connect_addr: String,

    #[arg(long, short, default_value = "data/db_data")]
    db_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();
    server::run_server(cli.connect_addr, cli.db_path).await
}
