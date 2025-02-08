use clap::Parser;
use common::{init_tracing, set_default_rust_log};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, short, default_value = "0.0.0.0:23000")]
    connect_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();
    server::run_server(cli.connect_addr).await
}
