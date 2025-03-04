use clap::Parser;
use common::{init_tracing, set_default_rust_log};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "0.0.0.0:24000")]
    listen_addr: String,

    #[arg(long)]
    servers: String,

    #[arg(long, default_value = "1000000")]
    key_num: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_default_rust_log("info");
    init_tracing();

    let cli = Cli::parse();

    // Parse the server addresses
    let server_addresses: Vec<String> = cli
        .servers
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    if server_addresses.is_empty() {
        eprintln!("Error: no server addresses provided");
        return Err("No server addresses".into());
    }

    // Validate key_num parameter
    if cli.key_num == 0 {
        eprintln!("Error: key_num must be greater than 0");
        return Err("Invalid key_num".into());
    }

    let manager = manager::Manager::new(server_addresses, cli.key_num);

    // Start the manager service
    manager::run_manager(cli.listen_addr, manager).await?;

    Ok(())
}
