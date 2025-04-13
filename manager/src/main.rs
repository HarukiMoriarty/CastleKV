use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

use common::{init_tracing, set_default_rust_log};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(
        long,
        default_value = "0.0.0.0:24000",
        help = "The address for the manager service to listen on"
    )]
    listen_addr: String,

    #[arg(long, help = "Comma-separated list of KV server addresses")]
    servers: String,

    #[arg(
        long,
        help = "Table configurations in format: table1=1000000,table2=2000000 where numbers represent the key space size"
    )]
    tables: String,

    #[arg(
        long,
        default_value = "3",
        help = "The replication factor of a partition"
    )]
    server_rf: u32,

    #[arg(
        long,
        help = "Path to durable storage directory under ./data/ (optional)"
    )]
    backer_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    set_default_rust_log("info");
    init_tracing();

    // Parse command line arguments
    let cli = Cli::parse();

    // Parse server addresses
    let server_addresses: Vec<String> = parse_server_addresses(&cli.servers)?;

    // Check if server count is divisible by replication factor
    if server_addresses.len() % cli.server_rf as usize != 0 {
        return Err(format!(
            "Number of servers ({}) must be divisible by replication factor ({})",
            server_addresses.len(),
            cli.server_rf
        )
        .into());
    }

    // Parse and validate table configurations
    let table_config = parse_tables_config(&cli.tables)?;
    validate_table_config(&table_config)?;

    // Log configuration
    tracing::info!("Starting manager with the following configuration:");
    tracing::info!("  Service address: {}", cli.listen_addr);
    tracing::info!("  Server replication factor: {}", cli.server_rf);
    if let Some(path) = &cli.backer_path {
        tracing::info!("  Backer path: {}", path);
    } else {
        tracing::info!("  Backer path: None (in-memory only)");
    }
    tracing::info!("  Server count: {}", server_addresses.len());
    tracing::info!("  Table count: {}", table_config.len());

    // Create storage path if backer_path is provided
    let storage_path = cli.backer_path.map(|path| PathBuf::from("data").join(path));

    // Create and run the manager with storage and replication factor
    let manager =
        manager::Manager::new(server_addresses, &table_config, cli.server_rf, storage_path);

    manager::run_manager(cli.listen_addr, manager).await?;

    Ok(())
}

/// Parse comma-separated server addresses into a vector
fn parse_server_addresses(servers_str: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let server_addresses: Vec<String> = servers_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if server_addresses.is_empty() {
        return Err("No server addresses provided".into());
    }

    Ok(server_addresses)
}

/// Validate the table configuration
fn validate_table_config(
    table_config: &HashMap<String, u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    for (table_name, key_num) in table_config {
        if *key_num == 0 {
            return Err(format!(
                "Invalid key space size for table '{}': cannot be zero",
                table_name
            )
            .into());
        }
    }
    Ok(())
}

/// Parse table configurations from a string
fn parse_tables_config(
    config_str: &str,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
    let mut table_configs = HashMap::new();

    for table_config in config_str.split(',') {
        let parts: Vec<&str> = table_config.trim().split('=').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid table configuration format: {}", table_config).into());
        }

        let table_name = parts[0].trim().to_string();
        if table_name.is_empty() {
            return Err("Empty table name is not allowed".into());
        }

        let key_num = parts[1].trim().parse::<u64>().map_err(|_| {
            format!(
                "Invalid key space size for table {}: {}",
                table_name, parts[1]
            )
        })?;

        table_configs.insert(table_name, key_num);
    }

    if table_configs.is_empty() {
        return Err("No table configurations provided".into());
    }

    Ok(table_configs)
}
