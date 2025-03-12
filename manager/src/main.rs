use clap::Parser;
use std::collections::HashMap;

use common::{init_tracing, set_default_rust_log};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(
        long,
        default_value = "0.0.0.0:24000",
        help = "The address to listen on"
    )]
    listen_addr: String,

    #[arg(long, help = "The server list")]
    servers: String,

    #[arg(
        long,
        help = "Table configurations in format: table1=1000000,table2=2000000"
    )]
    tables: String,
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

    let table_config = parse_tables_config(&cli.tables)?;

    // Validate key_num parameter
    for (table_name, key_num) in &table_config {
        if *key_num == 0 {
            return Err(format!("Invalid key_num for table '{}'", table_name).into());
        }
    }

    let manager = manager::Manager::new(server_addresses, &table_config);

    // Start the manager service
    manager::run_manager(cli.listen_addr, manager).await?;

    Ok(())
}

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
        let key_num = parts[1]
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("Invalid key number for table {}: {}", table_name, parts[1]))?;

        table_configs.insert(table_name, key_num);
    }

    Ok(table_configs)
}
