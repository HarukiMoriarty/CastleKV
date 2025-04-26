use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use common::{init_tracing, set_default_rust_log};
use server::config::{ServerArgs, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    set_default_rust_log("info");
    init_tracing();

    // Parse command line arguments
    let args = ServerArgs::parse();

    // Load configuration from YAML file
    let mut config = ServerConfig::from_yaml_file(&args.config)
        .with_context(|| format!("Failed to load config from {}", args.config))?;

    // Override with command line arguments if provided
    config
        .override_from_args(&args)
        .context("Failed to override config with command line arguments")?;

    info!("Server configuration: {:#?}", config);

    // Connect to manager to get partition information
    server::connect_manager(&mut config).await?;

    // Run the server
    server::run_server(&config).await
}
