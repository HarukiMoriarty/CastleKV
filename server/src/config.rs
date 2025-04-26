use anyhow::{ensure, Context, Result};
use clap::Parser;
use common::NodeId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
};

/// Server configuration parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// The partition id of current server
    pub partition_id: NodeId,

    /// The replica id of current server
    pub replica_id: NodeId,

    /// The address to listen on from client
    pub client_listen_addr: String,

    /// The address to listen on from peer replicas
    pub peer_listen_addr: String,

    /// Directory path for database files
    pub db_path: Option<PathBuf>,

    /// Directory path for log files
    pub log_path: Option<PathBuf>,

    /// Directory path for persistent state files
    pub persistent_state_path: Option<PathBuf>,

    /// Address of the manager node
    pub manager_addr: String,

    /// Address of the peer replicas
    pub peer_replica_addr: HashMap<u32, String>,

    /// Enable database persistence
    pub persistence_enabled: bool,

    /// Batch size for write operations before flushing to disk
    pub batch_size: Option<usize>,

    /// Timeout in milliseconds before a batch is flushed even if not full
    pub batch_timeout_ms: Option<u64>,

    /// Set of table names
    pub table_name: HashSet<String>,

    /// Partition information mapping table names to (start, end) partition key ranges
    pub partition_info: HashMap<String, (u64, u64)>,

    /// Entry size of each log segment
    pub log_seg_entry_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            partition_id: NodeId(0),
            replica_id: NodeId(0),
            client_listen_addr: "0.0.0.0:23000".to_string(),
            peer_listen_addr: "0.0.0.0:25000".to_string(),
            db_path: None,
            log_path: None,
            persistent_state_path: None,
            manager_addr: "0.0.0.0:24000".to_string(),
            peer_replica_addr: HashMap::new(),
            persistence_enabled: false,
            batch_size: None,
            batch_timeout_ms: None,
            table_name: HashSet::new(),
            partition_info: HashMap::new(),
            log_seg_entry_size: 1024 * 1024,
        }
    }
}

impl ServerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a builder to construct a ServerConfig with custom values
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::default()
    }

    /// Load configuration from a YAML file
    pub fn from_yaml_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;
        Self::from_yaml_str(&content)
    }

    /// Parse configuration from a YAML string
    pub fn from_yaml_str(yaml_str: &str) -> Result<Self> {
        let config: Self =
            serde_yaml::from_str(yaml_str).context("Failed to parse YAML configuration")?;
        config.validate()
    }

    /// Validate the configuration
    pub fn validate(self) -> Result<Self> {
        // Add validation checks
        ensure!(
            !self.client_listen_addr.is_empty(),
            "client_listen_addr must not be empty"
        );

        ensure!(
            !self.peer_listen_addr.is_empty(),
            "peer_listen_addr must not be empty"
        );

        ensure!(
            !self.manager_addr.is_empty(),
            "manager_addr must not be empty"
        );

        // If persistence is enabled, certain paths should be set
        if self.persistence_enabled {
            ensure!(
                self.db_path.is_some(),
                "db_path must be set when persistence is enabled"
            );

            ensure!(
                self.log_path.is_some(),
                "log_path must be set when persistence is enabled"
            );

            ensure!(
                self.persistent_state_path.is_some(),
                "persistent_state_path must be set when persistence is enabled"
            );
        }

        // Validate that log_seg_entry_size is reasonable
        ensure!(
            self.log_seg_entry_size > 0,
            "log_seg_entry_size must be greater than 0"
        );

        Ok(self)
    }

    /// Save configuration to a YAML file
    pub fn to_yaml_file(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let yaml =
            serde_yaml::to_string(self).context("Failed to serialize configuration to YAML")?;
        fs::write(&path, yaml)
            .with_context(|| format!("Failed to write config to file: {:?}", path.as_ref()))?;
        Ok(())
    }

    /// Override configuration values from command line arguments
    pub fn override_from_args(&mut self, args: &ServerArgs) -> Result<()> {
        if let Some(id) = args.partition_id {
            self.partition_id = NodeId(id);
        }

        if let Some(id) = args.replica_id {
            self.replica_id = NodeId(id);
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the partition ID
    pub fn partition_id(mut self, partition_id: u32) -> Self {
        self.config.partition_id = NodeId(partition_id);
        self
    }

    /// Set the replica ID
    pub fn replica_id(mut self, replica_id: u32) -> Self {
        self.config.replica_id = NodeId(replica_id);
        self
    }

    /// Set the client listen address
    pub fn client_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.client_listen_addr = addr.into();
        self
    }

    /// Set the peer replicas listen address
    pub fn peer_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.peer_listen_addr = addr.into();
        self
    }

    /// Set the persistent database path
    pub fn db_path(mut self, path: impl Into<Option<PathBuf>>) -> Self {
        self.config.db_path = path.into();
        self
    }

    /// Set the persistent log path
    pub fn log_path(mut self, path: impl Into<Option<PathBuf>>) -> Self {
        self.config.log_path = path.into();
        self
    }

    /// Set the persistent state path
    pub fn persistent_state_path(mut self, path: impl Into<Option<PathBuf>>) -> Self {
        self.config.persistent_state_path = path.into();
        self
    }

    /// Set the manager address
    pub fn manager_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.manager_addr = addr.into();
        self
    }

    /// Set peer replica addresses
    pub fn peer_replica_addr(mut self, addrs: impl Into<String>) -> Self {
        let my_id: u32 = self.config.replica_id.into();

        // Create peer address mapping, skipping our own ID
        self.config.peer_replica_addr = addrs
            .into()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .enumerate()
            .map(|(i, addr)| {
                let peer_id = if i as u32 >= my_id {
                    (i + 1) as u32
                } else {
                    i as u32
                };
                (peer_id, addr.trim().to_string())
            })
            .collect();

        self
    }

    /// Enable or disable persistence
    pub fn persistence_enabled(mut self, enabled: bool) -> Self {
        self.config.persistence_enabled = enabled;
        self
    }

    /// Set the batch size
    pub fn batch_size(mut self, size: impl Into<Option<usize>>) -> Self {
        self.config.batch_size = size.into();
        self
    }

    /// Set the batch timeout in milliseconds
    pub fn batch_timeout_ms(mut self, timeout: impl Into<Option<u64>>) -> Self {
        self.config.batch_timeout_ms = timeout.into();
        self
    }

    /// Add a table name
    pub fn add_table(mut self, table: impl Into<String>) -> Self {
        self.config.table_name.insert(table.into());
        self
    }

    /// Add multiple table names
    pub fn add_tables(mut self, tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for table in tables {
            self.config.table_name.insert(table.into());
        }
        self
    }

    /// Add partition information for a table
    pub fn add_partition(mut self, table: impl Into<String>, range: (u64, u64)) -> Self {
        self.config.partition_info.insert(table.into(), range);
        self
    }

    /// Set the entry size of each log segment
    pub fn log_seg_entry_size(mut self, size: usize) -> Self {
        self.config.log_seg_entry_size = size;
        self
    }

    /// Load configuration from a YAML file
    pub fn from_yaml_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let config = ServerConfig::from_yaml_file(path)?;
        Ok(Self { config })
    }

    pub fn build(self) -> ServerConfig {
        self.config
    }
}

// Command line arguments using clap
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct ServerArgs {
    /// Path to the YAML config file
    #[clap(long, short, help = "Config yaml path")]
    pub config: String,

    /// Partition ID of the server
    #[arg(long, short, help = "Partition id of the current node")]
    pub partition_id: Option<u32>,

    /// Replica ID of the server
    #[arg(long, short, help = "Replica id of the current node")]
    pub replica_id: Option<u32>,
}
