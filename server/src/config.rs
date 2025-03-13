use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use common::NodeId;

/// Server configuration parameters
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The node id of current server
    pub node_id: NodeId,

    /// The address to listen on
    pub listen_addr: String,

    /// Directory path for database files
    pub db_path: Option<PathBuf>,

    /// Directory path for log files
    pub log_path: Option<PathBuf>,

    /// Address of the manager node
    pub manager_addr: String,

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(0),
            listen_addr: "0.0.0.0:23000".to_string(),
            db_path: None,
            log_path: None,
            manager_addr: "0.0.0.0:24000".to_string(),
            persistence_enabled: false,
            batch_size: None,
            batch_timeout_ms: None,
            table_name: HashSet::new(),
            partition_info: HashMap::new(),
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
}

/// Builder for creating ServerConfig with custom values
#[derive(Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the node ID
    pub fn node_id(mut self, node_id: u32) -> Self {
        self.config.node_id = NodeId(node_id);
        self
    }

    /// Set the listen address
    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.listen_addr = addr.into();
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

    /// Set the manager address
    pub fn manager_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.manager_addr = addr.into();
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

    pub fn build(self) -> ServerConfig {
        self.config
    }
}
