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
            node_id: NodeId(0),
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
        self.config.peer_replica_addr = addrs
            .into()
            .split(',')
            .enumerate()
            .map(|(i, addr)| ((i + 1) as u32, addr.trim().to_string()))
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

    pub fn build(self) -> ServerConfig {
        self.config
    }
}
