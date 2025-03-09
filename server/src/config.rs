use common::NodeId;
use std::path::PathBuf;

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

    /// Address of the manager
    pub manager_addr: String,

    /// Enable database persistence
    pub persistence_enabled: bool,

    /// Batch size for write operations before flushing to disk
    pub batch_size: Option<usize>,

    /// Timeout in milliseconds before a batch is flushed even if not full
    pub batch_timeout_ms: Option<u64>,
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
        }
    }
}

impl ServerConfig {
    /// Create a new ServerConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a builder to construct a ServerConfig with custom values
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::new()
    }
}

#[derive(Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
        }
    }

    pub fn node_id(mut self, node_id: u32) -> Self {
        self.config.node_id = NodeId(node_id);
        self
    }

    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.listen_addr = addr.into();
        self
    }

    pub fn db_path(mut self, path: Option<PathBuf>) -> Self {
        self.config.db_path = path;
        self
    }

    pub fn log_path(mut self, path: Option<PathBuf>) -> Self {
        self.config.log_path = path;
        self
    }

    pub fn manager_addr(mut self, addr: impl Into<String>) -> Self {
        self.config.manager_addr = addr.into();
        self
    }

    pub fn persistence_enabled(mut self, enabled: bool) -> Self {
        self.config.persistence_enabled = enabled;
        self
    }

    pub fn batch_size(mut self, size: Option<usize>) -> Self {
        self.config.batch_size = size;
        self
    }

    pub fn batch_timeout_ms(mut self, timeout: Option<u64>) -> Self {
        self.config.batch_timeout_ms = timeout;
        self
    }

    pub fn build(self) -> ServerConfig {
        self.config
    }
}
