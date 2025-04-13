use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use rpc::manager::PartitionInfo;
use sled::Db;

/// Storage manager for the manager's state
pub struct ManagerStorage {
    db: Db,
}

impl ManagerStorage {
    /// Create a new ManagerStorage
    ///
    /// # Arguments
    ///
    /// * `path` - The path to store the database
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(path).context("Failed to open sled database")?;
        Ok(Self { db })
    }
    
    /// Save partition assignments
    ///
    /// # Arguments
    ///
    /// * `assignments` - Map of server addresses to assigned partitions
    pub fn save_partition_assignments(
        &self,
        assignments: &HashMap<String, Vec<PartitionInfo>>,
    ) -> Result<()> {
        let data = bincode::serialize(assignments)
            .context("Failed to serialize partition assignments")?;
        self.db.insert("partition_assignments", data)
            .context("Failed to insert partition assignments into database")?;
        self.db.flush()
            .context("Failed to flush database after saving partition assignments")?;
        Ok(())
    }
    
    /// Save server addresses
    ///
    /// # Arguments
    ///
    /// * `addresses` - List of server addresses
    pub fn save_server_addresses(&self, addresses: &[String]) -> Result<()> {
        let data = bincode::serialize(addresses)
            .context("Failed to serialize server addresses")?;
        self.db.insert("server_addresses", data)
            .context("Failed to insert server addresses into database")?;
        self.db.flush()
            .context("Failed to flush database after saving server addresses")?;
        Ok(())
    }
    
    /// Save replication factor
    ///
    /// # Arguments
    ///
    /// * `replication_factor` - The replication factor
    pub fn save_replication_factor(&self, replication_factor: u32) -> Result<()> {
        let data = bincode::serialize(&replication_factor)
            .context("Failed to serialize replication factor")?;
        self.db.insert("replication_factor", data)
            .context("Failed to insert replication factor into database")?;
        self.db.flush()
            .context("Failed to flush database after saving replication factor")?;
        Ok(())
    }
}