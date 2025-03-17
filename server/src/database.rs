use common::{extract_key, form_key};
use rpc::gateway::{Operation, OperationResult};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use crate::plan::Plan;
use crate::storage::StorageMessage;

/// In-memory key-value database with optional persistent storage
pub struct KeyValueDb {
    /// Table-based in-memory storage, using BTreeMap for ordered key storage
    memory_db: HashMap<String, Arc<RwLock<BTreeMap<u64, String>>>>,
    /// Channel for sending storage operations to the persistence layer
    storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
}

impl KeyValueDb {
    /// Create a new KeyValueDb instance
    ///
    /// # Arguments
    /// * `db_path` - Path to load content from persistent storage
    /// * `storage_tx` - Channel for sending storage operations
    /// * `table_names` - Set of table names
    pub fn new(
        db_path: Option<impl AsRef<Path>>,
        storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
        table_names: &HashSet<String>,
    ) -> Result<Self, sled::Error> {
        let mut memory_db = HashMap::new();

        // Initialize tables initilization
        for table_name in table_names {
            memory_db.insert(table_name.clone(), Arc::new(RwLock::new(BTreeMap::new())));
            debug!("Created table '{}'", table_name);
        }

        // Load existing data from persistent storage into memory
        if let Some(path) = db_path {
            let persistent_db = sled::open(path)?;
            let mut loaded_entries = 0;

            for result in persistent_db.iter().flatten() {
                let key_str = String::from_utf8(result.0.to_vec()).unwrap();
                let value_str = String::from_utf8(result.1.to_vec()).unwrap();

                let (table_name, key_num) = extract_key(&key_str).unwrap();

                if let Some(table_lock) = memory_db.get(&table_name) {
                    // Insert into the appropriate table
                    let mut table = table_lock.write().unwrap();
                    table.insert(key_num, value_str);
                    loaded_entries += 1;
                } else {
                    error!("Table '{}' not found in provided table list", table_name);
                }
            }

            info!(
                "Loaded {} entries into {} tables from storage",
                loaded_entries,
                memory_db.len()
            );

            for (table_name, table) in &memory_db {
                let entries = table.read().unwrap().len();
                debug!("Table '{}' has {} entries", table_name, entries);
            }
        }

        Ok(Self {
            memory_db,
            storage_tx,
        })
    }

    /// Execute a plan containing multiple operations
    ///
    /// # Arguments
    /// * `plan` - The plan to execute
    pub(crate) fn execute(&self, plan: &Plan) -> Vec<OperationResult> {
        let mut ops_results = Vec::with_capacity(plan.ops.len());

        for op in &plan.ops {
            // Execute operation on the database
            let op_result = self.execute_operation(op, plan.log_entry_index);

            ops_results.push(OperationResult {
                id: op.id,
                content: op_result,
                has_err: false,
            });
        }

        ops_results
    }

    /// Execute a single operation
    ///
    /// # Arguments
    /// * `op` - The operation to execute
    /// * `log_entry_id` - Log entry ID for persistence; for reading operation, this field is empty
    pub fn execute_operation(&self, op: &Operation, log_entry_id: Option<u64>) -> String {
        match op.name.to_uppercase().as_str() {
            "PUT" => self.execute_put(op, log_entry_id),
            "SWAP" => self.execute_swap(op, log_entry_id),
            "GET" => self.execute_get(op),
            "DELETE" => self.execute_delete(op, log_entry_id),
            "SCAN" => self.execute_scan(op),
            _ => format!("ERROR: Unsupported operation: {}", op.name),
        }
    }

    /// Execute a PUT operation to insert or update a key-value pair
    fn execute_put(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done during planning
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();
        let value = op.args[1].clone();

        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let found = table.insert(key_num, value.clone()).is_some();

            // Send to persistent storage if enable
            if let Some(storage_tx) = &self.storage_tx {
                let _ = storage_tx.send(StorageMessage::Put {
                    log_entry_index,
                    key,
                    value,
                });
            }

            format!(
                "PUT {} {}",
                op.args[0],
                if found { "found" } else { "not_found" }
            )
        } else {
            format!("ERROR: Table '{}' not found", table_name)
        }
    }

    /// Execute a SWAP operation to update a key and return the previous value
    fn execute_swap(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done during planning
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();
        let new_value = op.args[1].clone();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let old_value = table.insert(key_num, new_value.clone());

            // Send to persistent storage if available
            if let Some(storage_tx) = &self.storage_tx {
                let _ = storage_tx.send(StorageMessage::Put {
                    log_entry_index,
                    key,
                    value: new_value,
                });
            }

            match old_value {
                Some(val) => format!("SWAP {} {}", op.args[0], val),
                None => format!("SWAP {} null", op.args[0]),
            }
        } else {
            format!("ERROR: Table '{}' not found", table_name)
        }
    }

    /// Execute a GET operation to retrieve a value
    fn execute_get(&self, op: &Operation) -> String {
        // Extract table and key, validation has already been done during planning
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table for reading
            let table = table_lock.read().unwrap();
            match table.get(&key_num) {
                Some(value) => format!("GET {} {}", op.args[0], value),
                None => format!("GET {} null", op.args[0]),
            }
        } else {
            format!("GET {} null", op.args[0])
        }
    }

    /// Execute a DELETE operation to remove a key-value pair
    fn execute_delete(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done during planning
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let found = table.remove(&key_num).is_some();

            // Send to persistent storage if available
            if let Some(storage_tx) = &self.storage_tx {
                let _ = storage_tx.send(StorageMessage::Delete {
                    log_entry_index,
                    key,
                });
            }

            format!(
                "DELETE {} {}",
                op.args[0],
                if found { "found" } else { "not_found" }
            )
        } else {
            format!("DELETE {} not_found", op.args[0])
        }
    }

    /// Execute a SCAN operation to retrieve a range of key-value pairs
    fn execute_scan(&self, op: &Operation) -> String {
        // Extract table and key ranges, validation has already been done during planning
        let (start_table, start_key) = extract_key(&op.args[0]).unwrap();
        let (_, end_key) = extract_key(&op.args[1]).unwrap();

        let table_name = start_table;
        let mut result = format!("SCAN {} {} BEGIN", op.args[0], op.args[1]);

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table for reading
            let table = table_lock.read().unwrap();
            for (key_num, value) in table.range(start_key..=end_key) {
                let key_str = form_key(&table_name, *key_num, None);
                result.push_str(&format!("\n  {} {}", key_str, value));
            }
        } else {
            debug!("Table {} not found for SCAN operation", table_name);
        }

        result.push_str("\nSCAN END");
        result
    }

    /// Synchronize in-memory data to persistent storage
    pub async fn sync(&self) -> Result<Option<u64>, oneshot::error::RecvError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        if let Some(storage_tx) = &self.storage_tx {
            let _ = storage_tx.send(StorageMessage::Flush { reply_tx });
        } else {
            // If no storage_tx, immediately resolve with None
            return Ok(None);
        }

        reply_rx.await
    }
}
