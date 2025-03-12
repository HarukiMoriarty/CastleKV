use common::{extract_key, form_key};
use rpc::gateway::{Operation, OperationResult};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::plan::Plan;
use crate::storage::StorageMessage;

pub struct KeyValueDb {
    memory_db: HashMap<String, Arc<RwLock<BTreeMap<u64, String>>>>,
    storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
}

impl KeyValueDb {
    pub fn new(
        db_path: Option<impl AsRef<Path>>,
        storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
        table_names: &HashSet<String>,
    ) -> Result<Self, sled::Error> {
        let mut memory_db = HashMap::new();

        // Initialize tables
        for table_name in table_names {
            memory_db.insert(table_name.clone(), Arc::new(RwLock::new(BTreeMap::new())));
            debug!("Created table '{}'", table_name);
        }

        // Load existing data from persistent storage into memory
        if let Some(path) = db_path {
            let persistent_db = sled::open(path)?;

            // Track loaded entries for each table
            let mut loaded_entries = 0;

            for result in persistent_db.iter().flatten() {
                let key_str = String::from_utf8(result.0.to_vec()).unwrap();
                let value_str = String::from_utf8(result.1.to_vec()).unwrap();

                let (table_name, key_num) = extract_key(&key_str).unwrap();

                // Check if table exists in our initialized tables
                if let Some(table_lock) = memory_db.get(&table_name) {
                    // Insert into the appropriate table
                    let mut table = table_lock.write().unwrap();
                    table.insert(key_num, value_str);
                    loaded_entries += 1;
                } else {
                    debug!("Table '{}' not found in provided table list", table_name);
                }
            }

            info!(
                "Loaded {} entries into {} tables from storage",
                loaded_entries,
                memory_db.len()
            );

            for (table_name, table) in &memory_db {
                let entries = table.read().unwrap().len();
                info!("Table '{}' has {} entries", table_name, entries);
            }
        }

        Ok(Self {
            memory_db,
            storage_tx,
        })
    }

    pub(crate) fn execute(&self, plan: &Plan) -> Vec<OperationResult> {
        let mut ops_results = Vec::new();

        for op in plan.ops.iter() {
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

    pub fn execute_operation(&self, op: &Operation, log_entry_id: Option<u64>) -> String {
        match op.name.to_uppercase().as_str() {
            "PUT" => self.execute_put(op, log_entry_id),
            "SWAP" => self.execute_swap(op, log_entry_id),
            "GET" => self.execute_get(op),
            "DELETE" => self.execute_delete(op, log_entry_id),
            "SCAN" => self.execute_scan(op),
            _ => panic!("Unsupported operation: {}", op.name),
        }
    }

    fn execute_put(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();
        let value = op.args[1].clone();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let found = table.insert(key_num, value.clone()).is_some();

            if let Some(storage_tx) = &self.storage_tx {
                storage_tx
                    .send(StorageMessage::Put {
                        log_entry_index,
                        key,
                        value,
                    })
                    .unwrap();
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

    fn execute_swap(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();
        let new_value = op.args[1].clone();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let old_value = table.insert(key_num, new_value.clone());

            if let Some(storage_tx) = &self.storage_tx {
                storage_tx
                    .send(StorageMessage::Put {
                        log_entry_index,
                        key,
                        value: new_value,
                    })
                    .unwrap();
            }

            match old_value {
                Some(val) => format!("SWAP {} {}", op.args[0], val),
                None => format!("SWAP {} null", op.args[0]),
            }
        } else {
            format!("ERROR: Table '{}' not found", table_name)
        }
    }

    fn execute_get(&self, op: &Operation) -> String {
        // Extract table and key, validation has already been done
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

    fn execute_delete(&self, op: &Operation, log_entry_index: Option<u64>) -> String {
        // Extract table and key, validation has already been done
        let (table_name, key_num) = extract_key(&op.args[0]).unwrap();
        let key = op.args[0].clone();

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table
            let mut table = table_lock.write().unwrap();
            let found = table.remove(&key_num).is_some();

            if let Some(storage_tx) = &self.storage_tx {
                storage_tx
                    .send(StorageMessage::Delete {
                        log_entry_index,
                        key,
                    })
                    .unwrap();
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

    fn execute_scan(&self, op: &Operation) -> String {
        // Extract table and key ranges, validation has already been done
        let (start_table, start_key) = extract_key(&op.args[0]).unwrap();
        let (end_table, end_key) = extract_key(&op.args[1]).unwrap();

        // Ensure tables match (should be validated earlier, but check again)
        if start_table != end_table {
            return format!(
                "ERROR: SCAN operation cannot span multiple tables: {} and {}",
                start_table, end_table
            );
        }

        let table_name = start_table;
        let mut result = format!("SCAN {} {} BEGIN", op.args[0], op.args[1]);

        // Check if table exists
        if let Some(table_lock) = self.memory_db.get(&table_name) {
            // Only lock this specific table for reading
            let table = table_lock.read().unwrap();
            for (key_num, value) in table.range(start_key..=end_key) {
                let key_str = form_key(&table_name, *key_num);
                result.push_str(&format!("\n  {} {}", key_str, value));
            }
        } else {
            debug!("Table {} not found for SCAN operation", table_name);
            // Table doesn't exist, return empty result
        }

        result.push_str("\nSCAN END");
        result
    }

    pub async fn sync(&self) -> Result<Option<u64>, oneshot::error::RecvError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if let Some(storage_tx) = &self.storage_tx {
            storage_tx.send(StorageMessage::Flush { reply_tx }).unwrap();
        }
        reply_rx.await
    }
}
