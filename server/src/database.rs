use common::CommandId;
use rpc::gateway::Operation;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};

use crate::comm::StorageMessage;

pub struct KeyValueDb {
    memory_db: Arc<RwLock<BTreeMap<String, String>>>,
    storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
}

impl KeyValueDb {
    pub fn new(
        db_path: Option<impl AsRef<Path>>,
        storage_tx: Option<mpsc::UnboundedSender<StorageMessage>>,
    ) -> Result<Self, sled::Error> {
        let memory_db = Arc::new(RwLock::new(BTreeMap::new()));

        // Load existing data from persistent storage into memory
        if let Some(path) = db_path {
            let persistent_db = sled::open(path)?;

            // Load existing data from persistent storage into memory
            {
                let mut memory_cache = memory_db.write().unwrap();
                for (key, value) in persistent_db.iter().flatten() {
                    let key_str = String::from_utf8(key.to_vec()).unwrap_or_default();
                    let value_str = String::from_utf8(value.to_vec()).unwrap_or_default();
                    memory_cache.insert(key_str, value_str);
                }
            }
        }

        Ok(Self {
            memory_db,
            storage_tx,
        })
    }

    pub fn execute(&self, op: &Operation, cmd_id: CommandId) -> String {
        match op.name.to_uppercase().as_str() {
            "PUT" => self.execute_put(op, cmd_id),
            "SWAP" => self.execute_swap(op, cmd_id),
            "GET" => self.execute_get(op),
            "DELETE" => self.execute_delete(op, cmd_id),
            "SCAN" => self.execute_scan(op),
            _ => panic!("Unsupported operation: {}", op.name),
        }
    }

    fn execute_put(&self, op: &Operation, cmd_id: CommandId) -> String {
        if op.args.len() != 2 {
            panic!("PUT requires 2 arguments");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];
        let value = &op.args[1];

        let found = memory_db.insert(key.clone(), value.clone()).is_some();
        if let Some(storage_tx) = &self.storage_tx {
            storage_tx
                .send(StorageMessage::Put {
                    cmd_id,
                    op_id: op.id,
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap();
        }

        format!(
            "PUT {} {}",
            op.args[0],
            if found { "found" } else { "not_found" }
        )
    }

    fn execute_swap(&self, op: &Operation, cmd_id: CommandId) -> String {
        if op.args.len() != 2 {
            panic!("SWAP requires 2 arguments");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];
        let new_value = &op.args[1];

        let old_value = memory_db.insert(key.clone(), new_value.clone());
        if let Some(storage_tx) = &self.storage_tx {
            storage_tx
                .send(StorageMessage::Put {
                    cmd_id,
                    op_id: op.id,
                    key: key.clone(),
                    value: new_value.clone(),
                })
                .unwrap();
        }

        match old_value {
            Some(val) => format!("SWAP {} {}", op.args[0], val),
            None => format!("SWAP {} null", op.args[0]),
        }
    }

    fn execute_get(&self, op: &Operation) -> String {
        if op.args.len() != 1 {
            panic!("GET requires 1 argument");
        }

        let memory_db = self.memory_db.read().unwrap();
        let key = &op.args[0];

        match memory_db.get(key) {
            Some(value) => format!("GET {} {}", key, value),
            None => format!("GET {} null", key),
        }
    }

    fn execute_delete(&self, op: &Operation, cmd_id: CommandId) -> String {
        if op.args.len() != 1 {
            panic!("DELETE requires 1 argument");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];

        let found = memory_db.remove(key).is_some();
        if let Some(storage_tx) = &self.storage_tx {
            storage_tx
                .send(StorageMessage::Delete {
                    key: key.clone(),
                    cmd_id,
                    op_id: op.id,
                })
                .unwrap();
        }

        format!(
            "DELETE {} {}",
            key,
            if found { "found" } else { "not_found" }
        )
    }

    fn execute_scan(&self, op: &Operation) -> String {
        if op.args.len() != 2 {
            panic!("SCAN requires 2 arguments");
        }

        let memory_db = self.memory_db.read().unwrap();
        let start_key = &op.args[0];
        let end_key = &op.args[1];

        let mut result = format!("SCAN {} {} BEGIN", start_key, end_key);

        for (key, value) in memory_db.range(start_key.to_owned()..=end_key.to_owned()) {
            result.push_str(&format!("\n  {} {}", key, value));
        }

        result.push_str("\nSCAN END");
        result
    }

    pub async fn sync(&self) -> Result<(), oneshot::error::RecvError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        if let Some(storage_tx) = &self.storage_tx {
            storage_tx.send(StorageMessage::Flush { reply_tx }).unwrap();
        }
        reply_rx.await
    }
}
