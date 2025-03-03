use rpc::gateway::Operation;
use sled::Db;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct KeyValueDb {
    memory_db: Arc<RwLock<BTreeMap<String, String>>>,
    persistent_db: Db,
}

impl KeyValueDb {
    pub fn new(db_path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let persistent_db = sled::open(db_path)?;
        let memory_db = Arc::new(RwLock::new(BTreeMap::new()));

        // Load existing data from persistent storage into memory
        {
            let mut memory_cache = memory_db.write().unwrap();
            for result in persistent_db.iter() {
                if let Ok((key, value)) = result {
                    let key_str = String::from_utf8(key.to_vec()).unwrap_or_default();
                    let value_str = String::from_utf8(value.to_vec()).unwrap_or_default();
                    memory_cache.insert(key_str, value_str);
                }
            }
        }

        Ok(Self {
            memory_db,
            persistent_db,
        })
    }

    pub fn execute(&self, op: &Operation) -> String {
        match op.name.to_uppercase().as_str() {
            "PUT" => self.execute_put(op),
            "SWAP" => self.execute_swap(op),
            "GET" => self.execute_get(op),
            "DELETE" => self.execute_delete(op),
            "SCAN" => self.execute_scan(op),
            _ => panic!("Unsupported operation: {}", op.name),
        }
    }

    fn execute_put(&self, op: &Operation) -> String {
        if op.args.len() != 2 {
            panic!("PUT requires 2 arguments");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];
        let value = &op.args[1];

        let found = memory_db.insert(key.clone(), value.clone()).is_some();
        let _ = self.persistent_db.insert(key.as_bytes(), value.as_bytes());

        format!(
            "PUT {} {}",
            op.args[0],
            if found { "found" } else { "not_found" }
        )
    }

    fn execute_swap(&self, op: &Operation) -> String {
        if op.args.len() != 2 {
            panic!("SWAP requires 2 arguments");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];
        let new_value = &op.args[1];

        let old_value = memory_db.insert(key.clone(), new_value.clone());
        let _ = self
            .persistent_db
            .insert(key.as_bytes(), new_value.as_bytes());

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

    fn execute_delete(&self, op: &Operation) -> String {
        if op.args.len() != 1 {
            panic!("DELETE requires 1 argument");
        }

        let mut memory_db = self.memory_db.write().unwrap();
        let key = &op.args[0];

        let found = memory_db.remove(key).is_some();
        let _ = self.persistent_db.remove(key.as_bytes());

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let db_path = dir.path();

        // Create and populate the database
        {
            let db = KeyValueDb::new(db_path).unwrap();

            let put_op = Operation {
                id: 1,
                name: "PUT".to_string(),
                args: vec!["persist_key".to_string(), "persist_value".to_string()],
            };

            db.execute(&put_op);
            // Explicitly flush to ensure data is written
            db.persistent_db.flush().unwrap();
        }

        // Create a new instance and verify data persisted
        {
            let db = KeyValueDb::new(db_path).unwrap();

            let get_op = Operation {
                id: 2,
                name: "GET".to_string(),
                args: vec!["persist_key".to_string()],
            };

            let result = db.execute(&get_op);
            assert_eq!(result, "GET persist_key persist_value");
        }
    }

    #[test]
    fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let db = KeyValueDb::new(dir.path()).unwrap();

        // First PUT
        let put_op1 = Operation {
            id: 1,
            name: "PUT".to_string(),
            args: vec!["key1".to_string(), "value1".to_string()],
        };
        assert_eq!(db.execute(&put_op1), "PUT key1 not_found");

        // Second PUT to same key
        let put_op2 = Operation {
            id: 2,
            name: "PUT".to_string(),
            args: vec!["key1".to_string(), "value2".to_string()],
        };
        assert_eq!(db.execute(&put_op2), "PUT key1 found");

        // GET
        let get_op = Operation {
            id: 3,
            name: "GET".to_string(),
            args: vec!["key1".to_string()],
        };
        assert_eq!(db.execute(&get_op), "GET key1 value2");
    }

    #[test]
    fn test_swap() {
        let dir = tempdir().unwrap();
        let db = KeyValueDb::new(dir.path()).unwrap();

        // First SWAP (should return null)
        let swap_op1 = Operation {
            id: 1,
            name: "SWAP".to_string(),
            args: vec!["key1".to_string(), "value1".to_string()],
        };
        assert_eq!(db.execute(&swap_op1), "SWAP key1 null");

        // Second SWAP
        let swap_op2 = Operation {
            id: 2,
            name: "SWAP".to_string(),
            args: vec!["key1".to_string(), "value2".to_string()],
        };
        assert_eq!(db.execute(&swap_op2), "SWAP key1 value1");
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let db = KeyValueDb::new(dir.path()).unwrap();

        // PUT first
        let put_op = Operation {
            id: 1,
            name: "PUT".to_string(),
            args: vec!["key1".to_string(), "value1".to_string()],
        };
        db.execute(&put_op);

        // First DELETE
        let delete_op1 = Operation {
            id: 2,
            name: "DELETE".to_string(),
            args: vec!["key1".to_string()],
        };
        assert_eq!(db.execute(&delete_op1), "DELETE key1 found");

        // Second DELETE
        let delete_op2 = Operation {
            id: 3,
            name: "DELETE".to_string(),
            args: vec!["key1".to_string()],
        };
        assert_eq!(db.execute(&delete_op2), "DELETE key1 not_found");
    }

    #[test]
    fn test_scan() {
        let dir = tempdir().unwrap();
        let db = KeyValueDb::new(dir.path()).unwrap();

        // Populate some data
        let put_ops = vec![
            Operation {
                id: 1,
                name: "PUT".to_string(),
                args: vec!["key127".to_string(), "valueaaa".to_string()],
            },
            Operation {
                id: 2,
                name: "PUT".to_string(),
                args: vec!["key299".to_string(), "valuebbb".to_string()],
            },
            Operation {
                id: 3,
                name: "PUT".to_string(),
                args: vec!["key456".to_string(), "valueccc".to_string()],
            },
            Operation {
                id: 4,
                name: "PUT".to_string(),
                args: vec!["key600".to_string(), "valueddd".to_string()],
            },
        ];

        for op in put_ops {
            db.execute(&op);
        }

        // SCAN
        let scan_op = Operation {
            id: 5,
            name: "SCAN".to_string(),
            args: vec!["key123".to_string(), "key456".to_string()],
        };

        let result = db.execute(&scan_op);
        assert_eq!(
            result,
            "SCAN key123 key456 BEGIN\n  key127 valueaaa\n  key299 valuebbb\n  key456 valueccc\nSCAN END"
        );
    }
}
