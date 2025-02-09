use rpc::gateway::Operation;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct KeyValueDb {
    data: Arc<RwLock<BTreeMap<String, String>>>,
}

impl KeyValueDb {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
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

        let mut data = self.data.write().unwrap();
        let key = op.args[0].clone();
        let value = op.args[1].clone();

        let found = data.contains_key(&key);
        data.insert(key, value);

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

        let mut data = self.data.write().unwrap();
        let key = op.args[0].clone();
        let new_value = op.args[1].clone();

        let old_value = data.insert(key, new_value);

        match old_value {
            Some(val) => format!("SWAP {} {}", op.args[0], val),
            None => format!("SWAP {} null", op.args[0]),
        }
    }

    fn execute_get(&self, op: &Operation) -> String {
        if op.args.len() != 1 {
            panic!("GET requires 1 argument");
        }

        let data = self.data.read().unwrap();
        let key = &op.args[0];

        match data.get(key) {
            Some(value) => format!("GET {} {}", key, value),
            None => format!("GET {} null", key),
        }
    }

    fn execute_delete(&self, op: &Operation) -> String {
        if op.args.len() != 1 {
            panic!("DELETE requires 1 argument");
        }

        let mut data = self.data.write().unwrap();
        let key = &op.args[0];

        let found = data.remove(key).is_some();

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

        let data = self.data.read().unwrap();
        let start_key = &op.args[0];
        let end_key = &op.args[1];

        let mut result = format!("SCAN {} {} BEGIN", start_key, end_key);

        for (key, value) in data.range(start_key.to_owned()..=end_key.to_owned()) {
            result.push_str(&format!("\n  {} {}", key, value));
        }

        result.push_str("\nSCAN END");
        result
    }
}

impl Default for KeyValueDb {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let db = KeyValueDb::new();

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
        let db = KeyValueDb::new();

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
        let db = KeyValueDb::new();

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
        let db = KeyValueDb::new();

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
