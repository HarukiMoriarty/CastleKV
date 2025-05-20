pub(crate) mod load;
pub(crate) mod uniform;

/// An operation to be executed by the benchmark
#[derive(Debug, Clone)]
pub struct Operation {
    /// The name of the operation (GET, PUT, DELETE, SCAN)
    pub name: String,

    /// The arguments for the operation
    pub args: Vec<String>,
}

impl Operation {
    /// Create a new GET operation
    pub fn get(key: String) -> Self {
        Self {
            name: "GET".to_string(),
            args: vec![key],
        }
    }

    /// Create a new PUT operation
    pub fn put(key: String, value: String) -> Self {
        Self {
            name: "PUT".to_string(),
            args: vec![key, value],
        }
    }

    /// Create a new DELETE operation
    pub fn delete(key: String) -> Self {
        Self {
            name: "DELETE".to_string(),
            args: vec![key],
        }
    }

    /// Create a new SCAN operation
    pub fn scan(start_key: String, end_key: String) -> Self {
        Self {
            name: "SCAN".to_string(),
            args: vec![start_key, end_key],
        }
    }
}

/// Workload Trait
pub trait Workload {
    /// Get the next operation to execute, or None if the workload is complete
    fn next_operation(&mut self) -> Operation;
}
