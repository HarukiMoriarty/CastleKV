mod id;
mod session;

pub use id::{CommandId, NodeId};
pub use session::Session;

/// Length of the key in the key table
pub const KEY_LEN: usize = 8;

/// Initialize tracing with the default environment filter
pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

/// Extract table name and key number from a composite key string
pub fn extract_key(key: &str) -> Result<(String, u64), String> {
    // Check if key contains at least one digit
    if !key.chars().any(|c| c.is_ascii_digit()) {
        return Err(format!(
            "Invalid key format: '{}'. No numeric portion found",
            key
        ));
    }

    // Find where the numeric part starts
    let (table_part, num_part) =
        key.split_at(key.chars().position(|c| c.is_ascii_digit()).unwrap());

    // Check that the table part is not empty
    if table_part.is_empty() {
        return Err(format!(
            "Invalid key format: '{}'. Missing table name prefix",
            key
        ));
    }

    // Parse the number portion
    match num_part.parse::<u64>() {
        Ok(num) => Ok((table_part.to_string(), num)),
        Err(_) => Err(format!(
            "Invalid key number: '{}'. Expected numeric value",
            num_part
        )),
    }
}

/// Form a composite key from table name and number
pub fn form_key(table_name: &String, num: u64) -> String {
    if table_name == "key" {
        format!("key{:0w$}", num, w = KEY_LEN - 3)
    } else {
        format!("{}{}", table_name, num)
    }
}

/// Set the default RUST_LOG environment variable if not already set
pub fn set_default_rust_log(val: &str) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", val);
    }
}

/// Constants for metadata keys
pub mod metadata {
    /// Session name metadata key
    pub const SESSION_NAME: &str = "session-name";

    /// Executor ID metadata key
    pub const EXECUTOR_ID: &str = "executor-id";
}
