mod id;
mod session;

pub use id::{CommandId, NodeId, TransactionId};
pub use session::Session;

pub fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}

pub fn set_default_rust_log(val: &str) {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", val);
    }
}

pub mod metadata {
    pub const SESSION_NAME: &str = "session-name";
    pub const EXECUTOR_ID: &str = "executor-id";
}
