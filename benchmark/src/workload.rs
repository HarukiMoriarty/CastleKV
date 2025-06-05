pub mod random;

pub use random::RandomWorkload;

use async_trait::async_trait;

#[async_trait]
pub trait Workload {
    async fn next_command(&mut self) -> Option<String>;
}
