use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::config::LoadConfig;
use crate::workload::{Operation, Workload};

/// A workload specifically optimized for loading initial data
pub struct LoadWorkload {
    /// Key prefix to use
    key_prefix: String,

    /// Size of values in bytes
    value_size: usize,

    /// Current key being loaded
    current_key: u64,

    /// Random number generator for value generation
    rng: StdRng,
}

impl LoadWorkload {
    /// Create a new load workload with default random seed
    pub fn new(config: &LoadConfig, seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        Self {
            key_prefix: config
                .key_prefix
                .clone()
                .unwrap_or_else(|| "key".to_string()),
            value_size: config.value_size,
            current_key: 0,
            rng,
        }
    }

    /// Generate a sequential key
    fn next_key(&mut self) -> String {
        let key = format!("{}{}", self.key_prefix, self.current_key);
        self.current_key += 1;
        key
    }

    /// Generate a random value of specified size
    fn generate_value(&mut self) -> String {
        (0..self.value_size)
            .map(|_| self.rng.random_range(b'a'..=b'z') as char)
            .collect()
    }
}

impl Workload for LoadWorkload {
    fn next_operation(&mut self) -> Operation {
        // Generate key and value
        let key = self.next_key();
        let value = self.generate_value();

        // Return PUT operation
        Operation::put(key, value)
    }
}
