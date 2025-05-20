use rand::{rngs::StdRng, Rng, SeedableRng};

use crate::config::ExecuteConfig;
use crate::workload::{Operation, Workload};

/// A workload that generates operations with uniformly distributed keys
pub struct UniformWorkload {
    /// Key prefix to use
    key_prefix: String,

    /// Total number of keys in the key space
    num_keys: u64,

    /// Size of values in bytes
    value_size: usize,

    /// Percentage of read operations (0-100)
    read_percentage: u8,

    /// Random number generator for key/value generation
    rng: StdRng,
}

impl UniformWorkload {
    /// Create a new uniform workload with default random seed
    pub fn new(config: &ExecuteConfig, seed: u64) -> Self {
        // Create RNG with specified seed or from entropy
        let rng = StdRng::seed_from_u64(seed);

        Self {
            key_prefix: config
                .key_prefix
                .clone()
                .unwrap_or_else(|| "key".to_string()),
            num_keys: config.num_keys,
            value_size: config.value_size,
            read_percentage: config.read_percentage,
            rng,
        }
    }

    /// Generate a uniformly random key
    fn generate_key(&mut self) -> String {
        let key_id = self.rng.random_range(0..self.num_keys);
        format!("{}{}", self.key_prefix, key_id)
    }

    /// Generate a random value of specified size
    fn generate_value(&mut self) -> String {
        // Simple random value generator
        (0..self.value_size)
            .map(|_| self.rng.random_range(b'a'..=b'z') as char)
            .collect()
    }

    /// Determine if the next operation should be a read based on read percentage
    fn should_read(&mut self) -> bool {
        self.rng.random_range(0..100) < self.read_percentage
    }
}

impl Workload for UniformWorkload {
    fn next_operation(&mut self) -> Operation {
        // Determine operation type based on read percentage
        if self.should_read() {
            let key = self.generate_key();
            Operation::get(key)
        } else {
            let key = self.generate_key();
            let value = self.generate_value();
            Operation::put(key, value)
        }
    }
}
