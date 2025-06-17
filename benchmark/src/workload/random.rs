use crate::workload::Workload;
use async_trait::async_trait;
use rand::distr::{Alphanumeric, Distribution};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct RandomWorkload {
    rng: StdRng,
    key_range: usize,
    rw_ratio: f64,
}

impl RandomWorkload {
    pub fn new(key_range: usize, rw_ratio: f64, seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            key_range,
            rw_ratio,
        }
    }

    fn gen_value(&mut self) -> String {
        Alphanumeric
            .sample_iter(&mut self.rng)
            .take(30)
            .map(char::from)
            .collect()
    }
}

#[async_trait]
impl Workload for RandomWorkload {
    async fn next_command(&mut self) -> Option<String> {
        let key = format!("users{}", self.rng.random_range(0..self.key_range));
        let is_get = self.rng.random_bool(self.rw_ratio);

        Some(if is_get {
            format!("GET {}", key)
        } else {
            format!("PUT {} {}", key, self.gen_value())
        })
    }
}
