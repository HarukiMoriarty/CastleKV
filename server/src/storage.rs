use sled::Db;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;

pub enum StorageMessage {
    Put {
        log_entry_index: Option<u64>,
        key: String,
        value: String,
    },
    Delete {
        log_entry_index: Option<u64>,
        key: String,
    },
    Flush {
        reply_tx: oneshot::Sender<Option<u64>>,
    },
}

pub struct Storage {
    // Persistence sled database
    db: Db,
    // Channel receiver for incoming storage requests
    storage_rx: mpsc::UnboundedReceiver<StorageMessage>,
    // Batching configuration
    max_batch_size: usize,
    flush_interval: Duration,
}

impl Storage {
    pub fn new(
        config: &ServerConfig,
        storage_rx: mpsc::UnboundedReceiver<StorageMessage>,
    ) -> Result<Self, sled::Error> {
        // Open the sled database
        let db = sled::open(config.db_path.clone().unwrap())?;

        Ok(Self {
            db,
            storage_rx,
            max_batch_size: config.batch_size.unwrap(),
            flush_interval: Duration::from_millis(config.batch_timeout_ms.unwrap()),
        })
    }

    pub async fn run(mut self) {
        let mut batch = Vec::new();
        let mut last_flush = Instant::now();

        while let Some(msg) = self.storage_rx.recv().await {
            match msg {
                StorageMessage::Put {
                    log_entry_index,
                    key,
                    value,
                } => {
                    // Add to batch
                    batch.push((log_entry_index, key, Some(value)));

                    // Flush if batch is large enough
                    if batch.len() >= self.max_batch_size {
                        self.flush_batch(&mut batch).await;
                        last_flush = Instant::now();
                    }
                }
                StorageMessage::Delete {
                    log_entry_index,
                    key,
                } => {
                    // Add to batch
                    batch.push((log_entry_index, key, None));

                    // Flush if batch is large enough
                    if batch.len() >= self.max_batch_size {
                        self.flush_batch(&mut batch).await;
                        last_flush = Instant::now();
                    }
                }
                StorageMessage::Flush { reply_tx } => {
                    let log_checkpoint_index = match batch.first() {
                        Some(log_entry_index) => log_entry_index.0,
                        None => None,
                    };
                    // Flush any pending operations
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch).await;
                    }

                    // Ensure data is written to disk
                    if let Err(e) = self.db.flush() {
                        error!("Failed to flush database: {}", e);
                    }
                    debug!("Database flushed");

                    // Send acknowledgement
                    // TODO: should check highest continuous command id, checkpoint
                    if reply_tx.send(log_checkpoint_index).is_err() {
                        warn!("Failed to send flush acknowledgement");
                    }

                    last_flush = Instant::now();
                }
            }

            // Time-based flush check
            if !batch.is_empty() && last_flush.elapsed() >= self.flush_interval {
                self.flush_batch(&mut batch).await;
                last_flush = Instant::now();
            }
        }

        info!("Storage service stopped");
    }

    async fn flush_batch(&self, batch: &mut Vec<(Option<u64>, String, Option<String>)>) {
        for (_, key, value_opt) in batch.drain(..) {
            match value_opt {
                Some(value) => {
                    if let Err(e) = self.db.insert(key.as_bytes(), value.as_bytes()) {
                        error!("Failed to persist PUT for key {}: {}", key, e);
                    }
                }
                None => {
                    if let Err(e) = self.db.remove(key.as_bytes()) {
                        error!("Failed to persist DELETE for key {}: {}", key, e);
                    }
                }
            }
        }
    }
}
