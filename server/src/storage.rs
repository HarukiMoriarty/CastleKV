use sled::Db;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::config::ServerConfig;

/// Messages for storage service communication
#[derive(Debug)]
pub enum StorageMessage {
    /// Store a key-value pair
    Put {
        /// Associated log entry index for waterline tracking
        log_entry_index: Option<u64>,
        /// Key to store
        key: String,
        /// Value to store
        value: String,
    },
    /// Delete a key-value pair
    Delete {
        /// Associated log entry index for waterline tracking
        log_entry_index: Option<u64>,
        /// Key to delete
        key: String,
    },
    /// Flush pending operations to disk
    Flush {
        /// Channel for reply waterline log entry index
        reply_tx: oneshot::Sender<Option<u64>>,
    },
}

/// Persistent storage service for database operations
pub struct Storage {
    /// Underlying sled database for persistence
    db: Db,
    /// Channel receiver for incoming storage requests
    storage_rx: mpsc::UnboundedReceiver<StorageMessage>,
    /// Maximum number of operations to batch before flushing
    max_batch_size: usize,
    /// Maximum time to wait before flushing
    flush_interval: Duration,
}

impl Storage {
    /// Create a new Storage service
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing storage settings
    /// * `storage_rx` - Channel receiver for storage messages
    pub fn new(
        config: &ServerConfig,
        storage_rx: mpsc::UnboundedReceiver<StorageMessage>,
    ) -> Result<Self, sled::Error> {
        // Ensure db_path is set
        let db_path = config
            .db_path
            .clone()
            .ok_or_else(|| sled::Error::Unsupported("DB path not configured".to_string()))?;

        // Open the sled database
        let db = sled::open(db_path)?;

        // Ensure batch settings are configured
        let max_batch_size = config
            .batch_size
            .ok_or_else(|| sled::Error::Unsupported("Batch size not configured".to_string()))?;

        let flush_interval =
            Duration::from_millis(config.batch_timeout_ms.ok_or_else(|| {
                sled::Error::Unsupported("Batch timeout not configured".to_string())
            })?);

        Ok(Self {
            db,
            storage_rx,
            max_batch_size,
            flush_interval,
        })
    }

    /// Run the storage service, processing messages until the channel is closed
    pub async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.max_batch_size);
        let mut last_flush = Instant::now();
        let mut interval = tokio::time::interval(self.flush_interval);

        loop {
            tokio::select! {
                // Handle incoming messages
                Some(msg) = self.storage_rx.recv() => {
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
                            // Get the log checkpoint index from the first entry in the batch
                            let log_checkpoint_index = batch.first().and_then(|entry| entry.0);

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
                            if reply_tx.send(log_checkpoint_index).is_err() {
                                warn!("Failed to send flush acknowledgement");
                            }

                            last_flush = Instant::now();
                        }
                    }
                }

                // Time-based flush
                _ = interval.tick() => {
                    // Check if channel is closed - fail fast if it is
                    if self.storage_rx.is_closed() {
                        debug!("Storage channel closed, detected during timer tick");
                        break;
                    }

                    // Perform time-based flush if we have data
                    if !batch.is_empty() {
                        debug!("Time-based flush triggered after {:?}", last_flush.elapsed());
                        self.flush_batch(&mut batch).await;
                        last_flush = Instant::now();
                    }
                }
            }
        }

        // Final flush of any pending operations
        if !batch.is_empty() {
            debug!("Flushing remaining {} items before shutdown", batch.len());
            self.flush_batch(&mut batch).await;

            // Final flush to disk
            if let Err(e) = self.db.flush() {
                error!("Failed to flush database during shutdown: {}", e);
            }
        }

        info!("Storage service stopped");
    }

    /// Flush a batch of operations to the persistent storage
    ///
    /// # Arguments
    ///
    /// * `batch` - Vector of operations to flush (will be cleared)
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

        debug!("Flushed batch to storage");
    }
}
