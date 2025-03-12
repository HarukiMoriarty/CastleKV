use bincode;
use common::CommandId;
use glob;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing::{debug, error, info};

use crate::database::KeyValueDb;
use crate::plan::Plan;

pub type LogManagerSender = UnboundedSender<LogManagerMessage>;
type LogManagerReceiver = UnboundedReceiver<LogManagerMessage>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct LogCommand {
    cmd_id: CommandId,
    pub(crate) ops: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct LogEntry {
    term: u64,
    pub(crate) index: u64,
    pub(crate) command: LogCommand,
}

/// A single log segment
struct Segment {
    file: File,
    path: String,
    start_index: u64,
}

impl Segment {
    /// Creates or opens a log segment
    pub fn new(path: &str, start_index: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Segment {
            file,
            path: path.to_string(),
            start_index,
        })
    }

    /// Appends a log entry to the current segment and ensures it's persisted
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        let data =
            bincode::serialize(entry).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.file.write_all(&data)?;
        // Use fsync to ensure data is safely persisted
        self.file.sync_data()?;
        Ok(())
    }

    /// Loads all log entries from the current segment
    pub fn load_entries(&mut self) -> io::Result<Vec<LogEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        let mut cursor = std::io::Cursor::new(buf);
        let mut entries = Vec::new();
        while let Ok(entry) = bincode::deserialize_from::<_, LogEntry>(&mut cursor) {
            entries.push(entry);
        }
        Ok(entries)
    }
}

/// Raft persistent log manager with segmented storage support
pub struct RaftLog {
    segments: Vec<Segment>,
    current_segment: Segment,
    base_path: String,
    max_segment_size: usize, // Maximum size of each segment
}

impl RaftLog {
    /// Opens or creates a log, handling recovery automatically
    pub fn open(base_path: &str, max_segment_size: usize) -> io::Result<Self> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(base_path)?;

        // Check for existing segments
        let log_pattern = format!("{}/raft_log_segment_*.log", base_path);
        let mut existing_segments: Vec<_> = glob::glob(&log_pattern)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .filter_map(Result::ok)
            .collect();

        // Sort segments by their number to ensure correct order
        existing_segments.sort_by(|a, b| {
            let a_num = a
                .to_string_lossy()
                .split("segment_")
                .nth(1)
                .unwrap_or("0")
                .split('.')
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            let b_num = b
                .to_string_lossy()
                .split("segment_")
                .nth(1)
                .unwrap_or("0")
                .split('.')
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
            a_num.cmp(&b_num)
        });

        let mut segments = Vec::new();
        let mut last_index = 0;

        // Load existing segments
        for segment_path in &existing_segments {
            let path_str = segment_path.to_string_lossy();
            // Extract segment number to determine start index
            let segment_num = path_str
                .split("segment_")
                .nth(1)
                .unwrap_or("0")
                .split('.')
                .next()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);

            // For the first segment, start index is 1
            // For others, we'll determine it from the previous segment's entries
            let start_index = if segment_num == 0 { 1 } else { last_index + 1 };

            let mut segment = Segment::new(&path_str, start_index)?;

            // Load entries to determine the last index for the next segment
            if let Ok(entries) = segment.load_entries() {
                if let Some(last_entry) = entries.last() {
                    last_index = last_entry.index;
                }
            }

            segments.push(segment);
        }

        // Create or use the last segment as current
        let current_segment = if segments.is_empty() {
            // No segments found, create a new one
            debug!("Creating initial log segment");
            Segment::new(&format!("{}/raft_log_segment_0.log", base_path), 1)?
        } else {
            // Use the last segment as current
            segments.pop().unwrap()
        };

        Ok(RaftLog {
            segments,
            current_segment,
            base_path: base_path.to_string(),
            max_segment_size,
        })
    }

    /// Appends a new log entry and creates a new segment when the current one reaches its size limit
    fn append(&mut self, entry: LogEntry) -> io::Result<()> {
        // Check current segment size, create a new one if it exceeds the limit
        let file_size = self.current_segment.file.metadata()?.len() as usize;
        if file_size > self.max_segment_size {
            // Archive the old segment
            self.segments.push(std::mem::replace(
                &mut self.current_segment,
                Segment::new(
                    &format!(
                        "{}/raft_log_segment_{}.log",
                        self.base_path,
                        self.segments.len() + 1
                    ),
                    entry.index,
                )?,
            ));
        }
        self.current_segment.append(&entry)
    }

    /// Loads all log entries from all segments
    fn load_all_entries(&mut self) -> io::Result<Vec<LogEntry>> {
        let mut all_entries = Vec::new();
        for segment in &mut self.segments {
            let mut entries = segment.load_entries()?;
            all_entries.append(&mut entries);
        }
        let mut current_entries = self.current_segment.load_entries()?;
        all_entries.append(&mut current_entries);
        Ok(all_entries)
    }
}

pub enum LogManagerMessage {
    AppendEntry {
        cmd_id: CommandId,
        ops: Vec<(String, String)>,
        resp_tx: oneshot::Sender<u64>,
    },
}

pub struct LogManager {
    log: RaftLog,
    rx: LogManagerReceiver,
    db: Arc<KeyValueDb>,
}

impl LogManager {
    pub fn new(
        log_path: Option<impl AsRef<Path>>,
        rx: LogManagerReceiver,
        db: Arc<KeyValueDb>,
        max_segment_size: usize,
    ) -> Self {
        let path_str = log_path
            .as_ref()
            .map(|p| p.as_ref().to_str().unwrap())
            .unwrap_or("raft");

        // RaftLog::open handles directory creation/recovery
        let mut log = match RaftLog::open(path_str, max_segment_size) {
            Ok(log) => {
                info!("Successfully opened log at {}", path_str);
                log
            }
            Err(e) => {
                error!("Failed to open log: {}, retrying...", e);
                RaftLog::open(path_str, max_segment_size).unwrap()
            }
        };

        // Load all log entries into in-memory DB and storage
        let entries = log.load_all_entries().unwrap();
        info!("Loaded {} log entries", entries.len());
        for entry in entries {
            debug!("Log entry: {:?}", entry);

            let plan = Plan::from_log_entry(entry).unwrap();
            db.execute(&plan);
        }

        LogManager { log, rx, db }
    }

    pub async fn run(mut self) {
        let mut rx = self.rx;

        while let Some(msg) = rx.recv().await {
            match msg {
                LogManagerMessage::AppendEntry {
                    ops,
                    cmd_id,
                    resp_tx,
                } => {
                    // TODO: use raft to determine term and log index
                    let term = 0;
                    let index = 0;

                    let entry = LogEntry {
                        term,
                        index,
                        command: LogCommand { cmd_id, ops },
                    };
                    info!("Appending log entry: {:?}", entry);

                    // retry append log entry if failed
                    match self.log.append(entry.clone()) {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Failed to append log entry: {}", e);
                            let mut retry_count = 0;
                            const MAX_RETRIES: usize = 3;

                            while retry_count < MAX_RETRIES {
                                match self.log.append(entry.clone()) {
                                    Ok(_) => break,
                                    Err(e) => {
                                        retry_count += 1;
                                        tracing::warn!(
                                            "Retry {}/{} failed: {}",
                                            retry_count,
                                            MAX_RETRIES,
                                            e
                                        );
                                        if retry_count == MAX_RETRIES {
                                            tracing::error!(
                                                "Max retries reached, giving up on append"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    resp_tx.send(index).unwrap();
                }
            }
        }
    }
}
