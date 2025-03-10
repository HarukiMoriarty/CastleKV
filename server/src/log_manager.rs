use bincode;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

use common::CommandId;

pub type LogManagerSender = UnboundedSender<LogManagerMessage>;
type LogManagerReceiver = UnboundedReceiver<LogManagerMessage>;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogCommand {
    cmd_id: CommandId,
    ops: Vec<(String, String)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogEntry {
    term: u64,
    index: u64,
    command: LogCommand,
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
    /// Initializes the log manager, loads existing segments or creates a new one
    pub fn open(base_path: &str, max_segment_size: usize) -> io::Result<Self> {
        // For simplicity, directly create a new segment file
        let segment_path = format!("{}/raft_log_segment_0.log", base_path);
        let current_segment = Segment::new(&segment_path, 1)?;
        Ok(RaftLog {
            segments: Vec::new(),
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
        term: u64,
        index: u64,
        cmd_id: CommandId,
        ops: Vec<(String, String)>,
        resp_tx: oneshot::Sender<()>,
    },
}

pub struct LogManager {
    log: RaftLog,
    rx: LogManagerReceiver,
}

impl LogManager {
    pub fn new(
        log_path: Option<impl AsRef<Path>>,
        rx: LogManagerReceiver,
        max_segment_size: usize,
    ) -> Self {
        let path_str = log_path
            .as_ref()
            .map(|p| p.as_ref().to_str().unwrap())
            .unwrap_or("raft");
        let log = RaftLog::open(path_str, max_segment_size).unwrap();

        LogManager { log, rx }
    }

    pub async fn run(mut self) {
        let mut rx = self.rx;

        while let Some(msg) = rx.recv().await {
            match msg {
                LogManagerMessage::AppendEntry {
                    term,
                    index,
                    ops,
                    cmd_id,
                    resp_tx,
                } => {
                    let entry = LogEntry {
                        term,
                        index,
                        command: LogCommand { cmd_id, ops },
                    };

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
                    let _ = resp_tx.send(());
                }
            }
        }
    }
}
