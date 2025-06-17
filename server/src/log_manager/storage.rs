use bincode;
use glob;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use tracing::debug;

use rpc::raft::LogEntry;

/// A single log segment file
pub(crate) struct Segment {
    /// File handle for this segment
    file: File,
}

impl Segment {
    /// Creates or opens a log segment
    ///
    /// # Arguments
    ///
    /// * `path` - File path for the segment
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Segment { file })
    }

    /// Appends a log entry to the current segment and ensures it's persisted
    ///
    /// # Arguments
    ///
    /// * `entry` - The log entry to append
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        let data =
            bincode::serialize(entry).map_err(io::Error::other)?;
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

/// Raft log manager with segmented storage support
pub(crate) struct RaftLog {
    /// Closed (archived) log segments
    segments: Vec<Segment>,
    /// Current active segment for writing
    current_segment: Segment,
    /// Base directory for log files
    base_path: String,
    /// Maximum size of each segment in bytes
    max_segment_size: usize,
    /// in-memory log entries
    entries: Vec<LogEntry>,
}

impl RaftLog {
    /// Opens or creates a log, handling recovery automatically
    ///
    /// # Arguments
    ///
    /// * `base_path` - Directory for log files
    /// * `max_segment_size` - Maximum size of each segment in bytes
    pub fn open(base_path: &str, max_segment_size: usize) -> io::Result<Self> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(base_path)?;

        // Check for existing segments
        let log_pattern = format!("{}/raft_log_segment_*.log", base_path);
        let mut existing_segments: Vec<_> = glob::glob(&log_pattern)
            .map_err(io::Error::other)?
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

        // Load existing segments
        for segment_path in &existing_segments {
            let path_str = segment_path.to_string_lossy();

            // For the first segment, start index is 1
            let segment = Segment::new(&path_str)?;

            segments.push(segment);
        }

        // Create or use the last segment as current
        let current_segment = if segments.is_empty() {
            // No segments found, create a new one
            debug!("Creating initial log segment");
            Segment::new(&format!("{}/raft_log_segment_0.log", base_path))?
        } else {
            // Use the last segment as current
            segments.pop().unwrap()
        };

        // Create an empty entries vector - we'll load them separately
        let entries = Vec::new();

        let mut log = RaftLog {
            segments,
            current_segment,
            base_path: base_path.to_string(),
            max_segment_size,
            entries,
        };

        // Load all entries into memory
        log.entries = log.load_all_entries()?;

        Ok(log)
    }

    /// Appends a new log entry and creates a new segment when the current one reaches its size limit
    ///
    /// # Arguments
    ///
    /// * `entry` - Log entry to append
    pub fn append(&mut self, entry: LogEntry) -> io::Result<()> {
        // Check current segment size, create a new one if it exceeds the limit
        let file_size = self.current_segment.file.metadata()?.len() as usize;
        if file_size > self.max_segment_size {
            // Archive the old segment
            let new_segment_path = format!(
                "{}/raft_log_segment_{}.log",
                self.base_path,
                self.segments.len() + 1
            );

            debug!(
                "Creating new log segment: {} (current size: {} bytes)",
                new_segment_path, file_size
            );

            self.segments.push(std::mem::replace(
                &mut self.current_segment,
                Segment::new(&new_segment_path)?,
            ));
        }

        // Append to disk
        self.current_segment.append(&entry)?;

        // Also append to in-memory entries
        self.entries.push(entry);

        Ok(())
    }

    /// Loads all log entries from all segments
    pub fn load_all_entries(&mut self) -> io::Result<Vec<LogEntry>> {
        let mut all_entries = Vec::new();

        // Load entries from all archived segments
        for (i, segment) in self.segments.iter_mut().enumerate() {
            debug!("Loading entries from segment {}", i);
            let mut entries = segment.load_entries()?;
            all_entries.append(&mut entries);
        }

        // Load entries from the current segment
        let mut current_entries = self.current_segment.load_entries()?;
        all_entries.append(&mut current_entries);

        debug!("Loaded {} log entries into memory", all_entries.len());
        Ok(all_entries)
    }

    /// Get the index of the last log entry
    pub fn get_last_index(&self) -> u64 {
        if let Some(entry) = self.entries.last() {
            entry.index
        } else {
            0 // Return 0 if log is empty
        }
    }

    /// Get the term of the last log entry
    pub fn get_last_term(&self) -> u64 {
        if let Some(entry) = self.entries.last() {
            entry.term
        } else {
            0 // Return 0 if log is empty
        }
    }

    /// Get a log entry at a specific index
    pub fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        // Find the entry with the matching index
        // This assumes entries are stored in order by index
        self.entries.iter().find(|entry| entry.index == index)
    }

    /// Truncate the log from the given index onwards
    pub fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        // Remove all entries from index onwards
        self.entries.truncate(index as usize);

        // TODO: Also truncate the on-disk segments if needed
        Ok(())
    }
}
